package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/vskurikhin/raft/pkg/raft/contract"
)

// _preVoteJitterMs — верхняя граница случайной паузы (миллисекунды)
// перед выборами после выигранного предварительного голосования.
// Пауза разводит по времени узлы, одновременно победившие в pre-vote,
// предотвращая одновременный переход в кандидаты с одинаковым
// термом (split vote).
const _preVoteJitterMs = 50

// forcedReelectionEnv — имя переменной окружения стресс-хука форсирования
// выборов. Значение читается один раз при старте узла и кэшируется в
// _forcedReelectionHook (ADR-CONF-011); тестовое смещение, в промышленных
// запусках не задаётся.
const forcedReelectionEnv = "RAFT_FORCE_MORE_REELECTION"

// _forcedReelectionHook — кэш переменной окружения forcedReelectionEnv:
// при значении true electionTimeoutLocked в трети вызовов возвращает ровно
// базу, отключая рандомизацию тайм-аута выборов (стресс-смещение).
// Записывается один раз при создании CM, читается атомарно.
var _forcedReelectionHook atomic.Bool

// becomeFollowerLocked делает cm последователем и сбрасывает его состояние.
// Если cm был лидером, отправляет сигнал в stepDown, чтобы leaderLoop
// завершил работу и разрешил все ожидающие future с ErrLeadershipLost.
// Требует удержания cm.mu.
func (cm *ConsensusModule) becomeFollowerLocked(term int) {
	wasLeader := cm.cmState.state == Leader
	if traceEnabled(_traceLevelKeyEvents) {
		cm.traceLogfLocked("becomes Follower with term=%d; len(log)=%v", term, len(cm.cmState.log))
	}

	// Сбрасываем inflightAE для всех соседей при потере лидерства.
	// Это гарантирует, что новый лидер начнёт с «чистого листа».
	// Проверка на nil защищает от обращения к несуществующему пиру:
	// запись в nil *atomic.Bool приведёт к панике.
	for peerID := range cm.leaderState.inflightAE {
		if cm.leaderState.inflightAE[peerID] != nil {
			cm.leaderState.inflightAE[peerID].Store(false)
		}
	}

	cm.cmState.state = Follower
	if wasLeader {
		if term > cm.cmState.currentTerm {
			cm.counters.stepDowns.higherTerm.Add(1)
		}
		// Очищаем состояние повторных попыток репликации, связанное с ролью лидера.
		// Жизненный цикл этих полей строго привязан к роли Leader;
		// такой сброс исключает утечки и некорректное использование при смене роли.
		cm.leaderState.replFailures = nil
		cm.leaderState.lastAttempt = nil
		select {
		case cm.stepDown <- struct{}{}:
		default:
		}
	}
	if term > cm.cmState.currentTerm {
		cm.cmState.currentTerm = term
		cm.cmState.votedFor = -1
		cm.persistToStorageLocked(persistSourceFollowerTerm)
	}
	cm.cmState.leaderLastContact = time.Time{}
	cm.cmState.leaderID = -1
	cm.cmState.electionResetEvent = time.Now()

	select {
	case <-cm.cmState.electionTimerDone:
	default:
		close(cm.cmState.electionTimerDone)
	}
	cm.cmState.electionTimerDone = make(chan struct{})
	cm.goSpawnLocked(cm.runElectionTimer)
}

// startElectionLocked запускает новые выборы с этим CM в качестве кандидата.
// Требует удержания cm.mu.
//
// Если candidateFromLeadershipTransfer установлен, в RequestVoteArgs
// передаётся LeadershipTransfer=true, что bypass проверку наличия лидера
// на узлах-получателях. Флаг сбрасывается после того, как все горутины
// отправили RequestVote, чтобы последующие обычные выборы не имели этого флага.
//
// Неголосующие исключаются из рассылки RequestVote — они не участвуют
// в голосовании и не должны получать запросы на предоставление голоса.
func (cm *ConsensusModule) startElectionLocked() {
	startTimeNow := time.Now()
	defer func() { cm.latency.election.observe(time.Since(startTimeNow)) }()
	cm.cmState.state = Candidate
	cm.cmState.currentTerm += 1
	savedCurrentTerm := cm.cmState.currentTerm
	cm.cmState.electionResetEvent = time.Now()
	cm.cmState.votedFor = cm.id
	cm.persistToStorageLocked(persistSourceCandidate)
	if traceEnabled(_traceLevelKeyEvents) {
		cm.traceLogfLocked(
			"becomes Candidate (currentTerm=%d); len(log)=%v",
			savedCurrentTerm, len(cm.cmState.log),
		)
	}

	// Сохраняем флаг в локальную переменную до его сброса.
	isLeadershipTransfer := cm.cmState.candidateFromLeadershipTransfer.Load()
	// Сбрасываем флаг, чтобы последующие обычные выборы не имели этого флага.
	cm.cmState.candidateFromLeadershipTransfer.Store(false)

	// Определяем состав кластера из текущей конфигурации.
	cfg := cm.cmState.configurations.latest
	voters := voterIDs(cfg)
	voterCount := len(voters)

	// Одиночный узел: побеждает на выборах немедленно.
	if voterCount <= 1 {
		cm.startLeaderLocked()
		return
	}

	var votesReceived atomic.Int32
	votesReceived.Store(1)

	// Параллельно отправить RPC-запросы RequestVote всем голосующим.
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		if s.Suffrage != Voter {
			continue
		}
		cm.goSpawnLocked(func() {
			cm.requestVoteFromPeer(peerID, savedCurrentTerm, voterCount, isLeadershipTransfer, &votesReceived)
		})
	}

	// Запустить новый таймер выборов на случай, если текущие выборы не завершатся успешно.
	cm.goSpawnLocked(cm.runElectionTimer)
}

// requestVoteFromPeer отправляет RequestVote одному соседу и учитывает ответ;
// голос засчитывается, пока узел остаётся кандидатом того же терма.
//
// Самостоятельно захватывает и освобождает cm.mu: две короткие критические
// секции — снимок lastLogIndexAndTermLocked() и разбор ответа; вызов транспорта
// происходит между ними, без блокировки. votesReceived передаётся указателем,
// чтобы общий счётчик голосов выборов разделялся между горутинами соседей.
func (cm *ConsensusModule) requestVoteFromPeer(
	peerID, savedCurrentTerm, voterCount int,
	isLeadershipTransfer bool,
	votesReceived *atomic.Int32,
) {
	cm.mu.Lock()
	savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTermLocked()
	cm.mu.Unlock()

	args := RequestVoteArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		},
		Term:               savedCurrentTerm,
		CandidateID:        cm.id,
		LastLogIndex:       savedLastLogIndex,
		LastLogTerm:        savedLastLogTerm,
		LeadershipTransfer: isLeadershipTransfer,
	}

	if traceEnabled(_traceLevelKeyEvents) {
		cm.traceLogf("sending RequestVote to %d: %+v", peerID, args)
	}
	reply, err := cm.transport.RequestVote(ServerID(peerID), args)
	//nolint:nestif // проверки уровня механически повышают метрики, логика не меняется
	if err == nil {
		cm.mu.Lock()
		if traceEnabled(_traceLevelKeyEvents) {
			cm.traceLogfLocked("received RequestVoteReply %+v", reply)
		}

		if cm.cmState.state != Candidate {
			if traceEnabled(_traceLevelKeyEvents) {
				cm.traceLogfLocked("while waiting for reply, state = %v", cm.cmState.state)
			}
			cm.mu.Unlock()
			return
		}

		if reply.Term > cm.cmState.currentTerm {
			if traceEnabled(_traceLevelKeyEvents) {
				cm.traceLogfLocked("term out of date in RequestVoteReply")
			}
			cm.becomeFollowerLocked(reply.Term)
			cm.mu.Unlock()
			return
		} else if reply.Term == cm.cmState.currentTerm {
			if reply.VoteGranted {
				votesReceived.Add(1)
				if int(votesReceived.Load()) >= quorumSize(voterCount) {
					// Выиграл выборы!
					if traceEnabled(_traceLevelProgress) {
						cm.traceLogfLocked("wins election with %d votes", votesReceived.Load())
					}
					cm.startLeaderLocked()
					cm.mu.Unlock()
					slog.Info("wins election", slog.Int("votes", int(votesReceived.Load())))
					return
				}
			}
		}
		cm.mu.Unlock()
	}
}

// runElectionTimer — фоновый таймер выборов.
// Не запускается для узлов без права голоса.
// При истечении таймаута инициирует выборы или Pre‑Vote.
// Останавливается при смене терма, изменении роли, получении сигнала
// остановки таймера или завершении работы CM.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.cmState.currentTerm
	electionTimerDone := cm.cmState.electionTimerDone
	// Такт тикера читается под cm.mu и нормализуется: нулевое или
	// отрицательное значение (литеральный тестовый CM) заменяется умолчанием.
	tickerTimeout := cm.tickerTimeout
	if tickerTimeout <= 0 {
		tickerTimeout = DefaultTickerTimeout
	}

	// Nonvoter не участвует в выборах — не запускаем таймер.
	if !hasVote(cm.cmState.configurations.latest, cm.id) {
		cm.mu.Unlock()
		return
	}

	cm.mu.Unlock()
	if traceEnabled(_traceLevelLoops) {
		cm.traceLogf("election timer started (%v), term=%d", timeoutDuration, termStarted)
	}

	// Цикл таймера работает, пока не наступит одно из условий:
	//   - Истекло время ожидания без получения сообщений от лидера — тогда
	//     узел инициирует выборы (или Pre‑Vote, если включён).
	//   - Изменился текущий терм — значит, контекст выборов устарел.
	//   - Узел вышел из состояний Follower/Candidate (например, стал лидером).
	//   - Получен сигнал остановки таймера (electionTimerDone) или общий сигнал
	//     завершения работы модуля (shutdownCh).
	ticker := time.NewTicker(tickerTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cm.mu.Lock()
			if cm.cmState.state != Candidate && cm.cmState.state != Follower {
				if traceEnabled(_traceLevelLoops) {
					cm.traceLogfLocked("in election timer state=%s, bailing out", cm.cmState.state)
				}
				cm.mu.Unlock()
				return
			}

			if termStarted != cm.cmState.currentTerm {
				if traceEnabled(_traceLevelLoops) {
					cm.traceLogfLocked(
						"in election timer term changed from %d to %d, bailing out",
						termStarted, cm.cmState.currentTerm,
					)
				}
				cm.mu.Unlock()
				return
			}

			// Начать выборы (или Pre-Vote), если в течение времени ожидания мы
			// не получили сообщение от лидера или не проголосовали за кого-либо.
			if elapsed := time.Since(cm.cmState.electionResetEvent); elapsed >= timeoutDuration {
				cm.startCampaignLocked()
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()

		case <-electionTimerDone:
			return
		case <-cm.shutdownCh:
			return
		}
	}
}

// preVoteCampaign — идентичность и снимок кампании предварительного
// голосования на момент входа в PreCandidate: номер кампании, конфигурация и
// предлагаемый терм выборов.
type preVoteCampaign struct {
	generation   uint64
	cfg          Configuration
	proposedTerm int
}

// preVoteCampaignCurrentLocked сообщает, вправе ли кампания c управлять
// состоянием узла: узел остаётся PreCandidate, вошедшим в эту роль именно
// кампанией c, в терме, из которого она начата. Отмена необратима: выход из
// PreCandidate меняет роль, а повторный вход выдаёт новый номер.
//
// Требует удержания cm.mu.
func (cm *ConsensusModule) preVoteCampaignCurrentLocked(c preVoteCampaign) bool {
	return cm.cmState.state == PreCandidate &&
		cm.cmState.preVoteGeneration == c.generation &&
		cm.cmState.currentTerm+1 == c.proposedTerm
}

// startCampaignLocked исполняет решение истёкшего таймера выборов: выборы
// без PreVote (PreVote отключён или идёт передача лидерства) либо переход
// в PreCandidate с запуском кампании предварительного голосования.
//
// Переход в PreCandidate и выдача номера кампании выполняются в той же
// критической секции, что и решение таймера, а сетевая работа — в отдельной
// горутине после снятия блокировки. AppendEntries, смена терма или остановка,
// пришедшие позже, выводят узел из PreCandidate и тем отменяют кампанию.
//
// Требует удержания cm.mu.
func (cm *ConsensusModule) startCampaignLocked() {
	if cm.preVoteDisabled || cm.cmState.candidateFromLeadershipTransfer.Load() {
		cm.startElectionLocked()
		return
	}
	campaign, ok := cm.enterPreCandidateLocked()
	if !ok {
		return
	}
	run := func() { cm.runPreCandidate(campaign) }
	if hook := cm.preVoteWorkerHook; hook != nil {
		cm.goSpawnLocked(func() { hook(campaign, run) })
		return
	}
	cm.goSpawnLocked(run)
}

// enterPreCandidateLocked переводит узел в PreCandidate (не увеличивая
// currentTerm), выдаёт новой кампании предварительного голосования номер и
// возвращает её снимок. Одиночный голосующий узел сразу начинает выборы;
// тогда, как и при недопустимой роли, возвращается ok == false.
//
// Вход из состояния Candidate допускается: это retry-путь кандидата,
// проигравшего реальные выборы (split vote, когда два кандидата в одном
// терме голосуют каждый за себя и не набирают кворум). Без перехода
// Candidate → PreCandidate такой кандидат навсегда остался бы в Candidate:
// горутина таймера уже завершилась, и новые выборы больше никогда не
// запускались бы (livelock без лидера).
//
// Требует удержания cm.mu.
func (cm *ConsensusModule) enterPreCandidateLocked() (campaign preVoteCampaign, ok bool) {
	if cm.cmState.state != Follower && cm.cmState.state != PreCandidate && cm.cmState.state != Candidate {
		return preVoteCampaign{}, false
	}
	cm.cmState.state = PreCandidate
	cm.cmState.preVoteGeneration++
	if traceEnabled(_traceLevelKeyEvents) {
		cm.traceLogfLocked(
			"becomes PreCandidate; term=%d, len(log)=%d",
			cm.cmState.currentTerm, len(cm.cmState.log),
		)
	}

	cfg := cm.cmState.configurations.latest

	// Одиночный узел: PreVote не нужен, сразу выборы.
	if len(voterIDs(cfg)) <= 1 {
		cm.startElectionLocked()
		return preVoteCampaign{}, false
	}
	return preVoteCampaign{
		generation:   cm.cmState.preVoteGeneration,
		cfg:          cfg,
		proposedTerm: cm.cmState.currentTerm + 1,
	}, true
}

// runPreCandidate выполняет предварительное голосование (§4 Pre-Vote) узла,
// уже переведённого в PreCandidate вызовом enterPreCandidateLocked.
//
// Отправляет RequestPreVote всем голосующим и собирает ответы. Если получен
// кворум положительных ответов — вызывает startElectionLocked() для настоящих
// выборов. Если кворум не получен или обнаружен более высокий term —
// возвращается в Follower.
//
// Каждое решение принимается, только пока кампания актуальна
// (preVoteCampaignCurrentLocked). Отменённая кампания не начинает рассылку,
// не засчитывает ответы и не меняет роль и таймер узла: отменивший её переход
// уже запустил новый таймер выборов (becomeFollowerLocked,
// startElectionLocked) либо узел остановлен. Уже отправленный запрос не
// отзывается; его ответ просто не учитывается.
//
// Горутина завершается при:
//   - отмене кампании до рассылки → возврат без RPC
//   - получении кворума PreVote → вызов startElectionLocked()
//   - истечении таймаута выборов без кворума → becomeFollowerLocked()
//   - обнаружении более высокого терма от другого узла → becomeFollowerLocked()
//   - обнаружении отмены кампании → возврат без изменения состояния
//
// Запускается из startCampaignLocked, когда preVoteDisabled == false.
// Самостоятельно захватывает и освобождает cm.mu.
func (cm *ConsensusModule) runPreCandidate(c preVoteCampaign) {
	cm.mu.Lock()
	if !cm.preVoteCampaignCurrentLocked(c) {
		if traceEnabled(_traceLevelPreVote) {
			cm.traceLogfLocked(
				"runPreCandidate: stale campaign for term %d (state=%s, term=%d), bailing out",
				c.proposedTerm, cm.cmState.state, cm.cmState.currentTerm,
			)
		}
		cm.mu.Unlock()
		return
	}
	cm.mu.Unlock()

	voterCount := len(voterIDs(c.cfg))

	// Канал для сбора ответов.
	preVoteRespCh := make(chan *RequestPreVoteReply, voterCount-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Отправить RequestPreVote параллельно всем голосующим (кроме себя).
	for _, s := range c.cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		if s.Suffrage != Voter {
			continue
		}
		cm.goSpawn(func() {
			cm.sendPreVoteToPeer(ctx, peerID, c, preVoteRespCh)
		})
	}

	cm.collectPreVoteReplies(c, voterCount, preVoteRespCh)
}

// sendPreVoteToPeer отправляет RequestPreVote одному соседу и помещает его
// ответ в канал ответов. В запросе идёт предлагаемый терм выборов;
// собственный терм узла при этом не меняется. Если кампания уже отменена,
// запрос не отправляется. Если кампания отменена, транспорт отсутствует или
// вызов завершился ошибкой, в канал помещается синтетический ответ: отказ, а
// при отсутствии поддержки предварительного голосования у транспорта — грант,
// ради совместимости с соседями. Сборщик ответов проверяет актуальность
// кампании до учёта любого ответа, поэтому синтетический отказ отменённой
// кампании лишь будит сборщик. Каждая отправка уступает отмене контекста,
// поэтому горутина не зависает, когда сбор ответов уже завершён.
//
// Самостоятельно захватывает и освобождает cm.mu: под блокировкой проверяется
// актуальность кампании и снимаются последний индекс и терм журнала вместе с
// текущим термом; вызов транспорта происходит без блокировки. Канал в
// сигнатуре — только для отправки.
func (cm *ConsensusModule) sendPreVoteToPeer(
	ctx context.Context,
	peerID int,
	c preVoteCampaign,
	respCh chan<- *RequestPreVoteReply,
) {
	cm.mu.Lock()
	current := cm.preVoteCampaignCurrentLocked(c)
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTermLocked()
	savedTerm := cm.cmState.currentTerm
	cm.mu.Unlock()

	deny := func() {
		select {
		case respCh <- &RequestPreVoteReply{
			RPCHeader:   RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
			Term:        savedTerm,
			VoteGranted: false,
		}:
		case <-ctx.Done():
		}
	}
	if !current {
		deny()
		return
	}
	if cm.transport == nil {
		if traceEnabled(_traceLevelKeyEvents) {
			cm.traceLogf("runPreCandidate: transport is nil, cannot send to %d", peerID)
		}
		deny()
		return
	}

	args := RequestPreVoteArgs{
		RPCHeader: RPCHeader{
			ProtocolVersion: ProtocolVersion,
			ServerID:        cm.id,
		},
		Term:         c.proposedTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	if traceEnabled(_traceLevelPreVote) {
		cm.traceLogf("sending RequestPreVote to %d: %+v", peerID, args)
	}
	reply, err := cm.transport.RequestPreVote(ServerID(peerID), args)
	if err != nil {
		if traceEnabled(_traceLevelPreVote) {
			cm.traceLogf("RequestPreVote to %d failed: %v", peerID, err)
		}
		var resp *RequestPreVoteReply
		if err == contract.ErrNotImplemented {
			// Если транспорт не поддерживает PreVote, считаем голос
			// предоставленным.
			resp = &RequestPreVoteReply{
				RPCHeader:   RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
				Term:        savedTerm,
				VoteGranted: true,
			}
		} else {
			resp = &RequestPreVoteReply{
				RPCHeader:   RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
				Term:        savedTerm,
				VoteGranted: false,
			}
		}
		select {
		case respCh <- resp:
		case <-ctx.Done():
		}
		return
	}
	select {
	case respCh <- &reply:
	case <-ctx.Done():
	}
}

// collectPreVoteReplies собирает ответы соседей на предварительное голосование
// кампании c и решает его исход. Голос за себя учтён заранее. Каждый
// пришедший ответ засчитывается, только если кампания ещё актуальна; ответ
// отменённой кампании прекращает сбор без изменения состояния, даже если
// в нём более высокий терм. Ответ с более высоким термом в актуальной
// кампании возвращает узел в ведомые с этим термом. При наборе кворума узел
// выжидает случайную паузу — чтобы несколько узлов не начали выборы лидера
// одновременно с одинаковым термом — и, если кампания всё ещё актуальна,
// запускает выборы лидера. Истечение тайм-аута выборов и исчерпание ответов
// без кворума возвращают в ведомые только узел с актуальной кампанией;
// остановка узла прекращает сбор, не трогая состояния.
//
// Самостоятельно захватывает и освобождает cm.mu. Канал в сигнатуре — только
// для приёма.
//
//nolint:gocognit // проверки уровня механически повышают метрики, логика не меняется
func (cm *ConsensusModule) collectPreVoteReplies(
	c preVoteCampaign,
	voterCount int,
	respCh <-chan *RequestPreVoteReply,
) {
	grantedVotes := 1 // голос за себя
	neededVotes := quorumSize(voterCount)
	votersResponded := 0
	totalVoters := voterCount - 1

	timeout := time.After(cm.electionTimeout())

	// Собирать ответы от соседей.
	for votersResponded < totalVoters {
		select {
		case reply := <-respCh:
			votersResponded++
			cm.mu.Lock()
			if !cm.preVoteCampaignCurrentLocked(c) {
				if traceEnabled(_traceLevelPreVote) {
					cm.traceLogfLocked(
						"runPreCandidate: campaign is stale (state=%s), bailing out",
						cm.cmState.state,
					)
				}
				cm.mu.Unlock()
				return
			}
			if reply.Term > cm.cmState.currentTerm {
				if traceEnabled(_traceLevelPreVote) {
					cm.traceLogfLocked("runPreCandidate: found higher term %d", reply.Term)
				}
				cm.becomeFollowerLocked(reply.Term)
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
			if reply.VoteGranted {
				grantedVotes++
				if traceEnabled(_traceLevelPreVote) {
					cm.traceLogf(
						"runPreCandidate: granted vote from peer, total=%d, needed=%d",
						grantedVotes, neededVotes,
					)
				}
				if grantedVotes >= neededVotes {
					cm.startElectionAfterPreVote(c, grantedVotes)
					return
				}
			}
		case <-timeout:
			cm.mu.Lock()
			if cm.preVoteCampaignCurrentLocked(c) {
				if traceEnabled(_traceLevelPreVote) {
					cm.traceLogfLocked(
						"runPreCandidate: pre-vote timeout, returning to follower",
					)
				}
				cm.becomeFollowerLocked(cm.cmState.currentTerm)
			}
			cm.mu.Unlock()
			return
		case <-cm.shutdownCh:
			return
		}

		// После обработки всех ответов пересчитать необходимые голоса.
		if votersResponded >= totalVoters && grantedVotes < neededVotes {
			cm.mu.Lock()
			if cm.preVoteCampaignCurrentLocked(c) {
				if traceEnabled(_traceLevelPreVote) {
					cm.traceLogfLocked(
						"runPreCandidate: pre-vote lost (%d/%d), returning to follower",
						grantedVotes, neededVotes,
					)
				}
				cm.becomeFollowerLocked(cm.cmState.currentTerm)
			}
			cm.mu.Unlock()
			return
		}
	}
}

// startElectionAfterPreVote запускает выборы лидера после победы кампании c в
// предварительном голосовании. Перед переходом узел выжидает случайную паузу,
// чтобы несколько узлов не начали выборы одновременно с одинаковым термом, и
// начинает выборы, только если за время паузы кампания осталась актуальной.
//
// Самостоятельно захватывает и освобождает cm.mu: пауза выдерживается без
// блокировки, под блокировкой выполняются перепроверка кампании и запуск
// выборов.
func (cm *ConsensusModule) startElectionAfterPreVote(c preVoteCampaign, grantedVotes int) {
	// Jitter перед выборами, чтобы одновременно несколько узлов
	// не перешли в Candidate с одинаковым term (split vote).
	time.Sleep(time.Duration(rand.Intn(_preVoteJitterMs)) * time.Millisecond)
	cm.mu.Lock()
	if cm.preVoteCampaignCurrentLocked(c) {
		if traceEnabled(_traceLevelPreVote) {
			cm.traceLogfLocked(
				"runPreCandidate: won pre-vote with %d votes, starting election",
				grantedVotes,
			)
		}
		cm.startElectionLocked()
	}
	cm.mu.Unlock()
}

// electionTimeoutLocked генерирует псевдослучайную длительность тайм-аута
// выборов от базы cm.reelectionTimeout с сохранением миллисекундной
// дискретности: результат лежит в диапазоне [база; 2·база) с шагом 1 мс.
// База нормализуется условием d < time.Millisecond → умолчание:
// гарантирует, что аргумент rand.Intn(int(base/time.Millisecond)) ≥ 1,
// и закрывает положительные субмиллисекундные значения.
//
// Стресс-хук: при включённом _forcedReelectionHook в трети вызовов
// возвращается ровно база (рандомизация отключена — провоцирует
// коллизии выборов).
//
// Требует удержания cm.mu.
func (cm *ConsensusModule) electionTimeoutLocked() time.Duration {
	d := cm.reelectionTimeout
	if d < time.Millisecond {
		d = DefaultReelectionTimeout
	}
	base := d
	// Если установлен хук форсирования выборов, в трети случаев вернуть
	// ровно базу: намеренно частое совпадение тайм-аутов разных узлов
	// вызывает коллизии и рост числа перевыборов.
	if _forcedReelectionHook.Load() && rand.Intn(3) == 0 {
		return base
	}
	return base + time.Duration(rand.Intn(int(base/time.Millisecond)))*time.Millisecond
}

// electionTimeout — обёртка над electionTimeoutLocked для вызывающих без
// удержания cm.mu. Самостоятельно захватывает cm.mu и снимает её через
// defer; никогда не вызывается из-под cm.mu (sync.Mutex не реентерентен).
func (cm *ConsensusModule) electionTimeout() time.Duration {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.electionTimeoutLocked()
}

// timeoutNow обрабатывает входящий TimeoutNowRequest.
// Устанавливает candidateFromLeadershipTransfer и форсирует немедленные
// выборы (без PreVote, без ожидания election timeout).
func (cm *ConsensusModule) timeoutNow(rpc RPC, req *TimeoutNowRequest) {
	startTimeNow := time.Now()
	defer func() { cm.latency.timeoutNowRequest.observe(time.Since(startTimeNow)) }()
	if traceEnabled(_traceLevelKeyEvents) {
		cm.traceLogf("received TimeoutNow from %d", req.ServerID)
	}

	// Версия протокола проверяется до любого изменения состояния: запрос
	// несовместимой версии не останавливает таймер выборов и не начинает
	// выборы.
	if err := checkRPCHeader(req); err != nil {
		rpc.RespChan <- RPCResponse{Error: err}
		return
	}

	// Уже лидер — no-op.
	cm.mu.Lock()
	if cm.cmState.state == Leader {
		term := cm.cmState.currentTerm
		cm.mu.Unlock()
		rpc.RespChan <- RPCResponse{
			Reply: &TimeoutNowResponse{
				RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
				Success:   false,
				Term:      term,
			},
		}
		return
	}
	// Если не лидер, проверяем только состояние Follower (Candidate/PreCandidate
	// не могут получить TimeoutNow). Терм снимается под cm.mu до Unlock:
	// чтение cm.cmState.currentTerm вне критической секции — гонка данных.
	if cm.cmState.state != Follower {
		term := cm.cmState.currentTerm
		cm.mu.Unlock()
		rpc.RespChan <- RPCResponse{
			Reply: &TimeoutNowResponse{
				RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
				Success:   false,
				Term:      term,
			},
		}
		return
	}

	// Форсированная остановка текущего election timer.
	select {
	case <-cm.cmState.electionTimerDone:
	default:
		close(cm.cmState.electionTimerDone)
	}
	cm.cmState.electionTimerDone = make(chan struct{})
	cm.cmState.candidateFromLeadershipTransfer.Store(true)
	if traceEnabled(_traceLevelLoops) {
		cm.traceLogfLocked("candidateFromLeadershipTransfer set to true (cm.id=%d)", cm.id)
	}

	// Запускаем форсированные выборы немедленно, без ожидания election timeout.
	// cm.mu удерживается, что гарантирует атомарность.
	cm.startElectionLocked()
	newTerm := cm.cmState.currentTerm
	if traceEnabled(_traceLevelLoops) {
		cm.traceLogfLocked(
			"started immediate election after TimeoutNow, term=%d (cm.id=%d)",
			newTerm, cm.id,
		)
	}
	cm.mu.Unlock()

	rpc.RespChan <- RPCResponse{
		Reply: &TimeoutNowResponse{
			RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
			Success:   true,
			Term:      newTerm,
		},
	}
}
