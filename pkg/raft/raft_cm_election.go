package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

// becomeFollower делает cm последователем и сбрасывает его состояние.
// Если cm был лидером, отправляет сигнал в stepDown, чтобы leaderLoop
// завершил работу и разрешил все ожидающие future с ErrLeadershipLost.
// Ожидается, что cm.mu будет заблокирован.
func (cm *ConsensusModule) becomeFollower(term int) {
	wasLeader := cm.cmState.state == Leader
	cm.traceLockedLogf(0, "becomes Follower with term=%d; len(log)=%v", term, len(cm.cmState.log))

	// Сбросить inflightAE для всех peer при потере лидерства.
	// Это гарантирует, что новый лидер начнёт с "чистого листа".
	// nil-проверка защищает от nil-элемента (peer вне конфигурации) —
	// Store на nil *atomic.Bool паникует без recover().
	for peerID := range cm.leaderState.inflightAE {
		if cm.leaderState.inflightAE[peerID] != nil {
			cm.leaderState.inflightAE[peerID].Store(false)
		}
	}

	cm.cmState.state = Follower
	if wasLeader {
		select {
		case cm.stepDown <- struct{}{}:
		default:
		}
	}
	if term > cm.cmState.currentTerm {
		cm.cmState.currentTerm = term
		cm.cmState.votedFor = -1
		cm.persistToStorage()
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
	go cm.runElectionTimer()
}

// startElection запускает новые выборы с этим CM в качестве кандидата.
// Ожидается, что cm.mu будет заблокирован.
//
// Если candidateFromLeadershipTransfer установлен, в RequestVoteArgs
// передаётся LeadershipTransfer=true, что bypass проверку наличия лидера
// на узлах-получателях. Флаг сбрасывается после того, как все горутины
// отправили RequestVote, чтобы последующие обычные выборы не имели этого флага.
//
// Nonvoter'ы исключаются из рассылки RequestVote — они не участвуют
// в голосовании и не должны получать запросы на предоставление голоса.
//
//nolint:funlen
func (cm *ConsensusModule) startElection() {
	startTimeNow := time.Now()
	defer func() { cm.latency.election.observe(time.Since(startTimeNow)) }()
	cm.cmState.state = Candidate
	cm.cmState.currentTerm += 1
	savedCurrentTerm := cm.cmState.currentTerm
	cm.cmState.electionResetEvent = time.Now()
	cm.cmState.votedFor = cm.id
	cm.persistToStorage()
	cm.traceLockedLogf(0, "becomes Candidate (currentTerm=%d); len(log)=%v", savedCurrentTerm, len(cm.cmState.log))

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
		cm.startLeader()
		return
	}

	var votesReceived atomic.Int32
	votesReceived.Store(1)

	// Параллельно отправить RPC-запросы RequestVote всем voter'ам.
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		if s.Suffrage != Voter {
			continue
		}
		go func(peerID int, lt bool) { // TODO funlen
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
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
				LeadershipTransfer: lt,
			}

			cm.traceLogf(0, "sending RequestVote to %d: %+v", peerID, args)
			reply, err := cm.transport.RequestVote(ServerID(peerID), args)
			if err == nil {
				cm.mu.Lock()
				cm.traceLockedLogf(0, "received RequestVoteReply %+v", reply)

				if cm.cmState.state != Candidate {
					cm.traceLockedLogf(0, "while waiting for reply, state = %v", cm.cmState.state)
					cm.mu.Unlock()
					return
				}

				if reply.Term > cm.cmState.currentTerm {
					cm.traceLockedLogf(0, "term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					cm.mu.Unlock()
					return
				} else if reply.Term == cm.cmState.currentTerm {
					if reply.VoteGranted {
						votesReceived.Add(1)
						if int(votesReceived.Load()) >= quorumSize(voterCount) {
							// Выиграл выборы!
							cm.traceLockedLogf(2, "wins election with %d votes", votesReceived.Load())
							cm.startLeader()
							cm.mu.Unlock()
							slog.Info("wins election", slog.Int("votes", int(votesReceived.Load())))
							return
						}
					}
				}
				cm.mu.Unlock()
			}
		}(peerID, isLeadershipTransfer)
	}

	// Запустить новый таймер выборов на случай, если текущие выборы не завершатся успешно.
	go cm.runElectionTimer()
}

// runElectionTimer реализует таймер выборов. Она должна запускаться всякий раз, когда
// мы хотим запустить таймер для выдвижения в качестве кандидата на новых выборах.
//
// Эта функция является блокирующей и должна запускаться в отдельной горутине;
// она предназначена для работы с одним (одноразовым) таймером выборов, поскольку она завершается
// всякий раз, когда состояние CM меняется с «ведомый/кандидат» или меняется срок полномочий.
// runElectionTimer запускает таймер выборов для узла. Если узел не является
// voter'ом (Suffrage == Nonvoter), таймер немедленно завершается — nonvoter
// никогда не участвует в выборах и не может стать кандидатом.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.cmState.currentTerm
	electionTimerDone := cm.cmState.electionTimerDone

	// Nonvoter не участвует в выборах — не запускаем таймер.
	if !hasVote(cm.cmState.configurations.latest, cm.id) {
		cm.mu.Unlock()
		return
	}

	cm.mu.Unlock()
	cm.traceLogf(1, "election timer started (%v), term=%d", timeoutDuration, termStarted)

	// Этот цикл выполняется до тех пор, пока не произойдёт одно из двух:
	// - мы не обнаружим, что таймер выборов больше не нужен, или
	// - таймер выборов не истечёт и данный CM не станет кандидатом.
	// Для ведомого узла этот цикл обычно продолжает работать в фоновом режиме
	// в течение всего времени жизни CM.
	ticker := time.NewTicker(TickerTimeoutMs * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cm.mu.Lock()
			if cm.cmState.state != Candidate && cm.cmState.state != Follower {
				cm.traceLockedLogf(1, "in election timer state=%s, bailing out", cm.cmState.state)
				cm.mu.Unlock()
				return
			}

			if termStarted != cm.cmState.currentTerm {
				cm.traceLockedLogf(
					1, "in election timer term changed from %d to %d, bailing out",
					termStarted, cm.cmState.currentTerm,
				)
				cm.mu.Unlock()
				return
			}

			// Начать выборы (или Pre-Vote), если в течение времени ожидания мы
			// не получили сообщение от лидера или не проголосовали за кого-либо.
			if elapsed := time.Since(cm.cmState.electionResetEvent); elapsed >= timeoutDuration {
				if cm.preVoteDisabled || cm.cmState.candidateFromLeadershipTransfer.Load() {
					cm.startElection()
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				go cm.runPreCandidate()
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

// runPreCandidate выполняет предварительное голосование (§4 Pre-Vote).
//
// Узел переходит в PreCandidate (не увеличивая currentTerm), отправляет
// RequestPreVote всем voter'ам и собирает ответы. Если получен кворум
// положительных ответов — вызывает startElection() для настоящих выборов.
// Если кворум не получен или обнаружен более высокий term — возвращается
// в Follower.
//
// Вызов из состояния Candidate допускается: это retry-путь кандидата,
// проигравшего реальные выборы (split vote, когда два кандидата в одном
// терме голосуют каждый за себя и не набирают кворум). Таймер выборов
// (runElectionTimer) передаёт управление сюда независимо от состояния, и
// без перехода Candidate → PreCandidate такой кандидат навсегда остался бы
// в Candidate: runPreCandidate выходил бы сразу, а горутина таймера уже
// завершилась, поэтому новые выборы больше никогда не запускались бы
// (livelock без лидера).
//
// Горутина завершается при:
//   - получении кворума PreVote → вызов startElection()
//   - истечении election timeout без кворума → becomeFollower()
//   - обнаружении более высокого term от другого узла → becomeFollower()
//
// Вызывается из runElectionTimer(), когда preVoteDisabled == false.
//
//nolint:gocognit,funlen
func (cm *ConsensusModule) runPreCandidate() {
	cm.mu.Lock()
	if cm.cmState.state != Follower && cm.cmState.state != PreCandidate && cm.cmState.state != Candidate {
		cm.mu.Unlock()
		return
	}
	cm.cmState.state = PreCandidate
	cm.traceLockedLogf(0, "becomes PreCandidate; term=%d, len(log)=%d", cm.cmState.currentTerm, len(cm.cmState.log))

	cfg := cm.cmState.configurations.latest
	voters := voterIDs(cfg)
	voterCount := len(voters)

	// Одиночный узел: PreVote не нужен, сразу выборы.
	if voterCount <= 1 {
		cm.startElection()
		cm.mu.Unlock()
		return
	}

	proposedTerm := cm.cmState.currentTerm + 1
	cm.mu.Unlock()

	// Канал для сбора ответов.
	preVoteRespCh := make(chan *RequestPreVoteReply, voterCount-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Отправить RequestPreVote параллельно всем voter'ам (кроме себя).
	for _, s := range cfg.ConfigServers {
		peerID := int(s.ID)
		if peerID == cm.id {
			continue
		}
		if s.Suffrage != Voter {
			continue
		}
		go func(peerID int) {
			cm.mu.Lock()
			lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
			savedTerm := cm.cmState.currentTerm
			cm.mu.Unlock()

			if cm.transport == nil {
				cm.traceLogf(0, "runPreCandidate: transport is nil, cannot send to %d", peerID)
				select {
				case preVoteRespCh <- &RequestPreVoteReply{
					RPCHeader:   RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
					Term:        savedTerm,
					VoteGranted: false,
				}:
				case <-ctx.Done():
				}
				return
			}

			args := RequestPreVoteArgs{
				RPCHeader: RPCHeader{
					ProtocolVersion: ProtocolVersion,
					ServerID:        cm.id,
				},
				Term:         proposedTerm,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			cm.traceLogf(4, "sending RequestPreVote to %d: %+v", peerID, args)
			reply, err := cm.transport.RequestPreVote(ServerID(peerID), args)
			if err != nil {
				cm.traceLogf(4, "RequestPreVote to %d failed: %v", peerID, err)
				var resp *RequestPreVoteReply
				if err == ErrNotImplemented {
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
				case preVoteRespCh <- resp:
				case <-ctx.Done():
				}
				return
			}
			select {
			case preVoteRespCh <- &reply:
			case <-ctx.Done():
			}
		}(peerID)
	}

	grantedVotes := 1 // голос за себя
	neededVotes := quorumSize(voterCount)
	votersResponded := 0
	totalVoters := voterCount - 1

	timeout := time.After(cm.electionTimeout())

	// Собирать ответы от peer'ов.
	for votersResponded < totalVoters {
		select {
		case reply := <-preVoteRespCh:
			votersResponded++
			cm.mu.Lock()
			if cm.cmState.state != PreCandidate {
				cm.traceLockedLogf(4, "runPreCandidate: state changed to %s, bailing out", cm.cmState.state)
				cm.mu.Unlock()
				return
			}
			if reply.Term > cm.cmState.currentTerm {
				cm.traceLockedLogf(4, "runPreCandidate: found higher term %d", reply.Term)
				cm.becomeFollower(reply.Term)
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
			if reply.VoteGranted {
				grantedVotes++
				cm.traceLogf(
					4, "runPreCandidate: granted vote from peer, total=%d, needed=%d",
					grantedVotes, neededVotes,
				)
				if grantedVotes >= neededVotes {
					// Jitter перед выборами, чтобы одновременно несколько узлов
					// не перешли в Candidate с одинаковым term (split vote).
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
					cm.mu.Lock()
					if cm.cmState.state == PreCandidate {
						cm.traceLockedLogf(
							4, "runPreCandidate: won pre-vote with %d votes, starting election",
							grantedVotes,
						)
						cm.startElection()
					}
					cm.mu.Unlock()
					return
				}
			}
		case <-timeout:
			cm.mu.Lock()
			if cm.cmState.state == PreCandidate {
				cm.traceLockedLogf(4, "runPreCandidate: pre-vote timeout, returning to follower")
				cm.becomeFollower(cm.cmState.currentTerm)
			}
			cm.mu.Unlock()
			return
		case <-cm.shutdownCh:
			return
		}

		// После обработки всех ответов пересчитать необходимые голоса.
		if votersResponded >= totalVoters && grantedVotes < neededVotes {
			cm.mu.Lock()
			if cm.cmState.state == PreCandidate {
				cm.traceLockedLogf(
					4, "runPreCandidate: pre-vote lost (%d/%d), returning to follower",
					grantedVotes, neededVotes,
				)
				cm.becomeFollower(cm.cmState.currentTerm)
			}
			cm.mu.Unlock()
			return
		}
	}
}

// electionTimeout генерирует псевдослучайную длительность тайм-аута выборов.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// Если установлен параметр RAFT_FORCE_MORE_REELECTION, проведите стресс-тест, намеренно
	// генерируя жестко заданное число очень часто. Это вызовет коллизии
	// между различными серверами и приведет к увеличению количества перевыборов.
	if os.Getenv("RAFT_FORCE_MORE_REELECTION") != "" && rand.Intn(3) == 0 {
		return time.Duration(ReelectionTimeoutMs) * time.Millisecond
	}
	return time.Duration(ReelectionTimeoutMs+rand.Intn(ReelectionTimeoutMs)) * time.Millisecond
}

// timeoutNow обрабатывает входящий TimeoutNowRequest.
// Устанавливает candidateFromLeadershipTransfer и форсирует немедленные
// выборы (без PreVote, без ожидания election timeout).
func (cm *ConsensusModule) timeoutNow(rpc RPC, req *TimeoutNowRequest) {
	startTimeNow := time.Now()
	defer func() { cm.latency.timeoutNowRequest.observe(time.Since(startTimeNow)) }()
	cm.traceLogf(0, "received TimeoutNow from %d", req.ServerID)

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
	// не могут получить TimeoutNow).
	if cm.cmState.state != Follower {
		cm.mu.Unlock()
		rpc.RespChan <- RPCResponse{
			Reply: &TimeoutNowResponse{
				RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
				Success:   false,
				Term:      cm.cmState.currentTerm,
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
	cm.traceLockedLogf(1, "candidateFromLeadershipTransfer set to true (cm.id=%d)", cm.id)

	// Запускаем форсированные выборы немедленно, без ожидания election timeout.
	// cm.mu удерживается, что гарантирует атомарность.
	cm.startElection()
	newTerm := cm.cmState.currentTerm
	cm.traceLockedLogf(1, "started immediate election after TimeoutNow, term=%d (cm.id=%d)", newTerm, cm.id)
	cm.mu.Unlock()

	rpc.RespChan <- RPCResponse{
		Reply: &TimeoutNowResponse{
			RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersion, ServerID: cm.id},
			Success:   true,
			Term:      newTerm,
		},
	}
}
