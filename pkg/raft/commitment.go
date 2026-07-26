package raft

import "sort"

// commitmentTracker отслеживает прогресс репликации записей журнала для
// лидера Raft. Получает обновления matchIndex от каждого peer-а и вычисляет
// commitIndex на основе большинства (majority) с использованием медианного
// метода.
//
// Алгоритм: при каждом обновлении matchIndex собираются все значения
// (включая собственный ID лидера), сортируются, и commitIndex устанавливается
// равным медианному элементу (индекс n/2), но только если запись с этим
// индексом принадлежит текущему term (Raft safety, §5.4.2). Если условие
// не выполнено, commitIndex не продвигается.
//
// При изменении commitIndex в канал commitCh отправляется уведомление
// (non-blocking send). Это позволяет leaderLoop асинхронно обработать
// новый коммит без ожидания.
type commitmentTracker struct {
	matchIndex  map[int]int
	commitIndex int
	selfID      int
	peerIDs     []int
	commitCh    chan<- int
	term        int
}

// newCommitmentTracker создаёт новый commitmentTracker.
// selfID — идентификатор лидера, peerIDs — список всех участников кластера
// (все peerIds, исключая selfID). commitCh — канал для уведомления leaderLoop.
// term — текущий term лидера, используется для проверки Raft safety.
func newCommitmentTracker(
	selfID int,
	peerIDs []int,
	commitCh chan<- int,
	term int,
) *commitmentTracker {
	matchIndex := make(map[int]int, len(peerIDs)+1)
	matchIndex[selfID] = -1
	for _, peerID := range peerIDs {
		matchIndex[peerID] = -1
	}
	return &commitmentTracker{
		matchIndex:  matchIndex,
		commitIndex: -1,
		selfID:      selfID,
		peerIDs:     peerIDs,
		commitCh:    commitCh,
		term:        term,
	}
}

// setMatch обновляет значение matchIndex для указанного peer-а.
// Если это приводит к изменению commitIndex (через медианный пересчёт),
// отправляет новое значение в commitCh (non-blocking send).
// Вызов не требует внешней блокировки, но предполагается, что
// matchIndex не будет конкурентно изменяться из разных goroutine.
//
// lookupTerm — функция для получения term записи журнала по индексу.
// Необходима для проверки Raft safety: коммитим только записи текущего term.
func (ct *commitmentTracker) setMatch(peerID, index int, lookupTerm func(int) int) {
	if index < ct.matchIndex[peerID] {
		return
	}
	ct.matchIndex[peerID] = index
	ct.recalculateCommitIndex(lookupTerm)
}

// commit принудительно продвигает commitIndex до указанного значения
// (используется для single-node акселератора в dispatchLogs).
// Обновляет matchIndex для selfID и пересчитывает commitIndex через медиану.
func (ct *commitmentTracker) commit(index int, lookupTerm func(int) int) {
	ct.matchIndex[ct.selfID] = index
	ct.recalculateCommitIndex(lookupTerm)
}

// getCommitIndex возвращает текущее значение commitIndex.
func (ct *commitmentTracker) getCommitIndex() int {
	return ct.commitIndex
}

// setTerm обновляет currentTerm трекера (вызывается при смене терма лидера).
func (ct *commitmentTracker) setTerm(term int) {
	ct.term = term
}

// recalculateCommitIndex вычисляет новый commitIndex на основе медианы
// всех значений matchIndex. Для проверки Raft safety используется lookupTerm,
// который возвращает term записи журнала по индексу.
//
// Медианный подход: сортируем все matchIndex, берём элемент с индексом n/2.
// Если запись по этому индексу принадлежит currentTerm, устанавливаем
// commitIndex = median. Иначе оставляем без изменений.
func (ct *commitmentTracker) recalculateCommitIndex(lookupTerm func(int) int) {
	if len(ct.matchIndex) == 0 {
		return
	}
	matchVals := make([]int, 0, len(ct.matchIndex))
	for _, idx := range ct.matchIndex {
		matchVals = append(matchVals, idx)
	}
	sort.Ints(matchVals)

	median := matchVals[len(matchVals)/2]
	if median <= ct.commitIndex {
		return
	}
	term := lookupTerm(median)
	if term == ct.term {
		ct.commitIndex = median
		select {
		case ct.commitCh <- ct.commitIndex:
		default:
		}
	}
}
