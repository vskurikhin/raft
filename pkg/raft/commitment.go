package raft

import "sort"

// commitmentTracker отслеживает прогресс репликации записей журнала для
// лидера Raft. Получает обновления matchIndex от каждого узла-избирателя
// (voter) и вычисляет commitIndex на основе медианного значения
// (majority). При изменении commitIndex отправляет уведомление в commitCh.
//
// Алгоритм: при каждом обновлении matchIndex собираются все значения
// (только для voter'ов), сортируются, и commitIndex устанавливается
// равным медианному элементу (индекс len/2), но только если:
//   - запись с этим индексом принадлежит текущему term (Raft safety, §5.4.2),
//   - median >= startIndex (первый индекс текущего терма лидера).
//
// Если хотя бы одно из условий не выполнено, commitIndex не продвигается.
// При изменении commitIndex в канал commitCh отправляется уведомление
// (non-blocking send), что позволяет leaderLoop асинхронно обработать
// новый коммит без ожидания.
//
// Трекер НЕ является потокобезопасным. Вызывающий (leaderLoop) должен
// обеспечить sequential consistency.
type commitmentTracker struct {
	// matchIndex содержит пары voterID → последний известный совпадающий
	// индекс журнала. Только узлы-избиратели влияют на commitIndex.
	matchIndex map[int]int

	commitIndex int // текущий commitIndex (монотонно возрастает)
	selfID      int // идентификатор лидера
	startIndex  int // первый индекс, который может быть закоммичен в этом term
	commitCh    chan<- int
	term        int // текущий term лидера
}

// newCommitmentTracker создаёт трекер для лидера.
// selfID — идентификатор лидера.
// commitCh — канал для уведомления leaderLoop о новом commitIndex.
// term — текущий term лидера (используется для Raft safety, §5.4.2).
// startIndex — первый индекс, который может быть закоммичен в этом term.
// Начальный набор voter'ов пуст; вызывающий должен вызвать setConfiguration
// для инициализации списка голосующих узлов.
func newCommitmentTracker(
	selfID int,
	commitCh chan<- int,
	term int,
	startIndex int,
) *commitmentTracker {
	return &commitmentTracker{
		matchIndex:  make(map[int]int),
		commitIndex: -1,
		selfID:      selfID,
		startIndex:  startIndex,
		commitCh:    commitCh,
		term:        term,
	}
}

// setConfiguration устанавливает новый набор голосующих узлов (voter'ов).
// Вызывается при старте лидера и при каждом изменении конфигурации кластера
// (Feature 2 — Membership). Сохраняет существующие matchIndex для узлов,
// которые остаются voter'ами. Узлы, вышедшие из состава voter'ов, удаляются
// из matchIndex.
func (ct *commitmentTracker) setConfiguration(voters []int, lookupTerm func(int) int) {
	newMatchIndex := make(map[int]int, len(voters))
	for _, voter := range voters {
		if val, ok := ct.matchIndex[voter]; ok {
			newMatchIndex[voter] = val
		} else {
			newMatchIndex[voter] = -1
		}
	}
	ct.matchIndex = newMatchIndex
	ct.recalculateCommitIndex(lookupTerm)
}

// setMatch обновляет значение matchIndex для указанного узла.
// Если новый matchIndex меньше текущего — игнорируется (инвариант:
// matchIndex никогда не уменьшается). После обновления пересчитывает
// commitIndex через медиану.
//
// lookupTerm — функция для получения term записи журнала по индексу,
// необходима для проверки Raft safety.
func (ct *commitmentTracker) setMatch(peerID, index int, lookupTerm func(int) int) {
	if index < ct.matchIndex[peerID] {
		return
	}
	ct.matchIndex[peerID] = index
	ct.recalculateCommitIndex(lookupTerm)
}

// commit принудительно продвигает commitIndex через обновление matchIndex
// для selfID. Используется в dispatchLogs для single-node акселератора:
// лидер немедленно отмечает свою запись как совпадающую.
func (ct *commitmentTracker) commit(index int, lookupTerm func(int) int) {
	ct.matchIndex[ct.selfID] = index
	ct.recalculateCommitIndex(lookupTerm)
}

// getCommitIndex возвращает текущее значение commitIndex.
func (ct *commitmentTracker) getCommitIndex() int {
	return ct.commitIndex
}

// setTerm обновляет term и startIndex при смене терма лидера.
func (ct *commitmentTracker) setTerm(term int, startIndex int) {
	ct.term = term
	ct.startIndex = startIndex
}

// recalculateCommitIndex вычисляет новый commitIndex на основе медианы
// всех значений matchIndex. Для проверки Raft safety используются два
// условия: term записи равен текущему term лидера, и индекс >= startIndex.
//
// Медианный подход: сортируем все matchIndex, берём элемент с индексом n/2.
// Если запись по этому индексу удовлетворяет условиям безопасности,
// устанавливаем commitIndex = median и отправляем уведомление в commitCh.
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
	if lookupTerm(median) == ct.term && median >= ct.startIndex {
		ct.commitIndex = median
		select {
		case ct.commitCh <- ct.commitIndex:
		default:
		}
	}
}
