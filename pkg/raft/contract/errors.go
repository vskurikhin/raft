package contract

import "errors"

// ErrRaftShutdown — ошибка, возвращаемая операциями остановленного узла Raft.
var ErrRaftShutdown = errors.New("raft: raft is shutdown")

// ErrEnqueueTimeout — ошибка истечения срока ожидания постановки операции в очередь.
var ErrEnqueueTimeout = errors.New("raft: timeout enqueuing operation")

// ErrLogNotFound — маркерная ошибка отсутствия сохранённого журнала. LoadLog
// возвращает её, когда журнал ещё не создан; существующий пустой журнал даёт
// пустой срез и nil. Сравнивается через errors.Is; ошибка чтения или
// декодирования этой ошибкой не маскируется.
var ErrLogNotFound = errors.New("raft: log not found")
