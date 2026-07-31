package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"
)

// persistToStorage сохраняет постоянное состояние CM в cm.storage.
// Кодирование журнала выполняется только при cm.logNeedsPersist == true
// (т.е. когда лог действительно изменился), что позволяет избежать
// дорогого gob.Encode(cm.log) на каждом heartbeat или RPC.
func (cm *ConsensusModule) persistToStorage() {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		cm.traceLogfLocked(2, "persistToStorage elapsed %s", elapsed)
	}()
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	if cm.logNeedsPersist {
		var logData bytes.Buffer
		if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
			log.Fatal(err)
		}
		cm.storage.Set("log", logData.Bytes())
		cm.logNeedsPersist = false
	}

	var snapIdxData bytes.Buffer
	if err := gob.NewEncoder(&snapIdxData).Encode(cm.lastSnapshotIndex); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("lastSnapshotIndex", snapIdxData.Bytes())

	var snapTermData bytes.Buffer
	if err := gob.NewEncoder(&snapTermData).Encode(cm.lastSnapshotTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("lastSnapshotTerm", snapTermData.Bytes())
}

// restoreFromStorage восстанавливает постоянное состояние данного CM
// из хранилища. Должен вызываться в конструкторе до запуска какой-либо
// конкурентной работы.
func (cm *ConsensusModule) restoreFromStorage() {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
	cm.rebuildLastLog()
	cm.rebuildTermIndexMap()

	cm.rebuildConfigurations()

	if snapIdxData, found := cm.storage.Get("lastSnapshotIndex"); found {
		d := gob.NewDecoder(bytes.NewBuffer(snapIdxData))
		if err := d.Decode(&cm.lastSnapshotIndex); err != nil {
			log.Fatal(err)
		}
	}
	if snapTermData, found := cm.storage.Get("lastSnapshotTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(snapTermData))
		if err := d.Decode(&cm.lastSnapshotTerm); err != nil {
			log.Fatal(err)
		}
	}
}
