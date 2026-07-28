package raft

import (
	"context"
	"fmt"
	"os"
	"time"
)

const latencyChannelSize = 4096

// TODO refactoring
var (
	latencyAppendEntriesCh     = make(chan time.Duration, latencyChannelSize)
	latencyBatchingFSMCh       = make(chan time.Duration, latencyChannelSize)
	latencyElectionCh          = make(chan time.Duration, latencyChannelSize)
	latencyHandleFsmSnapshotCh = make(chan time.Duration, latencyChannelSize)
	latencyInstallSnapshotCh   = make(chan time.Duration, latencyChannelSize)
	latencyProcessLogsCh       = make(chan time.Duration, latencyChannelSize)
	latencyRequestPreVoteCh    = make(chan time.Duration, latencyChannelSize)
	latencyRequestVoteCh       = make(chan time.Duration, latencyChannelSize)
	latencySendBatchCh         = make(chan time.Duration, latencyChannelSize)
	latencyTakeSnapshotCh      = make(chan time.Duration, latencyChannelSize)
	latencyTimeoutNowRequestCh = make(chan time.Duration, latencyChannelSize)
)

// TODO
// - latencyAppendEntriesCh - Append entries elapsed
// - latencyBatchingFSMCh - FSM timer elapsed
// - latencyElectionCh - Election elapsed
// - latencyHandleFsmSnapshotCh- FSM snapshot handling elapsed
// - latencyInstallSnapshotCh - Install Snapshot elapsed
// - latencyProcessLogsCh - Logs processing elapsed
// - latencyRequestPreVoteCh - Request pre-vote elapsed
// - latencyRequestVoteCh - Request vote elapsed
// - latencySendBatchCh - Send batch elapsed
// - latencyTakeSnapshotCh - Take snapshot elapsed
// - latencyTimeoutNowRequestCh - TimeoutNow elapsed

/*
	startTimeNow := time.Now()
	defer func() { latencyHandleInstallSnapshotCh <- time.Since(startTimeNow) }()
*/

func (cm *ConsensusModule) stats(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var (
				appendEntriesLatencies     []float64
				batchingFSMLatencies       []float64
				electionLatencies          []float64
				handleFsmSnapshotLatencies []float64
				installSnapshotLatencies   []float64
				processLogsLatencies       []float64
				requestPreVoteLatencies    []float64
				requestVoteLatencies       []float64
				sendBatchLatencies         []float64
				takeSnapshotLatencies      []float64
				timeoutNowRequestLatencies []float64
			)
		drain:
			for {
				select {
				case d := <-latencyAppendEntriesCh:
					appendEntriesLatencies = append(appendEntriesLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyBatchingFSMCh:
					batchingFSMLatencies = append(batchingFSMLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyElectionCh:
					electionLatencies = append(electionLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyHandleFsmSnapshotCh:
					handleFsmSnapshotLatencies = append(handleFsmSnapshotLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyInstallSnapshotCh:
					installSnapshotLatencies = append(installSnapshotLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyProcessLogsCh:
					processLogsLatencies = append(processLogsLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyRequestPreVoteCh:
					requestPreVoteLatencies = append(requestPreVoteLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyRequestVoteCh:
					requestVoteLatencies = append(requestVoteLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencySendBatchCh:
					sendBatchLatencies = append(sendBatchLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyTakeSnapshotCh:
					takeSnapshotLatencies = append(takeSnapshotLatencies, float64(d.Microseconds())/1000.0)
				case d := <-latencyTimeoutNowRequestCh:
					timeoutNowRequestLatencies = append(timeoutNowRequestLatencies, float64(d.Microseconds())/1000.0)
				default:
					break drain
				}
			}
			ae := Mean(appendEntriesLatencies)
			batchingFSM := Mean(batchingFSMLatencies)
			election := Mean(electionLatencies)
			fsmSnapsh := Mean(handleFsmSnapshotLatencies)
			installSnapshot := Mean(installSnapshotLatencies)
			processLogs := Mean(processLogsLatencies)
			requestPreVote := Mean(requestPreVoteLatencies)
			requestVote := Mean(requestVoteLatencies)
			sendBatch := Mean(sendBatchLatencies)
			takeSnapshot := Mean(takeSnapshotLatencies)
			timeoutNowRequest := Mean(timeoutNowRequestLatencies)
			msg := fmt.Sprintf(
				"%v [%c,N:%d,T:%03d] AE=%5.2fms, BatchingFSM=%5.2fms, Election=%5.2fms, FSMSnapSh=%5.2fms,"+
					" InstSnapShot=%5.2fms, ProcessLog=%5.2fms, RqPVt=%5.2fms, RqVote=%5.2fms,"+
					" SendBatch=%5.2fms, TakeSnapshot=%5.2fms, TmOutNowRq=%5.2fms\n",
				time.Now().Format("2006-01-02 15:04:05.0000"),
				stateLetter(cm.state), cm.id, cm.currentTerm,
				ae, batchingFSM, election, fsmSnapsh,
				installSnapshot, processLogs, requestPreVote, requestVote,
				sendBatch, takeSnapshot, timeoutNowRequest,
			)
			_, _ = fmt.Fprint(os.Stdout, msg)
		}
	}
}

func Mean(latencies []float64) (result float64) {
	if n := len(latencies); n > 0 {
		result = SumFloat(latencies) / float64(n)
	}
	return result
}

func SumFloat(slice []float64) (sum float64) {
	for _, v := range slice {
		sum += v
	}
	return sum
}
