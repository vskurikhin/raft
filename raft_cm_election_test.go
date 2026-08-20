package raft

import (
	"testing"
	"time"
)

// Стресс-хук выборов: при непустом значении переменной окружения
// electionTimeout() в трети вызовов возвращает ровно ReelectionTimeoutMs,
// то есть УБИРАЕТ рандомизацию, а не уменьшает таймаут.
const forcedReelectionEnv = "RAFT_FORCE_MORE_REELECTION"

const (
	// hookSamples — размер выборки значений electionTimeout() в каждой
	// ветви. M = 1000 достаточно для разделения двух распределений
	// с запасом в несколько десятков сигм (обоснование порогов ниже).
	hookSamples = 1000

	// wantHookedShare — нижняя граница доли значений, равных ровно
	// ReelectionTimeoutMs, при ВКЛЮЧЁННОМ хуке. Матожидание доли — 1/3
	// (rand.Intn(3) == 0); при M = 1000 σ = sqrt(p(1-p)/M) ≈ 0.0149,
	// поэтому порог 0.20 отстоит от матожидания примерно на 9σ.
	wantHookedShare = 0.20

	// maxUnhookedShare — верхняя граница той же доли при ВЫКЛЮЧЕННОМ
	// хуке. Там значение ReelectionTimeoutMs выпадает только при
	// rand.Intn(ReelectionTimeoutMs) == 0, то есть с вероятностью
	// 1/381 ≈ 0.26 %; при M = 1000 σ ≈ 0.0016, и порог 0.05 отстоит
	// от матожидания примерно на 30σ.
	maxUnhookedShare = 0.05
)

// TestElectionTimeout_ForcedReelectionHook проверяет стресс-хук
// RAFT_FORCE_MORE_REELECTION на том уровне, где его эффект наблюдаем —
// на распределении значений, возвращаемых electionTimeout().
// На уровне кластера эффекта нет и быть не может: хук не
// опускает таймаут ниже интервала heartbeat, а Pre-Vote запрещает рост
// терма при живом связном лидере, поэтому смена лидера от хука
// структурно невозможна — именно это проверяет
// TestClusterConvergence_ForcedReelections вызванными сменами лидера,
// а не хуком.
//
// Тест детерминирован по времени: ни кластера, ни горутин, ни sleep.
// electionTimeout() не читает полей ConsensusModule (проверено:
// raft_cm_election.go), поэтому достаточно «голого» &ConsensusModule{}.
//
// Мутационное свойство: при удалении или отключении ветви хука в
// electionTimeout() ветвь «хук включён» даёт долю ≈ 1/381 и падает.
func TestElectionTimeout_ForcedReelectionHook(t *testing.T) {
	// t.Setenv несовместим с t.Parallel — тест остаётся serial (§15).
	hooked := time.Duration(ReelectionTimeoutMs) * time.Millisecond

	// Порядок ветвей: сначала контрольная (хук выключен), затем с хуком.
	t.Run("hook disabled", func(t *testing.T) {
		// Пустое значение неотличимо от отсутствия переменной для
		// предиката production-кода (os.Getenv(...) != ""), но, в отличие
		// от него, не зависит от окружения, в котором запущен бинарник.
		t.Setenv(forcedReelectionEnv, "")

		share := hookedShare(t, hooked)
		if share > maxUnhookedShare {
			t.Errorf(
				"без хука доля значений ровно %v = %.4f, want <= %.2f (матожидание 1/%d)",
				hooked, share, maxUnhookedShare, ReelectionTimeoutMs,
			)
		}
	})

	t.Run("hook enabled", func(t *testing.T) {
		t.Setenv(forcedReelectionEnv, "1")

		share := hookedShare(t, hooked)
		if share < wantHookedShare {
			t.Errorf(
				"с хуком доля значений ровно %v = %.4f, want >= %.2f (матожидание 1/3)",
				hooked, share, wantHookedShare,
			)
		}
	})
}

// hookedShare снимает выборку hookSamples значений electionTimeout(),
// проверяет диапазон каждого значения и возвращает долю значений,
// равных ровно hooked.
func hookedShare(t *testing.T, hooked time.Duration) float64 {
	t.Helper()

	minTimeout := time.Duration(ReelectionTimeoutMs) * time.Millisecond
	maxTimeout := time.Duration(2*ReelectionTimeoutMs) * time.Millisecond

	cm := &ConsensusModule{}
	exact := 0
	for i := 0; i < hookSamples; i++ {
		got := cm.electionTimeout()
		if got < minTimeout || got >= maxTimeout {
			t.Fatalf(
				"electionTimeout() = %v вне диапазона [%v, %v) на выборке %d",
				got, minTimeout, maxTimeout, i,
			)
		}
		if got == hooked {
			exact++
		}
	}
	return float64(exact) / float64(hookSamples)
}
