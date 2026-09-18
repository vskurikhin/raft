package raft

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestPersistCallSitesMatchSources проверяет карту производственных вызовов
// сохранения по исходному коду: ровно одиннадцать точек, в каждой — свой
// уникальный источник, и ни одного вызова с иным или тестовым источником.
// Проверка читает только production-файлы пакета: прямые вызовы в тестах и
// бенчмарках с источником test карту не затрагивают.
func TestPersistCallSitesMatchSources(t *testing.T) {
	// Ожидаемая карта: файл → множество источников его вызовов.
	want := map[string]map[string]bool{
		"raft_cm_apply.go": {
			"persistSourceApply": true,
		},
		"raft_cm_election.go": {
			"persistSourceFollowerTerm": true,
			"persistSourceCandidate":    true,
		},
		"raft_cm_log.go": {
			"persistSourceLeaderAppend": true,
		},
		"raft_cm_rpc.go": {
			"persistSourceAEFinish":   true,
			"persistSourceAETransfer": true,
			"persistSourceAECommit":   true,
			"persistSourceVote":       true,
		},
		"raft_cm_snapshot.go": {
			"persistSourceInstallSnapshot": true,
			"persistSourceTakeSnapshot":    true,
		},
		"raft_cm_storage.go": {
			"persistSourceStartupRestore": true,
		},
	}

	got := map[string]map[string]int{}
	total := 0
	files, err := filepath.Glob("raft_cm_*.go")
	if err != nil {
		t.Fatalf("список production-файлов: %v", err)
	}
	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("разбор %s: %v", file, err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "persistToStorageLocked" {
				return true
			}
			if len(call.Args) != 1 {
				t.Errorf("%s: вызов сохранения с %d аргументами, want 1", file, len(call.Args))
				return true
			}
			source, ok := call.Args[0].(*ast.Ident)
			if !ok {
				t.Errorf("%s: источник вызова сохранения не является кодом", file)
				return true
			}
			if got[file] == nil {
				got[file] = map[string]int{}
			}
			got[file][source.Name]++
			total++
			return true
		})
	}

	if total != 11 {
		t.Fatalf("производственных вызовов сохранения %d, want 11", total)
	}
	for file, sources := range want {
		if len(got[file]) != len(sources) {
			t.Fatalf("%s: источников %v, want %v", file, got[file], sources)
		}
		for source := range sources {
			if got[file][source] != 1 {
				t.Fatalf("%s: источник %s встречается %d раз, want 1", file, source, got[file][source])
			}
		}
	}
	for file, sources := range got {
		for source := range sources {
			if !want[file][source] {
				t.Fatalf("%s: неизвестный производственный источник %s", file, source)
			}
		}
	}
}
