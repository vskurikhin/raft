package raft

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestDirtyMarkCallSites проверяет карту отметок грязного журнала по исходному
// коду: ровно четыре рабочие точки и начальная инициализация, каждая со своей
// группой происхождения, и ровно один сброс начального периода при
// восстановлении. Проверка читает только production-файлы пакета:
// прямые вызовы помощника с передаваемым временем в тестах карту не затрагивают.
func TestDirtyMarkCallSites(t *testing.T) {
	// Ожидаемая карта: файл → множество групп его отметок.
	want := map[string]map[string]bool{
		"raft.go": {
			"dirtyCauseInitial": true,
		},
		"raft_cm_log.go": {
			"dirtyCauseCompact":      true,
			"dirtyCauseLeaderAppend": true,
		},
		"raft_cm_rpc.go": {
			"dirtyCauseFollowerAppend": true,
		},
		"raft_cm_snapshot.go": {
			"dirtyCauseInstallSnapshot": true,
		},
	}

	got := map[string]map[string]int{}
	total, resets := 0, 0
	files, err := filepath.Glob("raft*.go")
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
			if !ok {
				return true
			}
			chain, ok := identifierChain(sel)
			if !ok {
				return true
			}
			switch chain {
			case "cm.dirty.mark", "cm.dirty.markAt":
				if len(call.Args) != 2 {
					t.Errorf("%s: отметка с %d аргументами, want 2", file, len(call.Args))
					return true
				}
				cause, ok := call.Args[0].(*ast.Ident)
				if !ok {
					t.Errorf("%s: группа отметки не является кодом", file)
					return true
				}
				if got[file] == nil {
					got[file] = map[string]int{}
				}
				got[file][cause.Name]++
				total++
			case "cm.dirty.cancelInitial":
				resets++
			}
			return true
		})
	}

	if total != 5 {
		t.Fatalf("производственных отметок %d, want 5", total)
	}
	if resets != 1 {
		t.Fatalf("сбросов начального периода %d, want 1", resets)
	}
	for file, causes := range want {
		if len(got[file]) != len(causes) {
			t.Fatalf("%s: группы %v, want %v", file, got[file], causes)
		}
		for cause := range causes {
			if got[file][cause] != 1 {
				t.Fatalf("%s: группа %s встречается %d раз, want 1", file, cause, got[file][cause])
			}
		}
	}
	for file, causes := range got {
		for cause := range causes {
			if !want[file][cause] {
				t.Fatalf("%s: неизвестная производственная группа отметки %s", file, cause)
			}
		}
	}
}
