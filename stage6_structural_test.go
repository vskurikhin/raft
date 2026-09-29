package raft

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// measureCallNames — селекторы сетевого кодирования/измерения Data нового
// формата. Измерение выполняет gob-кодирование, поэтому его вызов под cm.mu
// удерживал бы блокировку консенсуса на время reflection.
var measureCallNames = map[string]bool{
	"protocol.MeasureData": true,
	"protocol.EncodeData":  true,
}

// TestProtocolMeasurementOutsideCMMutex — структурная проверка стоимости:
// ни одна функция корневого пакета, вызывающая сетевое измерение Data нового
// формата напрямую, не берёт cm.mu. Проверка охватывает и тела вложенных
// литералов функций: вызов измерения внутри замыкания под блокировкой также
// будет найден.
func TestProtocolMeasurementOutsideCMMutex(t *testing.T) {
	files := productionFiles(t, "*.go")
	fset := token.NewFileSet()
	checked := 0
	for _, file := range files {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("cannot parse %s: %v", file, err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if !callsAny(fn.Body, measureCallNames) {
				continue
			}
			checked++
			if locks := countCMLocks(fn.Body); locks != 0 {
				t.Errorf("%s: %s измеряет сетевую Data и берёт cm.mu %d раз(а): измерение обязано выполняться вне блокировки",
					file, fn.Name.Name, locks)
			}
		}
	}
	if checked == 0 {
		t.Fatal("не найдено ни одного места сетевого измерения Data: проверка потеряла предмет")
	}
}

// limitTogglePattern — имена, похожие на производственный выключатель
// проверки пределов. Такого выключателя в поставке быть не должно: отключение
// проверки меняет стоимость только вместе с корректностью.
var limitTogglePattern = regexp.MustCompile(`(?i)^(disable|skip|bypass|ignore|no)[A-Za-z]*(limit|preflight|measure|protocol)`)

// TestNoLimitBypassToggle — структурная проверка отсутствия производственного
// выключателя проверки пределов: среди имён функций, типов, полей и глобальных
// переменных корневого пакета нет включения/отключения измерения.
func TestNoLimitBypassToggle(t *testing.T) {
	files := productionFiles(t, "*.go")
	fset := token.NewFileSet()
	for _, file := range files {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("cannot parse %s: %v", file, err)
		}
		for _, decl := range parsed.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if limitTogglePattern.MatchString(d.Name.Name) {
					t.Errorf("%s: подозрительное имя функции %q", file, d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					checkToggleSpec(t, file, spec)
				}
			}
		}
	}
}

// checkToggleSpec проверяет имена объявлений общего блока на выключатель.
func checkToggleSpec(t *testing.T, file string, spec ast.Spec) {
	t.Helper()
	switch s := spec.(type) {
	case *ast.TypeSpec:
		if limitTogglePattern.MatchString(s.Name.Name) {
			t.Errorf("%s: подозрительное имя типа %q", file, s.Name.Name)
		}
		checkStructToggleFields(t, file, s.Type)
	case *ast.ValueSpec:
		for _, name := range s.Names {
			if limitTogglePattern.MatchString(name.Name) {
				t.Errorf("%s: подозрительное имя переменной %q", file, name.Name)
			}
		}
	}
}

// checkStructToggleFields проверяет поля структурных типов на выключатель.
func checkStructToggleFields(t *testing.T, file string, expr ast.Expr) {
	t.Helper()
	st, ok := expr.(*ast.StructType)
	if !ok || st.Fields == nil {
		return
	}
	for _, field := range st.Fields.List {
		for _, name := range field.Names {
			if limitTogglePattern.MatchString(name.Name) {
				t.Errorf("%s: подозрительное поле %q", file, name.Name)
			}
		}
	}
}

// productionFiles возвращает файлы корневого пакета без тестовых.
func productionFiles(t *testing.T, pattern string) []string {
	t.Helper()
	all, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("cannot list package files: %v", err)
	}
	var out []string
	for _, file := range all {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		out = append(out, file)
	}
	return out
}

// callsAny сообщает, содержит ли тело вызов с одним из указанных селекторов.
func callsAny(body *ast.BlockStmt, names map[string]bool) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if name, ok := identifierChain(call.Fun); ok && names[name] {
			found = true
			return false
		}
		return true
	})
	return found
}

// countCMLocks считает захваты cm.mu в теле функции, включая вложенные
// литералы функций.
func countCMLocks(body *ast.BlockStmt) int {
	count := 0
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := identifierChain(call.Fun)
		if ok && (strings.HasSuffix(name, ".mu.Lock") || strings.HasSuffix(name, ".mu.RLock")) {
			count++
		}
		return true
	})
	return count
}
