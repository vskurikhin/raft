package pkg

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/vskurikhin/raft"
	"github.com/vskurikhin/raft/pkg/kvclient"
	"github.com/vskurikhin/raft/pkg/kvservice"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

// Harness Тестовый стенд для системного тестирования kvservice и клиента.
//
// Владение состоянием: kvServiceAddrs, connected и
// alive защищены h.mu — адреса сервисов меняются при рестарте на :0
// (новый порт), поэтому читатели обязаны брать их под блокировкой.
// Инвариант границ: h.mu не удерживается через блокирующие
// операции жизненного цикла (Shutdown сервисов, ConnectToRaftPeer).
type Harness struct {
	mu sync.Mutex

	n int

	// kvCluster — список всех экземпляров KVService, участвующих в кластере.
	// Индекс сервиса в этом списке является его идентификатором (ID) в кластере.
	kvCluster []*kvservice.KVService

	// kvServiceAddrs — список HTTP-адресов (localhost:<PORT>), на которых
	// сервисы KV принимают клиентские команды.
	kvServiceAddrs []string

	storage []*raft.MapStorage

	t *testing.T

	// connected содержит по одному логическому значению для каждого сервера
	// кластера и указывает, подключён ли сервер в данный момент к остальным
	// узлам (если false, сервер изолирован, и сообщения не передаются ни к нему,
	// ни от него).
	connected []bool

	// alive содержит по одному логическому значению для каждого сервера
	// кластера и указывает, работает ли он в данный момент (false означает,
	// что сервер был аварийно остановлен и ещё не перезапущен).
	// Если connected == true, то сервер обязательно считается работающим.
	alive []bool

	// ctx — контекст, используемый HTTP-клиентом при выполнении команд в тестах.
	// ctxCancel — функция для его отмены.
	ctx       context.Context
	ctxCancel func()
}

func NewHarness(t *testing.T, n int) *Harness {
	kvss := make([]*kvservice.KVService, n)
	ready := make(chan any)
	connected := make([]bool, n)
	alive := make([]bool, n)
	storage := make([]*raft.MapStorage, n)

	// Создать все экземпляры KVService, входящие в этот кластер.
	for i := range n {
		peerIds := make([]int, 0)
		for p := range n {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = raft.NewMapStorage()
		kvss[i] = kvservice.NewKVService(":0", i, peerIds, storage[i], ready)
		alive[i] = true
	}

	// Соединить Raft-узлы всех сервисов между собой и закрыть канал ready,
	// уведомив их о том, что кластер полностью готов к работе.
	for i := range n {
		for j := range n {
			if i != j {
				_ = kvss[i].ConnectToRaftPeer(j, kvss[j].GetRaftListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	// Каждый экземпляр KVService обслуживает REST API на своём TCP-порту.
	// Порт назначает ядро (":0"): фиксированные порты приводили к конфликтам
	// при параллельном запуске тестовых процессов.
	kvServiceAddrs := make([]string, n)
	for i := range n {
		if err := kvss[i].ServeHTTP(":0"); err != nil {
			t.Fatalf("ServeHTTP for service %d: %v", i, err)
		}
		kvServiceAddrs[i] = clientAddr(t, kvss[i])
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	h := &Harness{
		n:              n,
		kvCluster:      kvss,
		kvServiceAddrs: kvServiceAddrs,
		t:              t,
		connected:      connected,
		alive:          alive,
		storage:        storage,
		ctx:            ctx,
		ctxCancel:      ctxCancel,
	}
	return h
}

func (h *Harness) DisconnectServiceFromPeers(id int) {
	tlog("Disconnect %d", id)
	h.kvCluster[id].DisconnectFromAllRaftPeers()
	for j := 0; j < h.n; j++ {
		if j != id {
			_ = h.kvCluster[j].DisconnectFromRaftPeer(id)
		}
	}
	h.mu.Lock()
	h.connected[id] = false
	h.mu.Unlock()
}

func (h *Harness) ReconnectServiceToPeers(id int) {
	tlog("Reconnect %d", id)
	alive := h.aliveSnapshot()
	for j := 0; j < h.n; j++ {
		if j != id && alive[j] {
			if err := h.kvCluster[id].ConnectToRaftPeer(j, h.kvCluster[j].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.kvCluster[j].ConnectToRaftPeer(id, h.kvCluster[id].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.mu.Lock()
	h.connected[id] = true
	h.mu.Unlock()
}

// clientAddr переводит фактический адрес слушателя сервиса
// (":0" даёт вид "[::]:PORT" — wildcard, непригодный для набора)
// в адрес "localhost:PORT", по которому обращаются тестовые клиенты.
func clientAddr(t *testing.T, kvs *kvservice.KVService) string {
	t.Helper()
	addr := kvs.GetHTTPListenAddr()
	if addr == "" {
		t.Fatal("service has no HTTP listen address; ServeHTTP was not called")
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("cannot parse HTTP listen address %q: %v", addr, err)
	}
	return net.JoinHostPort("localhost", port)
}

// aliveSnapshot возвращает копию среза alive, снятую под h.mu.
// Контракт: вызывается БЕЗ удерживаемой h.mu.
func (h *Harness) aliveSnapshot() []bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]bool, h.n)
	copy(out, h.alive)
	return out
}

// connectedSnapshot возвращает копию среза connected, снятую под h.mu.
// Контракт: вызывается БЕЗ удерживаемой h.mu.
func (h *Harness) connectedSnapshot() []bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]bool, h.n)
	copy(out, h.connected)
	return out
}

// aliveAddrs возвращает копию адресов работающих сервисов, снятую под
// h.mu. Копия обязательна: срез h.kvServiceAddrs обновляется при
// рестарте сервиса на новом порту.
func (h *Harness) aliveAddrs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	addrs := make([]string, 0, h.n)
	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}
	return addrs
}

// CrashService «аварийно завершает» работу сервиса, отключая его от всех
// соседних узлов, а затем корректно останавливая. Этот экземпляр сервиса
// больше не будет использоваться.
func (h *Harness) CrashService(id int) {
	tlog("Crash %d", id)
	h.DisconnectServiceFromPeers(id)
	h.mu.Lock()
	h.alive[id] = false
	h.mu.Unlock()
	if err := h.kvCluster[id].Shutdown(); err != nil {
		h.t.Errorf("error while shutting down service %d: %v", id, err)
	}
}

// RestartService «перезапускает» сервис, создавая новый экземпляр и
// подключая его к соседним узлам.
func (h *Harness) RestartService(id int) {
	if h.aliveSnapshot()[id] {
		log.Fatalf("id=%d is alive in RestartService", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := range h.n {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}
	ready := make(chan any)
	h.kvCluster[id] = kvservice.NewKVService(":0", id, peerIds, h.storage[id], ready)
	// Рестарт на ":0" даёт НОВЫЙ порт: адрес сервиса обязан быть обновлён,
	// иначе клиенты, собранные из kvServiceAddrs, обращались бы к порту
	// прежней инкарнации.
	if err := h.kvCluster[id].ServeHTTP(":0"); err != nil {
		h.t.Fatalf("ServeHTTP for restarted service %d: %v", id, err)
	}
	addr := clientAddr(h.t, h.kvCluster[id])

	h.ReconnectServiceToPeers(id)
	close(ready)
	h.mu.Lock()
	h.kvServiceAddrs[id] = addr
	h.alive[id] = true
	h.mu.Unlock()
	// keep: окно без наблюдаемого признака — перезапущенный сервис
	// поднимает Raft-узел и HTTP-сервер асинхронно от вызывающего;
	// бюджет 20*Quantum = 60 мс покрывает установку соединений
	// с соседями до первых клиентских запросов.
	time.Sleep(20 * raft.Quantum * time.Millisecond)
}

// DisableHTTPResponsesFromService заставляет указанный сервис перестать
// отвечать на HTTP-запросы клиентов (при этом он продолжит выполнять
// запрошенные операции).
func (h *Harness) DisableHTTPResponsesFromService(id int) {
	tlog("Disabling HTTP responses from %d", id)
	h.kvCluster[id].ToggleHTTPResponsesEnabled(false)
}

func (h *Harness) Shutdown() {
	for i := range h.n {
		h.kvCluster[i].DisconnectFromAllRaftPeers()
		h.mu.Lock()
		h.connected[i] = false
		h.mu.Unlock()
	}

	// Эти вызовы помогают HTTP-серверу внутри KVService корректно завершить работу.
	http.DefaultClient.CloseIdleConnections()
	h.ctxCancel()

	for i := range h.n {
		// Короткая секция h.mu: сам Shutdown сервиса выполняется вне её.
		h.mu.Lock()
		wasAlive := h.alive[i]
		h.alive[i] = false
		h.mu.Unlock()

		if wasAlive {
			if err := h.kvCluster[i].Shutdown(); err != nil {
				h.t.Errorf("error while shutting down service %d: %v", i, err)
			}
		}
	}
}

// NewClient создает нового клиента, который будет обращаться ко всем
// существующим работающим сервисам.
func (h *Harness) NewClient() *kvclient.KVClient {
	return kvclient.New(h.aliveAddrs())
}

// NewClientWithRandomAddrsOrder создает нового клиента, который будет
// обращаться ко всем существующим работающим сервисам, но в случайном
// порядке адресов.
func (h *Harness) NewClientWithRandomAddrsOrder() *kvclient.KVClient {
	addrs := h.aliveAddrs()
	// keep: легитимная рандомизация порядка адресов — предмет теста
	// (клиент обязан находить лидера при любом порядке). Инъекция
	// детерминированного RNG отклонена.
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return kvclient.New(addrs)
}

// NewClientSingleService создает нового клиента, который будет обращаться
// только к одному сервису (указанному по id). Обратите внимание, что если
// этот сервис не является лидером, клиент может бесконечно выполнять
// повторные попытки.
func (h *Harness) NewClientSingleService(id int) *kvclient.KVClient {
	// Копия, а не срез поверх общего массива: aliasing позволял клиенту
	// увидеть адрес другой инкарнации после RestartService.
	h.mu.Lock()
	addrs := []string{h.kvServiceAddrs[id]}
	h.mu.Unlock()
	return kvclient.New(addrs)
}

// ServiceAddr возвращает текущий HTTP-адрес сервиса id (под h.mu:
// адрес меняется при RestartService на ":0").
func (h *Harness) ServiceAddr(id int) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.kvServiceAddrs[id]
}

// CheckSingleLeader проверяет, что только один сервер считает себя лидером.
// Возвращает идентификатор лидера в кластере Raft. Если лидер еще не выбран,
// метод несколько раз повторяет проверку, поэтому его также удобно
// использовать для ожидания завершения выборов и готовности кластера
// выполнять команды.
func (h *Harness) CheckSingleLeader() int {
	for r := 0; r < 8; r++ {
		connected := h.connectedSnapshot()
		leaderId := -1
		for i := range h.n {
			if connected[i] && h.kvCluster[i].IsLeader() {
				if leaderId < 0 {
					leaderId = i
				} else {
					h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
				}
			}
		}
		if leaderId >= 0 {
			return leaderId
		}
		// poll-интервал condition-wait (не фиксированная пауза).
		time.Sleep(150 * raft.Quantum * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1
}

// Try*/Check* — две формы одних и тех же проверок.
//
// Try*-варианты возвращают ошибку и НЕ обращаются к *testing.T: только
// они допустимы внутри worker-горутин (t.Error*/t.Fatal* из горутины
// после конца теста — гонка и паника). Check*-варианты — тонкие обёртки,
// репортящие ту же ошибку через h.t; они вызываются только из тестовой
// горутины.

// TryPut отправляет через клиента c запрос Put. Возвращает
// (prevValue, keyFound, error) без обращения к *testing.T.
func (h *Harness) TryPut(c *kvclient.KVClient, key, value string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(h.ctx, 600*raft.Quantum*time.Millisecond)
	defer cancel()
	return c.Put(ctx, key, value)
}

// TryGet отправляет через клиента c запрос Get и проверяет, что ключ
// найден и его значение совпадает с ожидаемым. Возвращает ошибку
// без обращения к *testing.T.
func (h *Harness) TryGet(c *kvclient.KVClient, key string, wantValue string) error {
	ctx, cancel := context.WithTimeout(h.ctx, 600*raft.Quantum*time.Millisecond)
	defer cancel()
	gv, f, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	if !f {
		return fmt.Errorf("got found=false, want true for key=%s", key)
	}
	if gv != wantValue {
		return fmt.Errorf("got value=%v, want %v for key=%s", gv, wantValue, key)
	}
	return nil
}

// TryCAS отправляет через клиента c запрос CAS. Возвращает
// (prevValue, keyFound, error) без обращения к *testing.T.
func (h *Harness) TryCAS(c *kvclient.KVClient, key, compare, value string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(h.ctx, 800*raft.Quantum*time.Millisecond)
	defer cancel()
	return c.CAS(ctx, key, compare, value)
}

// TryGetNotFound отправляет через клиента c запрос Get и проверяет,
// что ключ отсутствует. Возвращает ошибку без обращения к *testing.T.
func (h *Harness) TryGetNotFound(c *kvclient.KVClient, key string) error {
	ctx, cancel := context.WithTimeout(h.ctx, 500*raft.Quantum*time.Millisecond)
	defer cancel()
	_, f, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	if f {
		return fmt.Errorf("got found=true, want false for key=%s", key)
	}
	return nil
}

// WaitForSingleLeader ждёт, пока среди подключённых сервисов останется
// ровно один лидер, и возвращает его ID.
//
// Отличие от CheckSingleLeader: окно «два лидера» после переподключения
// изолированного сервиса не является ошибкой — прежний лидер узнаёт о
// более высоком term'е только с первым дошедшим AppendEntries, доставка
// которого откладывается задержкой повторов репликации.
// CheckSingleLeader трактует такое состояние как фатальное, поэтому для
// ожидания схождения используется этот метод.
func (h *Harness) WaitForSingleLeader(timeout time.Duration) int {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		connected := h.connectedSnapshot()
		leaderId, multiple := -1, false
		for i := range h.n {
			if !connected[i] || !h.kvCluster[i].IsLeader() {
				continue
			}
			if leaderId >= 0 {
				multiple = true
				break
			}
			leaderId = i
		}
		if !multiple && leaderId >= 0 {
			return leaderId
		}
		if !time.Now().Before(deadline) {
			break
		}
		// poll-интервал condition-wait (не фиксированная пауза).
		time.Sleep(20 * time.Millisecond)
	}
	return h.CheckSingleLeader()
}

// CheckPut отправляет через клиента c запрос Put и проверяет, что он
// завершился без ошибок. Возвращает (prevValue, keyFound).
// Вызывается только из тестовой горутины (см. TryPut).
func (h *Harness) CheckPut(c *kvclient.KVClient, key, value string) (string, bool) {
	h.t.Helper()
	pv, f, err := h.TryPut(c, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

// CheckGet отправляет через клиента c запрос Get и проверяет отсутствие
// ошибок. Также проверяет, что ключ найден и его значение совпадает с
// ожидаемым. Вызывается только из тестовой горутины (см. TryGet).
func (h *Harness) CheckGet(c *kvclient.KVClient, key string, wantValue string) {
	h.t.Helper()
	if err := h.TryGet(c, key, wantValue); err != nil {
		h.t.Error(err)
	}
}

// CheckCAS отправляет через клиента c запрос CAS и проверяет, что он
// завершился без ошибок. Возвращает (prevValue, keyFound).
// Вызывается только из тестовой горутины (см. TryCAS).
func (h *Harness) CheckCAS(c *kvclient.KVClient, key, compare, value string) (string, bool) {
	h.t.Helper()
	pv, f, err := h.TryCAS(c, key, compare, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

// CheckGetNotFound отправляет через клиента c запрос Get и проверяет
// отсутствие ошибок, а также то, что указанный ключ отсутствует в сервисе.
// Вызывается только из тестовой горутины (см. TryGetNotFound).
func (h *Harness) CheckGetNotFound(c *kvclient.KVClient, key string) {
	h.t.Helper()
	if err := h.TryGetNotFound(c, key); err != nil {
		h.t.Error(err)
	}
}

// CheckGetTimesOut проверяет, что запрос Get, отправленный через данного
// клиента, завершится по тайм-ауту при использовании контекста с дедлайном,
// поскольку клиент не сможет добиться фиксации своей команды сервисом.
func (h *Harness) CheckGetTimesOut(c *kvclient.KVClient, key string) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*raft.Quantum*time.Millisecond)
	defer cancel()
	_, _, err := c.Get(ctx, key)
	if err == nil {
		h.t.Error("got err nil; want an error wrapping context.DeadlineExceeded")
		return
	}
	// Проверка по фактической цепочке ошибок, а не по подстроке
	// kvclient.Get → send → sendJSONRequest →
	// http.Client.Do возвращает *url.Error, оборачивающий
	// context.DeadlineExceeded, и send возвращает его как есть при
	// ctx.Err() != nil. Ветка «commit failed; please retry»
	// (kvclient StatusFailedCommit) контекст НЕ оборачивает — если
	// сценарий уйдёт в неё, тест обязан упасть, а не молча пройти.
	if !errors.Is(err, context.DeadlineExceeded) {
		h.t.Errorf("got err %v (%T); want an error wrapping context.DeadlineExceeded", err, err)
	}
}

// WaitForKeyValue ждёт, пока Get(key) не вернется с ожидаемым значением
// expectedValue или до истечения таймаута 2s. При таймауте вызывает
// h.CheckGet, который завершит тест с Fatal.
func (h *Harness) WaitForKeyValue(c *kvclient.KVClient, key, expectedValue string) {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(h.ctx, 500*raft.Quantum*time.Millisecond)
		val, _, err := c.Get(ctx, key)
		cancel()
		if err == nil && val == expectedValue {
			return
		}
		// poll-интервал condition-wait (не фиксированная пауза).
		time.Sleep(20 * time.Millisecond)
	}
	h.CheckGet(c, key, expectedValue)
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}
