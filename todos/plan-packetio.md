# packetio: план перехода для raft и storage-to-storage

> Источник: [atoonk/packetio](https://github.com/atoonk/packetio) (README + `netstack/`,
> Go, Apache-2.0) + наша кодовая база (состояние на 2026-11).
> Связано: `roadmap.md` Фаза 5 (производительность до уровня Vitastor), Фаза 6 (сеть
> control/data), `vitastor-ideas.md`, `bench-stand.md`.

Цель: использовать packetio для (1) raft-транспорта (control plane, meta) и
(2) коммуникации между сторажами (data plane, sdsnet-репликация/рекавери).

---

## 1. TL;DR — рекомендация

**Переходим не «всё разом», а по транспорт-абстракции и плоскостями, с
замером-гейтом на каждом шаге.**

1. packetio не переписывает наш wire-протокол. Его `netstack` отдаёт ровно
   `net.Listener`/`net.Conn` — то же самое, что уже использует
   `pkg/sdsnet` и `pkg/meta/rafttransport.go`. Миграция = **смена сокета под
   капотом**, а не смена framing-а.
2. **Control plane (raft) первым**: малые сообщения, мало соединений,
   в основном один поток leader→followers. Это *самый безопасный* сценарий
   для userspace-стека.
3. **Data plane (репликация) — по гейту замера**, и только потом. Двусторонний
   bulk — *слабое место* netstack по его же бенчмаркам (stall на bidirectional
   echo в upstream, фикс zero-window только в форке). Сегодняшняя
   сdsnet-репликация как раз bulk bidirectional.
4. Вводим один `Transport`-интерфейс в `sdsnet` (и тонкий шов в raft), чтобы
   kernel-TCP и packetio были **A/B-сравниваемы** на одном кластере, с
   переключением по флагу `-netstack backend=afxdp|mlx5|dpdk|kernel`.
5. **Если флот не Mellanox (без ConnectX):** data plane остаётся на ядре
   **в обеих TLS-конфигурациях** — plaintext: afxdp это регресс против нашего
   MSG_ZEROCOPY (1 vs 0 копий, ×2.4–5.5 ЦПУ, −42/44% по RX/bidir, см. §4.5);
   TLS: wash-to-worse (память+ЦПУ растут, выигрыша нет). Единственная
   вразумительная ниша без ConnectX — raft.

**Не трогаем** admin API (HTTP), nbd, vduse, client-facing — это вне рамок
«between storages».

---

## 2. Что такое packetio (кратко)

Один API, четыре способа достучаться до NIC:

| бэкенд | когда | что нужно | 1 core send |
| --- | --- | --- | ---: |
| **mlx5** | ConnectX/BlueField | rdma-core, `-tags mlx5` | 69.2 Mpps |
| **dpdk** | Intel/Broadcom/virtio, или нужен драйвер DPDK | libdpdk, `-tags dpdk`, hugepages | 56.7 Mpps |
| **afxdp** | любая нормальная NIC, NIC остаётся в ядре | драйвер с XDP | 18.7 Mpps |
| **afpacket** | «просто чтобы работало»: VM, контейнер | ничего | 1.7 Mpps |

Ключевое для нас: **`netstack/`** (отдельный Go-модуль) — gVisor-совёрнст
`netstack` поверх любого из четырёх бэкендов. `Listen`/`Dial` возвращают
`net.Listener`/`net.Conn`, а поверх этого идёт обычный Go (TCP CUBIC, ARP, IPv4,
Ethernet/802.1Q, GSO, GRO, софтовые чексамы). Ядро в пути нет.

```go
dev, _ := afxdp.Open("eth0", afxdp.WithSteering(filter))
st,  _ := netstack.New(dev, netstack.Config{
    Addr: netip.MustParsePrefix("10.10.10.2/24"),
    MAC:  <mac>,
})
ln, _ := st.Listen("tcp", ":3000")   // обычный net.Listener
c,  _ := st.Dial("tcp", "10.10.10.3:3000") // обычный net.Conn
```

**Границы netstack на сегодня** (важно для нас):
- **IPv4 + TCP только** (нет UDP, нет IPv6) → raft (TCP) и sdsnet (TCP) ок.
- **Один стек на device** (все очереди); per-queue — по одной горутине, и
  receive-горутина *сама* прогоняет TCP для соединений, чьи сегменты она
  доставила.
- **Софтовые чексамы** во всех бэкендах (NIC-offload — «в планах»).
- **Bidirectional bulk — слабое место**: один echo-flow на fraction one-way
  speed + retransmit-timeouts в *upstream* gVisor; у форка — zero-window-фикс
  (stall снят: 0.05 → 4.5 Gbit/s на ENA). One-way bulk ок (84–93 Gbit/s).
- `1 ms delayed ACK` и inline TCP processing — настройки форка, применить в
  Этапе 0 как измерять.

---

## 3. Ключевой инсайт: шов = `net.Conn` / `net.Listener`

Оба наших транспорта оперируют только стандартным `net`, никакого vendor-code:

**`pkg/sdsnet`** (data plane, TCP, TLS-aware, pipelined/batched):
- сервер: `net.Listen("tcp", addr)` → `srv.ServeListeners(...)`;
  `acceptLoop` обёртывает conn в `tls.Server(c, s.TLS)` при `s.TLS != nil`
  (`server.go:168,228`).
- клиент: `net.DialTimeout("tcp", ...)` в `DialTLS`; `NewConn(c net.Conn, ...)`
  принимает уже установленный транспорт (`client.go:71,111`); `Pool.Local`
  заменяет `DialTLS` на свой диалер (`client.go:524,630`).
- kernel-оптимизации, которые *привязаны к ядру*: `setNoDelay` (Nagle),
  `SO_ZEROCOPY`/`MSG_ZEROCOPY` (`zerocopy_linux.go`, **не работает под TLS** —
  `canZeroCopy` возвращает false для TLS-конна), `ListenReusePort`
  (`reuseport_linux.go`).

**`pkg/meta/rafttransport.go`** (control plane, TCP, length-prefixed protobuf):
- `net.Listen("tcp", t.addr)` (`:165`), `net.DialTimeout("tcp", ...)`
  (`:221,288`), `tlsClientConn`/`tls.Server` при `RaftConfig.TLS`,
  `acceptLoop` (`:317`).
- Фрейминг: `0xF1 message | 0xF2 snapshot` + 4B length + protobuf.
  Snapshot идёт отдельным конном (multi-MB не блокирует heartbeats).

**Вывод:** и sdsnet, и raft принимают `net.Listener`/`net.Conn` как «черный
ящик». packetio-netstack отдаёт ровно этот интерфейс. Значит:
- framing (32B-заголовок sdsnet, `0xF1/0xF2` raft) **не меняется**;
- меняется только *откуда* берутся `Listener`/`Conn`;
- `NewConn`/`ServeListener`/`Pool.Local` — готовые точки вставки.

---

## 4. Честная оценка: где packetio помогает, а где нет

### 4.1 Control plane (raft) — ПОДХОДИТ
- Трафик: heartbeats/appends/votes — мелкие, частые; snapshot — редкий bulk
  **one-way** (leader→follower). One-way bulk netstack держит.
- Мало соединений (N=3–5 реплик), поэтому «одна горутина на очередь» и
  per-connection-модель не создают contention.
- Потенциальный минус: `1 ms delayed ACK` форка + замена ядрового TCP →
  влияет на p99 replication lag и election timing. **Гейт: замерить p50/p99
  рафта и election-time на kernel vs netstack.**

### 4.2 Data plane (репликация OSD↔OSD) — РИСК
- Репликация = **bidirectional bulk** (записи идут на N реплик, ответы/ACK
  обратно; recovery/vacuum — тоже). Это ровно та workload, где netstack в
  upstream **застревает**. Форк снимает zero-window stall, но цифры
  bidirectional echo всё ещё в разы ниже one-way (16-flow echo 54 Gbit/s vs
  93 Gbit/s one-way на afxdp-форке) и на mlx5 — с потерями.
- Мы уже оптимизировали data-path под ядро: `SO_ZEROCOPY`, `MSG_ZEROCOPY`,
  reuseport, batch. В userspace-стеке `setNoDelay`/`canZeroCopy`/reuseport
  становятся no-op (это kernel-фичи), их заменяет GSO/GRO netstack —
  **но нужно доказать, что bulk не откатывается**.
- **Гейт: fio через репликацию на kernel vs netstack (afxdp и mlx5),
  bidirectional, при реальной загрузке. Только при паритете/выигрыше — на prod.**

### 4.3 TLS — усложняет
- Сегодня data plane работает под mutual TLS (`-tls-planes data,raft,...`).
- TLS-обёртка поверх `net.Conn` **механически работает** (обёрнуть
  netstack-конн в `tls.Client`/`tls.Server`) — sdsnet уже так делает.
- Но: TLS = per-packet crypto + мелкие сегменты → overhead поверх userspace
  стека; и в TLS-сценарии мы **всё равно** без zerocopy (ядро уже его не даёт —
  см. 4.5: дефолтный `-tls-planes` включает data, и `canZeroCopy` для
  `tls.Client` возвращает false). Т.е. в TLS-конфигурации разрыв
  kernel↔netstack **наименьший**, и ядро всё ещё имеет шанс быть не хуже.
  **Гейт: замерить data plane под TLS на обоих транспортах.** Разбор «А/Б» —
  в 4.5; он сводится к: *packetio на data plane закрывает наш бар «не потреблять
  больше» только в plaintext-data + mlx5/dpdk; в TLS-data — скорее всего wash.*

### 4.4 Что теряем/заменяем при переходе на netstack
| Сегодня (kernel TCP) | На netstack |
| --- | --- |
| `SO_ZEROCOPY`/`MSG_ZEROCOPY` | GSO: сегментация в память фреймов NIC in-place (по сути то, к чему шёл zerocopy) |
| `ListenReusePort` + NUMA-pinning воркеров | «1 горутина на очередь» + RSS стека/DPDK; pinning пересматриваем |
| kernel GRO/GSO | userspace GRO/GRO (on by default) |
| kernel TCP (CUBIC/CUBIC, RACK) | gVisor TCP (CUBIC, RACK, tail-loss, precise RTT) |
| `SetNoDelay` (Nagle) | fork-настройки (delayed ACK 1 ms) — замерить |

---

### 4.5 «А: не начать потреблять больше ЦПУ и памяти, Б: получить выигрыш» — честный расчёт по zero-copy

Это главный вопрос. Разбор по факту кода + бенчмаркам netstack.

**Сначала факт, который меняет расклад:** наш data plane по **дефолту работает под
TLS** — `-tls-planes` по умолчанию `"data,raft,nbd,admin"` (`cmd/unistord/main.go:242`).
А `canZeroCopy()` (zerocopy_linux.go) тип-ассертирует `cc.c.(*net.TCPConn)` и
**возвращает false для TLS-конна** (это `tls.Client`, не `*net.TCPConn`).
→ **В дефолтной прод-конфигурации наше ядровое MSG_ZEROCOPY уже НЕ СРАБАТЫВАЕТ.**
Мы уже платим ядровую копию plaintext-пейлоада в TLS-запись. Зеро-копи ядра у нас
есть только в «vitastor-style» конфигурации: `-tls-planes raft,admin,nbd` (data
в открытую, как в Vitastor).

**Что такое «zero-copy» в netstack.** Не SO_ZEROCOPY, а **GSO «in place»**:
TCP отдаёт endpoint пакет до 64 KiB, endpoint сегментирует его в MSS-фреймы
**на месте, прямо в память фреймов device** (нет per-segment-копии). На RX:
**один** copy фрейм → pooled view (это «пол», который ставит API gVisor — всё,
что бэкенд даёт сверх, — это только пропуск/ядро, а не про число копий).

**Глубина zero-copy — зависит от бэкенда (критично):**

| Бэкенд | TX (write) | RX (read) | Сравнение с «нашим сегодня» |
| --- | --- | --- | --- |
| **afxdp / afpacket** (UMEM, «лёгкие», VM/CI, наш likely-дефолт) | **1 copy**: payload → UMEM-фрейм, NIC DMA’ит UMEM | 1 copy: view → наш `p` | vs plaintext-kernel: **регресс** (теряем MSG_ZEROCOPY → одна копия); vs дефолт-TLS: **нейтрально** (копию мы уже платили) |
| **mlx5 / dpdk** (user-memory mapping) | **0 copy**: DMA из user-буфера (GSO in place) | 1 copy | vs оба наших пути: **выигрыш** — единственный случай, где «Б» реально достижимо |

Т.е. **истинный zero-copy (0 копий на TX) возможен только на mlx5/dpdk**, и то при
проточивании наших write-буферов через NIC DMA. На afxdp/afpacket (UMEM) через
шов `net.Conn` это **всегда 1 copy** — буфер должен попасть в UMEM-фрейм, а
«in place» экономит только сегментацию, не заполнение фрейма.

**А — ЦПУ (не гарантированно «станет меньше»):**
1. **Софтовые чексамы** — на **всех** бэкендах в v1, по пакету (NIC-offload —
   «в планах», `OffloadTransmitter`). Это гарантированный **прирост** ЦПУ против
   ядра, которое использует NIC-offload.
2. **«Один стек на device» + per-connection горутина** — при N×M коннах sdsnet
   это goroutine-сcheduling/lock-контекст. В bench-таблице netstack:
   **388k vs 349k req/s на 64 коннах (выигрыш) при 19 vs 14.3 busy cores
   (~+35% ЦПУ)** — т.е. форк **торгует ЦПУ за throughput**. Для storage-ноды,
   где ЦПУ — самый дефицитный ресурс, это ровно тот тред, против которого
   «не начать потреблять больше ЦПУ».
3. На наших bulk-флоу (мало потоков, много объёма) GSO/GRO должны помогать, но
   софт-чексамы + горутины могут съесть выигрыш при line rate. **Обязательный замер.**

**Б — память (гарантированно «станет больше», на всех бэкендах, по дизайну):**
- afxdp/afpacket держат **UMEM**: `WithFrames(n)` (дефолт 1024) ×
  `WithFrameSize(n)` (дефолт 2048; для zero-copy на ряде драйверов надо 4096)
  **mlock-ованной** памяти на очередь + gVisor-буферы на соединение.
- Ядро сегодня: **ноль** user-locked памяти (socket buffers ведёт ядро).
- → **Мы гарантированно начнём потреблять больше RSS + больше `MemlockLimit`.**
  Это не баг, а плата за userspace-буферы; оно **настраивается** (меньше/мельче
  фреймы = меньше памяти, но больше frame-churn). Бюджет закладывать заранее.

**Зависимость от карт (важно, если у нас не Mellanox/ConnectX):**
- mlx5-бэкенд — **только ConnectX**; без него потолок по бэкендам — **afxdp**
  (dpdk-ENA в их таблицах — это VFIO-путь для облаков, на generic `igb_uio`
  dpdk в netstack-бенчмарках **не покрыт** — нужно дообench'ить самим).
- На afxdp «Б» недостижимо (1-copy TX по UMEM), bidir bulk **проигрывает** ядру
  (54 vs 93 Gbit/s в их цифрах), req/resp — throughput-выигрыш **ценой
  +35% ЦПУ (19 vs 14.3 busy cores) и p50 60→90µs**. Т.е. на Intel-картах:
  «Б» нет, «А» (ЦПУ) в req/resp-нише нарушено, data plane остаётся
  проигрышным/нейтральным.
- **Конкретно plaintext (data plane без TLS — там, где наше MSG_ZEROCOPY
  мощнее всего). Цифры afxdp-fork vs kernel, 100G (afxdp-строки сняты на
  ConnectX-6, но путь generic — AF_XDP+UMEM, оверхед переносим на Intel-NIC;
  локальный дообench на нашей карте нужен):**

  | workload | kernel | busy cores | afxdp fork | busy cores |
  | --- | --- | ---: | --- | ---: |
  | one-way TX, 16 flows | 93.8 Gbit/s | 2.7 | 93.6 (line rate) | **15.0** |
  | one-way RX, 16 flows | 93.8 Gbit/s | 9.9 | **52.2 (−44%)** | 25.3 |
  | bidir echo, 16 flows | 92.7 Gbit/s | 13.2 | **54.2 (−42%)** | 32.0 |
  | req/resp 64B, 64 conn | 349k req/s, p50 60µs | 14.3 | 388k, p50 90µs | 19 |
  | req/resp, 1024 conn | 1.21M req/s | 33.1 | 962k (−20%) | — |

  Плюс по копиям: **наш** kernel в plaintext = 0-copy TX (MSG_ZEROCOPY) — их
  kernel-базлайн в таблице уже с 1 копией, т.е. мы впереди даже своего
  бенчмарка, а afxdp = 1 copy. **Вывод: выключение TLS на data plane делает
  packetio менее, а не более привлекательным** — plaintext + afxdp это регресс
  по всем bulk-метрикам (копии, ЦПУ ×2.4–5.5, RX/bidir −42/44%); единственный
  плюс (req/resp +11% на 64 коннах) — не наш профиль, sdsnet это bulk-
  репликация, а не req/resp.
- Единственная реальная ниша выигрыша **без ConnectX** — VM с ENA (dpdk-ENA:
  single-flow echo 0.05→4.5 Gbit/s, +1.25s) и, возможно, dpdk на `igb_uio`
  с дообench'ем. Для bare-metal Intel-флота переход на data plane **не даёт
  выигрыша** — только raft-ниша.

**Вывод по «А/Б»:**
- **«Б» (выигрыш) реален только** на `mlx5/dpdk` + plaintext-data + large-bulk
  writes (vitastor-style bulk-путь). Это и самый тяжёлый операционно бэкенд, и
  у netstack по mlx5 открытые потери (retransmits) — сначала разберём их.
- **«А» (не потреблять больше) нарушается по памяти на всех бэкендах** (UMEM+
  mlock + gVisor) и **под риском по ЦПУ на всех** (софт-чексамы, горутины).
- В **дефолтной** (TLS-data) конфигурации packetio, скорее всего, **wash-to-
  worse**: выигрыша по zero-copy нет (его не было и у нас под TLS), а память+
  ЦПУ вырастут. Реальный смысл пакетию есть **только в plaintext-data +
  mlx5/dpdk** конфигурации — т.е. ровно там, где мы его и планировали под bulk.

**Следствие для гейтов:** ключевое решение **до** этапа 3 — зафиксировать
две пары: **TLS или plaintext** на data plane, **ConnectX или generic
NIC**. Матрица: (a) plaintext + ConnectX + bulk-dominant → «Б» реален
(mlx5, 0-copy TX) — единственная комбинация для data-plane-миграции;
(b) plaintext + generic NIC (afxdp) → **регресс** (0→1 копия + ×2.4–5.5 ЦПУ
+ −42/44% по RX/bidir) — data plane на ядре; (c) TLS + любой бэкенд →
wash-to-worse, data plane на ядре. Во всех трёх случаях raft-ниша
(небольшие сообщения, мало коннов) на netstack/afxdp — вразумительна.

---

## 5. Риски (чек-лист)

1. **Bidirectional bulk** (data plane) — главный риск. Не тянуть в prod до
   замеров (4.2).
2. **IPv4-only / TCP-only** — ок для raft+sdsnet; не тянем сюда UDP/IPv6.
3. **Один стек на device** — control/data split (2026-11-11): если data и ctrl
   на **разных NIC** → два device → два стека, чисто. Если на одной NIC с
   разными IP — один стек на один `Addr`; разную сеть придётся вести стеком с
   steering-фильтром. **Рекомендация: держать data/ctrl на разных NIC**
   (или на разных VLAN-стеках).
4. **`1 ms delayed ACK`** — влияние на raft p99 и на ACK-поток bulk. Измерить.
5. **Софтовые чексамы** — CPU-цена на high pps. Сопоставить с ядром (оффлоад).
6. **Per-connection goroutine-модель** — sdsnet держит *много* коннов (Pool на
   таргет + accept-много на сервере). Проверить, как маппинг flow→queue и
  goroutine-размещение согласуются с нашим pinning-ом.
7. **`st.Done()` / `Err()`** — очередь, упавшая (NIC ушла), не останавливает
  стек сам; сервер должен трактовать `Done` как сигнал. Провести в shutdown-лог.
8. **Операционка**: hugepages (dpdk), `CAP_NET_RAW` везде, memory-lock (mlx5/
   dpdk), root or ulimit. CI/VM-флот — только afxdp/afpacket.
9. **Память растёт на всех бэкендах** (риск к «А»): afxdp/afpacket-UMEM
   (`WithFrames`×`WithFrameSize`, mlock) + gVisor-буферы — против ноля
   user-locked у ядра. Бюджет RSS/`MemlockLimit` заложить до этапа 3.
10. **ЦПУ под угрозой (риск к «А»)**: софт-чексамы по пакету (все бэкенды, v1)
    + «один стек на device»/per-conn горутины. Bench-форк: +35% busy cores vs
    ядро при выигрыше throughput (388k vs 349k req/s). Не гарантируется
    «не начать потреблять больше» без замера.
11. **Тесты**: chaos/fence/nospace/stream/fua-тесты сейчас ходят через
   kernel-TCP (`Local`/socketpair) и loopback. Держим их на kernel; netstack
   — отдельный вариант прогона (veth/2-VM).

---

## 6. Архитектура: транспорт-абстракция

Единый шов, чтобы kernel-TCP и packetio были взаимозаменяемы и A/B-сравниваемы.

```go
// pkg/netstack/ (новый) — тонкая обёртка над atoonk/packetio/netstack.
package netstack

// Stack оборачивает один device (одну NIC / один prefix).
// Держит gVisor-стек, отдаёт Listener/Conn стандартного net.
type Stack struct { /* dev, cfg, st */ }

// Listen/Dial возвращают обычные net.Listener / net.Conn поверх packetio.
func (s *Stack) Listen(network, addr string) (net.Listener, error)
func (s *Stack) Dial(network, addr string) (net.Conn, error)
// Done/Err — сигналы здоровья очередей (риски 7).
```

```go
// pkg/sdsnet/transport.go (новый) — шов в sdsnet.
type Transport interface {
    Listen(addr string) (net.Listener, error)
    Dial(addr string, timeout time.Duration) (net.Conn, error)
    IsKernel() bool // true → разрешаем setNoDelay/canZeroCopy/ListenReusePort
}
// KernelTransport = текущий путь (net.Listen/net.Dial).
// StackTransport  = поверх netstack.Stack.
```

```go
// pkg/meta/rafttransport.go — тонкий шов:
// заменить net.Listen/net.DialTimeout на wireTransport-транспорт
// (единый Dial/Listen, как в sdsnet). Фрейминг 0xF1/0xF2 не тронут.
```

Принципы:
- **`KernelTransport` — дефолт** во всех тестах и в CI; netstack — по флагу.
- Kernel-специфичные опции (`setNoDelay`, `canZeroCopy`, `ListenReusePort`)
  вызываются только когда `IsKernel()`; на netstack они no-op (заменяет GSO).
- `Pool.Local` и `NewConn`/`ServeListener` остаются как есть — netstack-конн
  просто заходят в них как `net.Conn`.

Флаги (unistord):
```
-netbackend kernel|afxdp|mlx5|dpdk     # дефолт: kernel
-netiface eth0                          # для non-kernel
-netaddr 10.10.10.2/24                 # prefix стека (non-kernel)
-vlan 2043                             # 802.1Q, если нужно
```

---

## 7. Поэтапный план

### Этап 0 — стенд и proof-of-concept (1–2 дня)
- `go get atoonk/packetio` + модуль `packetio/netstack`; прокинуть в go.mod.
- На **двух тестовых VM** (afxdp, VLAN) поднять `netstack` и прогнать:
  (а) `tcpecho` через **наш** `sdsnet` (server на netstack-Listen, client на
  netstack-Dial) — убедиться, что framing/handshake/batch работают без изменений;
  (б) базовый замер: 4K read/write, batch, p50/p99, throughput — **против
  kernel-TCP на том же стеке**.
- Вывод: (1) подтверждён ли «шов = net.Conn»; (2) базовый разрыв latency/
  throughput; (3) где боль (delayed-ACK, checksums, goroutines).
- **Gate 0:** sdsnet поверх netstack — функционально равен kernel (тесты
  пакета `pkg/sdsnet/...` проходят на netstack-транспорте).

### Этап 1 — транспорт-абстракция в sdsnet (3–5 дней)
- `pkg/netstack` + `pkg/sdsnet/transport.go`; `KernelTransport` (дефолт) и
  `StackTransport`.
- `Server`/`Pool`/`Dial`/`NewConn` принимают `Transport`; kernel-опции за
  `IsKernel()`.
- Chaos/stream/fence/nospace/fua-тесты прогоняются на обоих транспортах
  (kernel — дефолт, netstack — опционально).
- **Gate 1:** 100% `pkg/sdsnet/...` на kernel (без изменений поведения) +
  выборочные на netstack.

### Этап 2 — raft control plane ПЕРВЫМ (3–5 дней)
- Тонкий шов в `pkg/meta/rafttransport.go` (единый Dial/Listen).
- Флаг `-raft-netbackend` (отдельно от data, чтобы менять по плоскости).
- Замерить: p50/p99 replication, election-time, snapshot-время,
  `memTransport`/`tcpTransport` паритет на netstack.
- **Gate 2:** raft на netstack — election/replication не деградируют (p99
  в пределах ~x kernel; snapshot one-way в норме).

### Этап 3 — data plane репликация (только по гейту) (5–10 дней)
- **Предуловое решение (открыт. вопрос 0 в §9)**: зафиксировать прод-конфигуцию
  data plane (plaintext vitastor-style vs TLS). Без этого гейт некорректен —
  выигрыш zero-copy есть только в plaintext (4.5).
- Включить `StackTransport` для replication (OSD data-слушатель/конны на
  netstack), control/data split → разные NIC/стеки.
- fio-стенд (`bench-stand.md`): **bidirectional** через репликацию,
  kernel vs netstack (afxdp и mlx5), под TLS и без; мерить не только
  throughput/latency, но и **busy-cores + RSS/mlock** (бары «А» из 4.5).
- **Gate 3 (жёсткий):** bulk bidirectional паритет или выигрыш против
  kernel. Иначе data plane **остаётся на kernel**, netstack только для raft.
  Это осознанный исход, а не провал.

### Этап 4 — бэкенды и операционка (по ситуации)
- Дефолт prod: **afxdp** (NIC в ядре, без hugepages, мин. операционка).
- mlx5 для ConnectX-флота (`-tags mlx5`, rdma-core, mem-lock) — после
  разбора losses (см. 4.1, netstack-README).
- dpdk только если нужен конкретный драйвер/ENA-vfio.
- hugepages/CAP_NET_RAW/memlock в runbook; CI/VM — afxdp/afpacket.
- NUMA-pinning воркеров — пересмотреть под «1 горутина на очередь» (риск 6).

---

## 8. Что НЕ трогаем
- Admin API (HTTP, `cmd/unistord`), health/metrics — ядро, вне рамок.
- nbd, vduse, client-facing (нbd/vub) — разные протоколы, не «between storages».
- Wire-фрейминг sdsnet (32B/16B) и raft (0xF1/0xF2) — без изменений.
- `pkg/store`, crush, placement, journal — не сеть.

---

## 9. Открытые вопросы (закрыть на Этапе 0)
0. **Ключевое, до этапа 3**: какая прод-конфигуция data plane —
   **plaintext (vitastor-style)** или **TLS (дефолт)**? От этого зависит, где
   zero-copy вообще важен (см. 4.5): plaintext+mlx5 = есть смысл за «Б»,
   TLS-data = packetio на data plane, скорее всего, wash.
1. Как ведёт себя **`1 ms delayed ACK`** форка на наших request/response
   (sdsnet batch) и на raft heartbeats? Замерить p50/p99.
2. **Bidirectional bulk**: репликация fio на netstack-afxdp против kernel —
   есть ли stall (upstream-симптом) на форке?
3. **Один стек на device**: как разложить data+ctrl (разные IP) на одной NIC
   vs держать на разных NIC. Склониться к разным NIC.
4. **Per-connection goroutine**: при N×M соединениях sdsnet — нет ли contention
   на receive-горутинах стека; как согласовать с pinning.
5. **TLS**: data plane под mutual TLS — где точка безубыточности kernel vs
   netstack (там, где ядро уже без zerocopy).
6. **Go-версия/зависимости**: netstack требует Go 1.26 (у нас 1.26.0 — ок),
   тянет gVisor (отдельный модуль) — проверить разрастание бинарника и
   сборку `release` (GOOS=linux/GOARCH) с `-tags mlx5/dpdk`.

---

## 10. Оценка объёма (грубая)

| Этап | Суть | Дней (1 чел.) |
| --- | --- | ---: |
| 0 | стенд, 2-VM, POC sdsnet-on-netstack | 1–2 |
| 1 | `pkg/netstack` + sdsnet `Transport`-шов | 3–5 |
| 2 | raft-шов + замер (control plane) | 3–5 |
| 3 | data plane по гейту + fio | 5–10 |
| 4 | бэкенды, операционка, NUMA | по ситуации |

Критический путь до «raft на packetio» (Этапы 0–2): **~1,5–2 недели**.
Data plane — отдельный, гейт-зависимый блок (может и не понадобиться, если
bulk bidirectional не закроет паритет).
