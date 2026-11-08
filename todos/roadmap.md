# ROADMAP: unistack storage — распределённая блочная SDS на Go

Цели (зафиксированы владельцем, редакция 2026-11-08):
0. **Не копировать Ceph — быть лучше и быстрее.** Совместимость с Ceph —
   только канал входа (импорт существующих crushmap), не архитектурный
   ориентир. Всё остальное (протокол, журнал, метаданные, экспорт) — своё.
1. **CRUSH-крашмапы** — размещение object/реплик по topology-map
   (region → zone → rack → host → disk), детерминированное, с reweight/
   migration при изменении кластера; байт-в-байт импорт карт Ceph (crushkv,
   golden-тесты) — чтобы мигрировать пользователей, а не чтобы быть Ceph.
2. **Только Go; Vitastor — эталон, чтобы его обогнать.** Их цифры
   (~0.14 мс 4K при RDMA, миллионы IOPS/узел; бенчмарки сторонних
   пользователей подтверждают реальный разрыв с Ceph ~10x) достигнуты
   архитектурой, которую мы перенимаем: метаданные всегда в RAM, журнал
   = последовательное кольцо (запись = append в журнал + репликация в
   кворум журналов, данные флешатся фоном), один RPC клиент→primary,
   single-threaded per-core без локов, io_uring, zero-copy, RDMA.
   План обгона и честные ворота — в Фазе 5 («чем обгоняем Vitastor»).
3. **Конечная цель: диск внутри ВМ виден как NVMe-устройство**
   (цепочка vhost-user-blk → qemu-storage-daemon → vfio-user зафиксирована
   в `unistack/todos/storage-nvme-vfio-user.md`; путь без патчей QEMU;
   VDUSE уже реализован — мультиочерёдный, быстрее их NBD/UBLK-пути).

Стратегия: **greenfield** поверх каркаса репозитория (см. [AUDIT.md](AUDIT.md)):
реестр-паттерн интерфейсов оставить, sheepdog-слой и CLI-заглушки удалить,
данные/протоколы не совместимы с чем-либо — протокол свой.

---

## Фаза 0 — Гигиена репозитория (1–2 дня) — ✅ сделано

- [x] `go.mod` (`module github.com/unistack-org/storage`, Go 1.23); сборка
      с `GOWORK=off` (рядом лежит внешний `../go.work` — он не наш)
- [x] Удалено: `proxy/sheepdog/`, `sds-storage/` (CLI-заглушки sheepdog-dog),
      `api/`, `kv/`, `cache/block`, `cache/filesystem`, `journal/leveldb`,
      `cluster/etcdint` (зависимости от мёртвых пакетов/leveldb/etcd-embedded),
      `backend/block`, `backend/filesystem` (импортировали несуществующий
      `ring`), `bench/` (битые внешние хэши с bitbucket)
- [x] Скелет зафиксирован: `backend/`, `cache/` (+`cache/memory` LRU),
      `cluster/` (registry), `hash/` (+xxhash), `journal/`, `transport/`;
      import-пути переписаны `sdstack` → `unistack-org`; build/vet/gofmt чистые
- [x] CI: `.github/workflows/ci.yml` — build + vet + race-tests + gofmt
      для обоих модулей (`.` и `sds-nbd`)
- [x] README: назначение, статус (MVP sds-nbd, crushkv), ссылки на
      AUDIT/roadmap

## Фаза 1 — Локальное ядро: быстрый single-node data path (2–4 недели)

Цель: один узел держит диск; fio бьёт наш NBD/сокет — база для всех замеров.

- [x] `pkg/object`: объект = 64-бит ObjectID (OID), размер блока 2–4 MiB (shift, как Sheepdog object / Vitastor 4M)
- [x] `pkg/store` (локальный): файл на диск, **O_DIRECT** (Linux; на darwin —
      F_NOCACHE), имена по OID; preallocate (fallocate), usage-учёт,
      кэш fd, Lookup() для hole-punch
- [x] **io_uring** ring (`pkg/iouring`: registered fds, fixed offsets;
      SQPOLL опционально позже); fallback worker-pool `pread/pwrite` на non-Linux;
      Linux-часть проверена кросс-компиляцией (amd64+arm64), runtime-приёмка на Linux-хосте
- [x] No-alloc data path: никаких `make()` на I/O — единый пер-соединительный
      буфер `ioBuf` + preallocated header-буферы (стековые массивы утекали в
      heap через bufio.Write: 5 allocs/req -> 0). Гейт `TestNBDZeroAllocGate`
      (4000 конвейерных 4K READ, лимит 2 КБ/req) + бенчмарки
      `BenchmarkNBD{Read,Write}*`: **0 allocs/op** (Apple M1, loopback:
      4K READ 22 мкс, 64K 2.0 ГБ/с, 1M 8.4 ГБ/с)
- [x] `pkg/journal`: WAL для метаданных objects (16B заголовок: seq/len/crc32c,
      torn-tail устойчивость, replay, truncate) — задел под journaling Vitastor-типа
- [x] NBD-сервер (`sds-nbd/nbd`, протокол NBD over TCP) — точка входа для QEMU/fio;
      **StoreBackend**: экспорт поверх pkg/store (`sdsd -store -direct [-preallocate]`),
      split по 4M-границам, WRITE_ZEROES через punch hole (Linux), fallback zeroes на darwin
- [x] Бенчмарк-клиент `sdsbench` (pipelined NBD, QD, латентные перцентили)
- [ ] Приёмка фазы: `fio --ioengine=nbd/libaio` 4K randread ≥ базовой линии
      (замер локального файла как потолок), latency p99 < 1 мс на NVMe-хосте
      — **базовая линия fio-внутри-VM (Alpine 3.22 / kernel 6.12, QEMU aarch64
      nvme→NBD→sdsd -store -direct на macOS-хосте, io_uring, direct=1, QD:**
      seqwrite 1M QD8 = 1080 MiB/s; seqread 1M QD8 = 911 MiB/s;
      randwrite 4K QD32 (30 c) = 10.7k IOPS / 41.9 MiB/s (clat avg 2.96 мс);
      randread 4K QD32 (30 c) = 15.1k IOPS / 59.0 MiB/s (clat avg 2.10 мс);
      потолок хоста не проверен (macOS: F_NOCACHE read синхронный);
      sdsbench (host→sdsd, loopback, QD32): seqwrite 1M 733 MB/s,
      randwrite 4K 49k IOPS; приёмка p99<1мс — на Linux+NVMe

## Фаза 2 — Сеть и репликация (3–5 недель)

- [x] `pkg/sdsnet`: бинарный протокол (запрос 32 B: op/flags/seq/OID/offset/len/epoch;
      ответ 16 B: seq/status/value; handshake на 16 B + имя экспорта; без grpc/protobuf)
      — сервер `Server` (1 экспорт на соединение, ReadOnly-режим), клиент `Conn`
      (seq-валидация ответов, dead-conn детект), статусы OK/EOF/IO/Invalid/TooBig,
      raw-seq echo при битых заголовках (защита от desync конвейера)
- [x] Пулы соединений `sdsnet.Pool`: ленивый диал, Limit = пер-таргет троттлинг
      (Get блокируется до возврата соединения/ctx/Close), выброс dead-conn,
      reconnect после смерти сервера; тесты recovery/chaos зелёные под `-race`
- [x] Репликация `pkg/replication`: без primary, fan-out на N таргетов, commit по
      W из N (RF=3 W=2), stragglers дренируются окно grace=2 c, `ErrQuorumLost`
      при fails > N−W; epoch в заголовке зарезервирован под fencing (серверный
      fencing — Фаза 3); WriteZeroes/Trim/Sync фан-аутятся так же
- [x] Отказы: таймауты диала 5 c, ctx-отмена веерного запроса, замороженная
      реплика не блокирует commit по кворуму, восстановление реплики (redial),
      серверный `Close` рвёт живые соединения (нет висящих handler-ов)
- [x] Бенчмарк: `BenchmarkClientWrite4K` (loopback, mem-backend): 41.4 µs/op
      ≈ 98.9 MB/s, 5 allocs/op — запас под zero-copy/pipe-lining есть
- [ ] pipeline/batch запросов, `MSG_ZEROCOPY` (Linux), приём через io_uring
- [ ] heartbeat, деградация при потере реплики, recovery-трафик приоритетом
- [x] Тесты: chaos (смерть сервера, frozen replica, пул-давление, обрывы), `-race`
- Примечание: исправлен дедлок `iouring.fallbackRing.Submit` (mutex держался при
  отправке в unbuffered канал; файлы вынесены под отдельный filesMu)

## Фаза 3 — Кластер, метаданные, CRUSH (4–6 недель) — «как Ceph»

- [x] `pkg/meta`: метаданные кластера — интерфейс `Store` (Get/Put CAS/List/Watch)
      + `DirStore` (файловый бэкенд: каталог на все роли, watch поллингом 250 мс;
      для продакшена — реализация поверх **etcd**, тот же интерфейс): записи
      `osd/<id>` (адрес, uuid, вес), `maps/<epoch>` (crushmap), `volumes/<name>`
      (спека тома: размер, oid_shift, реплики, pg_num, rule)
- [x] `pkg/placement`: снапшот размещения — компиляция crushmap + реестра OSD
      в таблицу `pg → []OSD` (DoRule на всю сетку pg один раз за epoch,
      ~мс на 1024 pg); клиент считает placement сам и шлёт данные напрямую
      OSD — без прокси, как Vitastor/Ceph
- [x] `pkg/osd`: демон OSD = store + sdsnet-сервер + регистрация в meta +
      epoch-fencing (watch `maps/`: закрываем fence — запросы со старым
      epoch отбиваются `StatusStale` с требуемым epoch; фенсятся и чтения —
      только так клиент узнает, что карту пора пересобрать)
- [x] `pkg/client`: клиент кластерного тома — `Cluster` (Refresh/MapEpoch/
      CreateVolume/OpenVolume) + `Volume` (ReadAt/WriteAt/WriteZeroes/Flush):
      тонкие чтения (нет объекта → нули), веер writes на реплики с коммитом
      по кворуму W=R/2+1 (опоздавшие реплики дописываются detached-записью),
      авто-рефреш placement по `StatusStale`, fallback на живую реплику
- [x] Интеграция crushkv в storage: парсинг epoch-карт (text/json/binary),
      кэш снапшота placement в клиенте (троттлинг 5 мс на всплески stale)
  - [x] Эмуляция отказов + chi-square равномерность на больших картах —
        `pkg/placement/uniformity_test.go`: 10 host × 3 OSD, 65536 pg →
        chi2 osd=25.1 / host=8.8 (порог 200); отказ 1 OSD из 30 двигает
        ровно pg с ним (минимальность CRUSH), доля затронутых pg 3/30,
        пост-отказ chi2 ≤ 250; reweight(0) ≡ удаление из реестра;
        равномерное удвоение весов двигает 0% слотов (инвариантность
        масштабирования straw2). Попутно найден и исправлен баг:
        `buildTable` игнорировал реестр — устройства вне Registry теперь
        получают вес 0 (is_out), как и документировано.
  - [ ] Reweight-валидатор, `rule`-валидатор под наши volume-class (SSD/HDD) —
        поверх crushkv (CLI-команда `map check`).
- [x] sds-nbd: бэкенд `VolumeBackend` (том → NBD-экспорт) и режим `sdsd
      -cluster <meta-каталог>`: OSD в процессе (`-osd id@dir[@addr]`),
      публикация карты (`-crushmap -epoch`), `-create имя:размер`,
      `-export имя` — том как NVMe-диск в госте
  - [x] Смоук: 3 OSD, том 1G rep3 pg64, sdsbench поверх NBD (write 64K ~79 МБ/с,
        randread 4K ~2.7k IOPS loopback — потолок отладочной сборки, не цель),
        рестарт sdsd данные на месте; тесты: roundtrip, кворум с мёртвым OSD,
        stale-epoch рефреш — `pkg/client/client_test.go`
- [ ] `pkg/mds` поверх **etcd** (внешний; embedded `cluster/etcdint` для
      all-in-one): замена DirStore в production; inventory объектов
- [ ] CLI `unistorage`: node add/ls/out, volume create/ls/rm, map dump/check,
      reweight, recovery status
- [x] **CRUSH — ключевой deliverable: вынесен в отдельный репозиторий
      [`crushkv`](https://github.com/unistack-org/crushkv) (`../crushkv`), ГОТОВО:**
  - [x] Движок `crushkv/crush` на Go — **byte-exact порт C-ядра Ceph**
        (builder.c/mapper.c/hash.c): все алгоритмы uniform/list/tree/straw/straw2,
        choose/chooseleaf, все tunables-профили (legacy…optimal), LN-таблицы,
        jenkins hash. Проверено ~6000 golden-векторами, сгенерированными
        настоящим C-кодом Ceph (`crush/testdata/golden_*.txt`) — 100% совпадение.
  - [x] `crushkv/crushmap`: текстовый парсер `*.crush` (совместим с Ceph),
        JSON, и **двоичный кодек формата Ceph CrushWrapper** (`binary.go`:
        magic 0x00010000, bucket-slots с per-alg хвостами, rule-slots,
        string-мапы с историческим 64-бит quirk, tunables, class maps,
        choose_args) — decode/encode, круговой тест 600 golden-векторов через
        text→binary→decode→compile→placement, байт-стабильный encode.
  - [x] **零-GC data path**:placement считается в mmap-арене
        (`crush.Work` + `docs/GC_ARENA.md`): 225 OSD, 3 уровня, straw2, rep3 —
        **34.5 мкс на выбор, 0 B/op, 0 allocs/op**; carve из арены 290 нс vs
        690–940 нс у `make()`. GC на пути размещения отсутствует полностью.
  - [x] Тесты: воспроизводимость (golden Ceph), reweight in/out (вес 0),
        movement-свойства CRUSH — в `crush/*_test.go`.
  - [ ] Reweight-валидатор, `rule`-валидатор под наши volume-class (SSD/HDD) —
        CLI-команда поверх crushkv (эмуляция отказов и chi-square равномерность
        уже покрыты тестами: `storage/pkg/placement/uniformity_test.go`).
- [ ] CLI `unistorage`: node add/ls/out, volume create/ls/rm, map dump/check,
      reweight, recovery status

## Фаза 4 — Экспорт в ВМ: «NVMe внутри виртуалки» (2–4 недели)

Порядок от простого к целевому (детали и команды —
`unistack/todos/storage-nvme-vfio-user.md`):

- [x] **4a MVP (дни): NBD → QEMU `-blockdev driver=nbd` → `-device nvme`**
      — **сделано**: `storage/sds-nbd` (Go, stdlib-only NBD-сервер + демон `sdsd`);
      проверено: `qemu-img info/convert` туда-обратно по md5, Alpine-гость с
      `-device nvme` видит `/dev/nvme0n1` (1 GiB), mkfs.ext4 + mount + запись
      файла, байты видны в backing-файле на хосте. Детали: `sds-nbd/README.md`
      **E2E кластерного пути (Phase 3) проверен:** 3 OSD в процессе + том 2 GiB
      rep3 pg64 → NBD :10809 → Alpine ВМ: `/dev/nvme0n1` = 2147483648,
      mkfs.ext4 OK, запись 64 MiB blob + secret, md5 стабильны после
      umount/mount (данные через кворум-репликацию на месте), корректный
      shutdown/autofsck. fio в госте не поставился (aarch64-пакет вне зеркала
      ISO) — IOPS-числа сняты sdsbench на хосте: seqwrite 64K qd16 ~46 МБ/с,
      randread 4K qd32 ~2.3k IOPS (loopback, отладочная сборка)
- [ ] **4b Целевой: Go vhost-user-blk server (`pkg/vub`)** — unix-сокет,
      handshake (GET/SET_FEATURES, SET_MEM_TABLE, SET_VRING_*), mmap гостевой
      памяти (hugepages), разбор desc/avail/used rings, multi-queue; QSD как
      мост `--export type=vfio-user` → QEMU `-device vfio-user-pci`
      (в госте virtio-blk) + вариант NVMe через QSD NBD-экспорт + QEMU NVMe
      ⚠ Проверено: домашний хост (macOS, homebrew QEMU 11.1.3) НЕ имеет
      `vhost-user-blk-pci`/`vfio-pci`/vhost-объектов — 4b валидируется только
      на Linux+KVM (x86_64 или aarch64 Linux-хост с QEMU ≥ 8)
- [ ] Интеграция с `unistack/pkg/storage` (Provider-драйвер `unistorage`:
      CreateVolume/DeleteVolume/Attach) и `pkg/qemu` (`Drive.VfioUser`)
- [x] **4c VDUSE-экспорт (код готов; e2e — на Linux-хосте)**: `storage/pkg/vduse` —
      userspace-блочное устройство через `/dev/vduse` (VDUSE), без QEMU и без
      «устаревшего» vhost-user: гость цепляет устройство стандартно
      (`vdpa mgmtdev add vduse <имя>` → `vdpa dev add ... type block` →
      `/dev/vdX` как virtio-blk; в госте — родной драйвер, ядро-гость не трогается).
      Реализовано: control-протокол (CREATE_DEV/VQ_SETUP/SET_STATUS/GET_VQ_STATE/
      UPDATE_IOTLB, все офсеты сверены с kernel uapi), IOTLB (mmap гостевой памяти,
      zero-copy ReadAt/WriteAt по гостевым буферам), split-ring (desc/avail/used,
      indirect, event_idx, IRQ-инъекции), multi-queue. Бэкенд = тот же
      nbd.Backend: `sdsd -export vol0 ... -vduse vol0=sds-vol0` отдаёт кластерный
      том как диск. 8 тестов логики зелёные; кросс-компиляция linux/amd64+arm64
      чистая; Linux-runtime e2e требует хост с CONFIG_VDUSE (на macOS невозможен)
- [ ] Отказоустойчивость экспорта: смерть Go-бэкенда → QSD reconnect,
      без висячих сокетов/hugepages
- [ ] e2e-тест: Linux+KVM, гость пишет/читает, fio внутри гостя

## Фаза 5 — Производительность до уровня Vitastor (постоянно, с замерами на каждую фазу)

- [ ] **Чем обгоняем Vitastor (принято: они реально быстрее Ceph, мы целимся выше).**
      Их сильные стороны перенимаем 1:1: мета в RAM, journal-ring + кворум
      журналов, один RPC до primary, per-CPU single-thread, io_uring, zero-copy.
      Наши рычаги обгона: (1) Go 1.24+: per-CPU sharded workers c
      LockOSThread+busy-poll (SQPOLL), GOGC=off+ballast на data-потоках —
      GC-давление убираем полностью, а не снижаем; (2) арены/arena-аллокации
      вместо per-request-аллокаций (гейт 0 allocs/op в CI) — СДЕЛАНО для обоих
      протоколов: NBD (TestNBDZeroAllocGate, 0 allocs/op, Read4K 22мкс) и
      sdsnet клиент↔OSD (TestSDSNetZeroAllocGate, 0 allocs/op: Read4K 28мкс,
      Write4K 19мкс, 64K ~2.2GB/s; приём — пер-соединительные scratch-буферы,
      sync.Pool проигрывает из-за boxing в Put); плюс data-путь Volume-клиента
      (TestClientZeroAllocGate): Refresh с снапшот-кэшем placement (epoch+байты
      карты+registry без изменений → переиспользование скомпилированной таблицы,
      краш-лексер больше не аллоцирует на каждом цикле), quorumWrite на одном
      канале со стэк-массивом результатов (maxReplicas=8), targets() на
      стэк-скратче, refcounted payload из sync.Pool для отставателей
      (steady-state 0 allocs/pair), тонкие чтения неавторитетны (реплика-
      отставатель не даёт нулей — нули только когда все реплики сообщили
      «нет объекта»), дедлайн операции в sdsnet.Conn (ctx-дедлайн вызывающего
      либо opTimeout 30с — detached-операции не виснут вечно): Write4K
      288→52мкс (128→11 allocs op, 0 в steady state), Read4K 255→18мкс
      (93→0 alloc); пул каналов кворума, detached-контекст отставателей без
      аллокаций (value-структура вместо WithoutCancel), синхронный путь
      одной реплики (MVP-конфиг: без горутин/каналов/копий — Write4KSingle
      26мкс / 2 allocs, против 58мкс / 11 у реплик-3); (3) свой бинарный
      протокол (фикс. заголовок, батчинг, recvmmsg/sendmmsg) — NBD/VDUSE
      только гостю, не внутри кластера; (4) VDUSE multi-queue уже готов —
      их NBD однопоточный, UBLK в их стеке новее и тоже MQ (следить);
      (5) CRUSH precomputed-таблицы (компиляция 1x/epoch → placement =
      table lookup, их PG-генерация comparable); (6) RDMA = stretch (golang.org/x
      нет — cma через cgo, решения ищем при наличии стенда). Честно: их 0.14 мс
      — RDMA+оптимум; наши ворота ниже — это потолок-мишень, а не обещание.
- [ ] Бенч-стенд: 3–5 нод, NVMe, 25/40GbE; профили из `tests/fio.txt` +
      randread/randwrite 4K/64K/1M, depth 4/16/128
- [ ] **Отказоустойчивость по домену отказа (по мотивам Vitastor pool config)**:
      механизм уже есть (CRUSH `failure_domain` = chooseleaf distinct rack, golden-тесты),
      взять из Vitastor три вещи: (1) DONE `min_size` тома = их pg_minsize — поле
      VolumeSpec.MinSize, кворум записи W=min_size, ниже — write fast-fail ErrDegraded,
      чтения толерантны (любая живая реплика); флаг sdsd `-min-size`; тесты
      TestClusterMinSize*; (2) DONE `out_time`-выдержка (default 600 с) перед вытеснением
      упавшего OSD из placement — защита от ребаланса на «мигающих» OSD: клиентский
      детектор (только транспортные ошибки; любой протокольный ответ = жив), prober
      пингует по свежим соединениям, evict => meta weight 0 (placement исключает),
      восстановление => weight 1; SetOutTime(0) = noout; тесты TestClusterOutTime*;
      (3) пресеты правил DONE (crushkv 961dd00): `crush.RackAwareRep3(root, domain)` —
      rep3 с failure_domain по выбору (rack/ДЦ: уникальность гарантирована на уровне
      выбранного домена, выше — нет, как в Ceph); `crush.EC2Plus2AcrossDCs(root, dcType)` —
      EC k+m, chooseleaf_indep по ДЦ = их level_placement dc=NN..N (один шард на ДЦ);
      `crush.DCPreferred(ruleset, dcBucket, domain)` = их `dc ?= "x"` (правило на ДЦ,
      адресация правил — индекс в слайсе, Ruleset = метаданные). numrep=0 → размер пула.
      Тесты TestPreset* (5 шт, -race).
      Справочник Vitastor: docs/config/pool.ru.md (failure_domain, level_placement,
      raw_placement DSL, pg_minsize, osd_out_time, noout, reweight, tags).
- [ ] Цели (ориентир Vitastor highload-доклады): 4K randread **> 100k IOPS/ноду**
      на 3-реплики по TCP, **> 200k** с MSG_ZEROCOPY; латентность single
      4K read < 0.2–0.3 мс (их 0.14 мс — с RDMA; зафиксировать: RDMA — stretch)
- [ ] Оптимизации по результатам perf: pinned per-CPU workers (SO_REUSEPORT /
      EPOLL_TAG), registered io_uring fds+bufers, отключение GC-давления
      (GOGC off на data-потоках / ballast), hugepages для буферов,
      NUMA-affinity, опционально kernel-bypass (netmap/eBPF) — только если
      perf покажет сеть узким местом
- [ ] **In-memory мета-индекс объектов** (урок Vitastor: метаданные всегда в
      RAM, ~512 МБ/ТБ при блоке 128K; журнал по умолчанию тоже в RAM — 32 МБ
      на SSD-кластер): OSD не читает заголовок объекта с диска на пути
      запроса; per-object версия живёт в RAM-индексе (дешёвый CAS/версионность)
- [ ] **Ленивый fsync: операция SYNC + режим immediate_commit=off** (урок
      Vitastor): запись без fdatasync, клиент кэширует незафиксированное в
      памяти, повторяет после реконнекта, SYNC каждые client_dirty_limit
      байт — главный источник их цифр на дешёвых SSD; NBD flush/SYNC → в
      Volume.Flush
- [ ] **Восстановление пообъектное, не PG-ами** (урок Vitastor; Ceph из-за
      PG-гранулярности получает взрывной recovery): recovery-воркер чинит
      отдельные объекты строки pg
- [x] **Клиент «вечно ждёт», а не ошибается** (урок Vitastor: EPIPE →
      бесконечный retry): DONE._hardMode_ по умолчанию = hard: write при
      недоборе кворума (ErrDegraded из targets) засыпает в awaitQuorum —
      backoff 10→200 мс + forceRefresh, пока живых реплик не станет ≥ W;
      quorumWrite-ошибки (обёртки errQuorumFailed/errQuorumLost) ретраятся
      там же; запись с «мёртвыми транспортами» на всех целях тоже ждёт.
      Чтение ждёт при нуле живых целей и при смерти транспортов на всех
      целях (протокольный ответ = ОСД жив, ошибку отдаём сразу); при
      частичном кворуме чтение остаётся терпимым (любая реплика). Всё
      снимается отменой ctx. SetHardSemantics(false) = мягкий режим
      (быстрый отказ, 3 попытки) — тесты TestClusterHardSemantics*,
      TestClusterReadWaitsForTransports.
- [ ] Тест-кейс «призрачные удаления» (их известный баг): create/delete
      тома в деградированном кластере + подъём вернувшихся OSD — удалённые
      объекты не должны воскреснуть (epoch fencing это покрывает — проверить)
- [ ] Опционально: **монитор-ребалансировщик** (аналог Vitastor lp_solve,
      равномерность 96–99%): stateless-демон поверх etcd правит веса CRUSH
      по free-space и публикует новые epoch; OSD с ним не общаются (только
      etcd). Наших χ²=25 при 65k pg хватает без LP — LP только для кластеров
      с сильной неравномерностью дисков
- [ ] Регулярный сравнительный отчёт против Vitastor на том же железе
      (fio-скрипты в `bench/`)

## Фаза 6 — Функциональность уровня Ceph-фич (после 1–5)

- [ ] снапшоты (copy-on-write по OID-поколениям), клон volumа
- [ ] thin provisioning + UNMAP/discard (в NBD и vhost-user-blk)
- [ ] erasure coding (RS) для холодных классов
- [ ] квоты, QoS (IOPS/BPS-шейпинг per-volume)
- [ ] бекапы/экспорт (qemu-img convert-совместимый raw-стрим)
- [ ] AES-256-XTS шифрование per-volume (ключ в MDS)
- [ ] balancer: автоматический reweight по free-space (веса в CRUSH map)

### Сравнение с Vitastor по placement (по их исходникам, /tmp/vitastor)

   Источник архитектуры: vitastor/docs/intro/architecture.ru.md (PG-таблица в
   etcd + lp_solve; NBD «устарел» → VDUSE; метаданные и журнал в RAM;
   lazy fsync/SYNC; recovery объектами; монитор = node.js поверх etcd).

   | Аспект | Vitastor | Мы (crushkv) |
   |---|---|---|
   | Алгоритм размещения | **нет CRUSH**: формула O(1) для EC + таблица `pg_mapping` в etcd, составленная офлайн LP-планировщиком | CRUSH-деревья (straw2 и др.), детерминированный выбор на клиенте, карта в MDS |
   | Balance при изменениях | перестроение таблицы офлайн-планировщиком + массовый трафик переезда (LP минимизирует movement) | CRUSH-минимальный movement по построению (движок протестирован на movement-свойствах) |
   | Чтение карты в data path | etcd в пути клиента (Watch pg_mapping) — плюс: централизованный контроль; минус: etcd в критическом пути | карта epoch-ами в памяти клиента, кэш pg→OSD; компиляция карты один раз на epoch |
   | Веса/ревилиты | per-OSD утилизация в таблице планировщика | веса 16.16 в карте, reweight in/out из коробки (byte-exact Ceph) |
   | Failure domain | задаётся планировщиком руками/скриптами | топология деревом (region→zone→rack→host→osd), правила `failure_domain` |
   | Скорость placement | O(1) табличный lookup (очень быстро) | straw2 3-уровня rep3: 34.5 мкс/выбор (225 OSD), 0 аллокаций; кэш pg→OSD даёт тот же O(1) на горячем пути |
   | Совместимость | своя система | **байт-в-байт совместимость с Ceph CRUSH** (текст `.crush` и бинарный CrushWrapper) — можно брать готовые Ceph-крашмапы |

   Вывод: у Vitastor плюсы — O(1) и отсутствие movement-шторма при EC за счёт
   офлайн-планирования; минусы — etcd в пути клиента и отсутствие стандарта
   карты. Наш путь: CRUSH (переносимый, проверенный 15-ю годами Ceph) + кэш
   pg→OSD = тот же O(1) на горячем пути без внешних зависимостей.

   **Тезис «CRUSH тяжелее vitastor-формулы по вычислению placement» — верен,
   и это принято в дизайн (замеры Apple M1, `crush/bench_cache_test.go`):**
   - сырой `DoRule` (225 OSD, straw2, rep3): **38.5 мкс/выбор** → ~26k
     выборов/сек на ядро: на каждый I/O (при 1M IOPS бюджет 1 мкс/IO) не годится;
   - вариант Vitastor (формула/таблица): ~наносекунды — на 4 порядка быстрее;
   - поэтому горячий путь — **кэш pg→OSD**: hash(OID)→pg (фибоначчи-умножение)
     + lookup в массив: **1.4 нс/IO, 0 аллокаций** (map-вариант 8.8 нс) — тот же
     порядок, что у Vitastor;
   - CRUSH считается только на смене epoch: пересчёт всей таблицы
     4096 pg (rep3, 225 OSD) = **~20 мс** на весь кластер — событие отказа/
     топологии, не I/O; таблица 4096×3×int32 = 48 КБ в кэше L1/L2.
   Зафиксировано в спеке фазы 3: объект никогда не считает CRUSH per-IO.

### Бенчмарки Vitastor (инвентарь по их репо, /tmp/vitastor)

Их автотесты делятся на **функциональные** (62 скрипта `tests/test_*.sh`) и
**немногие микробенчи**. Важно: **плэйсмент они не бенчмаркают вообще** —
потому что у них там O(1)-формула/таблица, считать нечего. Что есть:

- `src/test/stub_bench.cpp` — сырой TCP-стресс **протокола OSD** против
  `stub_osd`: в цикле read 4K + write 4K + `SYNC_STAB_ALL` на один inode,
  считает среднюю латентность каждой операции. Замеряет сетевой стек +
  обработку опций OSD, **не** диск и **не** пле́йсмент.
- `src/cmd/cli_cpubench.cpp` (`vitastor-cli cpubench`) — CPU-микробенч
  **транспортного шифрования и чексумм**: AES-256-GCM (заголовок/полезная
  нагрузка), AES-256-XTS (e2e-шифрование) и xxhash3, по 1M и 4K блокам, в
  MB/s. На EPYC 9575F: xxhash3 1M ≈ 104 GB/s, AES-XTS+GCM 1M ≈ 12.6 GB/s.
  Это про CPU-стоимость криптографии, к пле́йсменту отношения не имеет.
- `docs/performance/bench2.{en,ru}.md` — **публикуемый** результат на fio
  (3 ноды, 8×PM9A3, Xeon 6342, 25GbE+RoCEv2, Vitastor 1.3.1): 4K randread
  T1Q1 ≈ 8.7–9.0k IOPS TCP (и 4.27M Iops T12Q128 RDMA), линейное чтение
  ~13–16 GB/s; отдельно CPU-потребление на OSD и на клиенте. Узкое место по
  их же выводам — **CPU и сеть, не диски**.
- `tests/test_write.sh` и др. — fio прогоняется как **функциональная**
  проверка (мелкие последовательные, 68K randwrite, fsync-шторм, линейная
  запись), с фиксацией регрессий, не как эталонный бенч.

**Что берём себе** (фаза 5, `bench/`):
1. Тот же профиль fio, что у них в bench2 (4K T1Q1 / T12Q128, 4M линейные,
   rand 4K/64K/1M, depth 4/16/128) на том же железе → прямое сравнение.
2. Свой отдельный **placement-бенч** (у Vitastor такого нет): у нас это
   `crush/bench_cache_test.go` (DoRule / pg→OSD кэш / epoch-rebuild) —
   закрывает их слепое пятно и доказывает, что кэш выводит нас на их же
   наносекундный порядок на горячем пути.
3. Крипто-микробенч по образцу `cpubench` (AES-XTS/GCM + чексумма) — когда
   добавим шифрование (фаза 6), чтобы видеть CPU-цену on-the-fly.
4. `stub_bench`-аналог (сырой TCP-стресс протокола против stub-OSD) для
   изоляции сетевого стека от дискового — дёшево и полезно на ранних фазах.

Вывод: у Vitastor **нет** бенча пле́йсмента, потому что пле́йсмент тривиален;
их бенчи — это конец-ту-конец fio + крипто-CPU + сырой TCP-стресс. Наша
сильная сторона — отдельный измеренный placement-путь (crushkv), которого у
них нет; наша слабость — пока нет end-to-end fio-стенда (в плане, фаза 5).

## Риски / решения к зафиксировать

1. **io_uring только Linux** — продуктовая платформа Linux; macOS только для
   разработки (fallback-путь preadv2).
2. **«NVMe в госте»**: честный выбор — NVMe-эмуляция QEMU поверх NBD (просто,
   медленно) vs virtio-blk через vfio-user (быстро, в госте `/dev/vd*`).
   Решение владельца: MVP=NBD+nvme, целевой=vhost-user. Не называть в GUI
   «NVMe» то, что отдаёт virtio-blk.
3. **Скорость Go против C++ Vitastor**: достижимо при no-alloc data path +
   io_uring + минимум протокола (их же выводы: CPU — узкое место). Риск:
   GC → **mitigation реализован и измерен** (crushkv/docs/GC_ARENA.md):
   placement в mmap-арене, 0 B/op на горячем пути; тот же приём (arena +
   переиспользование буферов) — обязательный стандарт для I/O-пути storage.
4. **CRUSH-совместимость** с Ceph-форматом карты: **реализовано в crushkv** —
   читаем и пишем и текстовый `*.crush`, и бинарный CrushWrapper Ceph
   (byte-exact движок + golden-тесты). У Ceph есть PG-слой, а у нас объект
   выбирается напрямую (свой pg→OSD-кэш при необходимости) — совместимость
   по формату карты и алгоритму выбора, семантика верхнего слоя своя.
5. Объём: это год+ полной разработки до production. MVP (фазы 0–4a) —
   несколько недель до «NVMe в госте».
