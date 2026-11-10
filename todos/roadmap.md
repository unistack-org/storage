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

Прикладной срез под конкретную задачу (тестовые ВМ без гарантий, 2 ноды /
4 NVMe, загрузка и скачивание дисков по HTTP, снапшоты, бекапы, форма
поставки «демон + контрол») — **[test-vm-plan.md](test-vm-plan.md)**: там
порядок недель, минимальный набор для передачи полигона и открытые решения
владельца.

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

> Статус (2026-11-09): код и тесты — готово; открыта только приёмка fio на Linux+NVMe.

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
      split по 4M-границам, WRITE_ZEROES через punch hole (Linux fallocate,
      darwin fcntl(F_PUNCHHOLE) — APFS отдаёт блоки), выравнивание 4K +
      zero-fill краёв, fallback zeroes только на платформах без punch
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

## Фаза 2 — Сеть и репликация (3–5 недель) — ✅ сделано

> Статус (2026-11-09): все пункты закрыты (протокол, репликация кворумом, heartbeat/out_time, pipeline/zerocopy/io_uring); e2e-приёмка на Linux — вне репозитория.

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
- [x] pipeline/batch запросов (`sdsnet.Stream`: окно+батч-флаш+backpressure),
      `MSG_ZEROCOPY` (Linux, `ZerocopyThreshold`, errqueue-синхронизация),
      приём через io_uring (`store.Options.AsyncRing`, fallback pread/pwrite)
- [x] heartbeat, деградация при потере реплики, recovery-трафик приоритетом —
      heartbeat = `out_time`-выдержка + prober (пинг упавших OSD по свежим
      соединениям; evict => weight 0, возврат => weight 1; см. Фазу 5
      «Отказоустойчивость»); деградация: чтения терпимы (любая живая реплика,
      порядок предпочтений local-first), запись ниже min_size fast-fail
      `ErrDegraded` + awaitQuorum (hard-семантика, тесты TestCluster*);
      recovery/vacuum-трафик paced QoS-лимитером и всегда ниже клиентского
      I/O; авторегулирование скорости вынесено отдельной позицией
      (`recovery_tune_interval`, Фаза 5b)
- [x] Тесты: chaos (смерть сервера, frozen replica, пул-давление, обрывы), `-race`
- Примечание: исправлен дедлок `iouring.fallbackRing.Submit` (mutex держался при
  отправке в unbuffered канал; файлы вынесены под отдельный filesMu)

## Фаза 3 — Кластер, метаданные, CRUSH (4–6 недель) — «как Ceph» — ✅ сделано

> Статус (2026-11-09): все пункты закрыты (etcd/raft-мета, CRUSH-импорт, PG-таблицы, map check/publish/reweight, CLI `unistorctl`).

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
  - [x] Валидатор карты CLI-командой `map check` — сделано: компиляция карты
        против реестра + ценз PG-таблицы (full/degraded/down), reweight in/out
        покрыт тестами (см. выше). Rule-валидатор под volume-class (SSD/HDD)
        **сделано 2026-11-10** (crushkv `crushmap.CheckClass`/`CheckWeights`):
        `map check -class <name>` — правила без class-фильтра против устройств
        класса, warnings на недостижимые max_size/chooseleaf-домены; при живом
        реестре — веса (OSD вне реестра = 0).
- [x] sds-nbd: бэкенд `VolumeBackend` (том → NBD-экспорт) и режим `sdsd
      -cluster <meta-каталог>`: OSD в процессе (`-osd id@dir[@addr]`),
      публикация карты (`-crushmap -epoch`), `-create имя:размер`,
      `-export имя` — том как NVMe-диск в госте
  - [x] Смоук: 3 OSD, том 1G rep3 pg64, sdsbench поверх NBD (write 64K ~79 МБ/с,
        randread 4K ~2.7k IOPS loopback — потолок отладочной сборки, не цель),
        рестарт sdsd данные на месте; тесты: roundtrip, кворум с мёртвым OSD,
        stale-epoch рефреш — `pkg/client/client_test.go`
- [x] **Meta-консенсус: embedded raft поверх библиотеки etcd** (`go.etcd.io/raft`,
      та что извлечена из etcd — не внешний сервер): `pkg/meta/raftstore.go` +
      `rafttransport.go` + `raftproxy.go` (пиши-прокси follower→leader),
      `-raft/-raft-peers/-raft-dir/-raft-proxy` в `unistord`, witness-voter без
      дисков; без кворума meta read-only (503). DirStore остаётся для
      лаборатории. Тесты: `pkg/meta/raftstore_test.go`.
  - [x] **Meta на 2+1 нодах (полигон из 2 машин)**: третьим голосом —
        witness-voter (нода без OSD/экспортов), кворум 2/3 переживает потерю
        одной storage-ноды; плавающего адреса нет — клиенты знают всех
        участников (raft-peers).
- [x] CLI `unistorctl`: osd add/ls/out/in, vol create/ls/rm/info/quota/resize/
      qos/import/export/snap/clone, map dump/check/publish/reweight (dump
      -format raw|text|json/yaml/binary), vacuum ls/run (включая пер-томный),
      recovery ls/check/run, status (машиночитаемый, -json), healthcheck
      (exit-code для оркестрации). Все команды — оболочка над HTTP API unistord.
- [x] **CRUSH — ключевой deliverable: вынесен в отдельный репозиторий
      [`crushkv`](https://github.com/unistack-org/crushkv) (`../crushkv`), ГОТОВО:**
  - [x] Движок `crushkv/crush` на Go — **byte-exact порт C-ядра Ceph**
        (builder.c/mapper.c/hash.c): все алгоритмы uniform/list/tree/straw/straw2,
        choose/chooseleaf, все tunables-профили (legacy…optimal), LN-таблицы,
        jenkins hash. Проверено ~6000 golden-векторами, сгенерированными
        настоящим C-кодом Ceph (`crush/testdata/golden_*.txt`) — 100% совпадение.
  - [x] `crushkv/crushmap` (ныне `crush/`): текстовый парсер `*.crush` (совместим с Ceph),
        JSON, и **двоичный кодек формата Ceph CrushWrapper** (`binary.go`:
        magic 0x00010000, bucket-slots с per-alg хвостами, rule-slots,
        string-мапы с историческим 64-бит quirk, tunables, class maps,
        choose_args) — decode/encode, круговой тест 600 golden-векторов через
        text→binary→decode→compile→placement, байт-стабильный encode.
  - [x] **Zero-GC data path**: placement считается в mmap-арене
        (`crush.Work` + `docs/GC_ARENA.md`): 225 OSD, 3 уровня, straw2, rep3 —
        **34.5 мкс на выбор, 0 B/op, 0 allocs/op**; carve из арены 290 нс vs
        690–940 нс у `make()`. GC на пути размещения отсутствует полностью.
  - [x] Тесты: воспроизводимость (golden Ceph), reweight in/out (вес 0),
        movement-свойства CRUSH — в `crush/*_test.go`.
  - [x] Валидатор карты CLI-командой `map check` — сделано (компияция карты
        против реестра + ценз PG-таблицы full/degraded/down, non-zero exit при
        down-строках); reweight покрыт `map reweight` + тестами. Эмуляция
        отказов и chi-square равномерность покрыты тестами:
        `storage/pkg/placement/uniformity_test.go`. Rule-валидатор под
        volume-class (SSD/HDD) **сделано 2026-11-10**: crushkv `crush.Check`
        (AST: правила/step/классы, pop-count rep3/rep2) + `CheckClass`/
        `CheckWeights` (правила против устройств класса и весов реестра), в CLI —
        `unistorctl map check [-class ssd|hdd]` (exit 1 на ошибках).
- [x] CLI `unistorctl`: osd add/ls/out/in, vol create/ls/rm/info/quota/resize/
      qos/import/export/snap/clone, map dump/check/publish/reweight (dump
      -format raw|text|json/yaml/binary), vacuum ls/run (включая пер-томный),
      recovery ls/check/run, status (машиночитаемый, -json), healthcheck
      (exit-code для оркестрации). Все команды — оболочка над HTTP API unistord.

## Фаза 4 — Экспорт в ВМ: «NVMe внутри виртуалки» (2–4 недели)

> Статус (2026-11-11): NBD + VDUSE (мультиочерёдный, online-resize, reconnect, base:allocation, lock-free IOTLB) готовы; `pkg/vub` (vhost-user-blk) реализован и покрыт fake-QEMU e2e-тестами (handshake, rings, multi-queue, GET_VRING_BASE/resume, RESET_DEVICE, reconnect) — все зелёные под `-race`; CLI `-vub export=sock[:ro]` в unistord; остаётся: 4b e2e на Linux+KVM.

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
- [x] **4b Целевой: Go vhost-user-blk server (`pkg/vub`) — код готов, e2e на Linux+KVM** — unix-сокет,
      handshake (GET/SET_FEATURES, GET/SET_PROTOCOL_FEATURES + REPLY_ACK,
      SET_OWNER, SET_MEM_TABLE с SCM_RIGHTS-fd, SET_VRING_NUM/ADDR/BASE/KICK/
      CALL/ENABLE, GET_VRING_BASE(+SKIP_DRAIN), RESET_DEVICE, GET/SET_CONFIG),
      mmap гостевой памяти, разбор desc/avail/used rings (split, indirect),
      multi-queue, диспетчер kick=unix.Poll+неблокирующий read (переносимо:
      SCM_RIGHTS-fd не регистрируется в netpoll на macOS), батч IRQ на call-fd,
      F_RO/F_FLUSH/F_DISCARD/F_ZEROES, GET_VRING_BASE drenит inflight
      (save/restore на тех же fd), reconnect-сессия (reset при дисконнекте).
      `unistord -vub vol0=/run/vol0.sock[:ro]` (-vub-queues/-vub-workers),
      admin API exports type=vub. Тесты: protocol-фрейминг + fake-QEMU e2e
      (11 тестов, -race, -count=2) — все зелёные на macOS; кросс-сборка linux
      чистая. QSD как мост `--export type=vfio-user` → QEMU
      `-device vfio-user-pci` не требуется (прямой vhost-user-blk).
      ⚠ Проверено: домашний хост (macOS, homebrew QEMU 11.1.3) НЕ имеет
      `vhost-user-blk-pci`/`vfio-pci`/vhost-объектов — живой QEMU-e2e 4b
      валидируется только на Linux+KVM (x86_64 или aarch64 Linux-хост с QEMU ≥ 8)
- [x] **Интеграция с `unistack/pkg/storage` (ушла в VDUSE-транспорт).**
      Клетка подключается драйвером `unistack-sds`
      (`unistack/pkg/storage/unistack_sds.go`) к admin API unistord
      (ADR-059): volumes/import-jobs/snapshots/exports + bearer-токен.
      Attach — VDUSE (`POST /v1/exports type=vduse` → `vdpa dev add` →
      QEMU `vhost-vdpa-device-pci`), вариант `pkg/qemu Drive.VfioUser`
      не потребовался
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
- [x] **Реконнект VDUSE-демона (commit bb64488d, проверено на железе)**:
      устройство переживает смерть демона; новый процесс переоткрывает
      `/dev/vduse/<имя>`, восстанавливает кольца (`VQ_GET_INFO`), переassign
      kickfd (ядро реплеит kicks), заново тянет IOTLB (полный `IOTLB_GET_FD`
      pull + pull-on-miss: драйвер virtio-vDPA не шлёт UPDATE_IOTLB).
      Дроп старых avail-записей под fault-guard (SetPanicOnFault+recover):
      SIGBUS по unmapped guest-памяти не убивает демон, запрос гасится EIO+IRQ.
      Ограничение ядра (для всех реализаций): `vduse_dev_release` снимает все
      IOTLB-маппинги при закрытии fd — запросы «в полёте» на момент смерти
      завершить нельзя; плановый restart — только с quiesce гостя, аварийный —
      лечится hot-unplug/plug устройства VM. Детали: `bench-stand.md`
- [x] systemd keep-alive юнит для VDUSE-демона + runbook восстановления
      (`sds-nbd/deploy/unistord.service` + `sds-nbd/deploy/README.md`)
- [ ] e2e-тест: Linux+KVM, гость пишет/читает, fio внутри гостя
      (нужна чистая загрузка стенда 1.100 — согласовать окно)

## Фаза 5 — Производительность до уровня Vitastor (постоянно, с замерами на каждую фазу)

> Статус (2026-11-10): 171–177K 4K randread (цель 100K — перекрыта), отказоустойчивость (min_size/out_time/пресеты) закрыта; закрыты in-memory мета-индекс (ADR), замер WA в sdsbench (WA=2.01), метрика свободного места журнала, write-intent journal (`-intent`, damage report при восстановлении), упаковка journal-записей (packed-пачки в `pkg/journal`, intent-лог 40→24 Б/запись) и VDUSE read-ahead (§3.8: `-vduse-readahead`, окно 256 КиБ/очередь, portable-тесты; 2–3× подтвердить на Linux-стенде); закрыты ручки GC-давления (`-gogc`, `-ballast-mb`); открыто: AWUPF/`RWF_ATOMIC` (цель WA→1), бенч-стенд 3–5 нод, cpubench, per-CPU оптимизации (по perf-профилю), цель >200K.

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
- [x] **Отказоустойчивость по домену отказа (по мотивам Vitastor pool config)**:
      механизм уже есть (CRUSH `failure_domain` = chooseleaf distinct rack, golden-тесты),
      взято из Vitastor всё: (1) DONE `min_size` тома = их pg_minsize — поле
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
- [x] Цели (ориентир Vitastor highload-доклады): 4K randread **> 100k IOPS/ноду**
      на 3-реплики по TCP — **сделано 2025-11-08: 171–177K** (конвейер NBD,
      см. `todos/bench-stand.md` «Фикс чтения»; Vitastor 129K обогнан на 34%).
      Остались: **> 200k** с MSG_ZEROCOPY; латентность single
      4K read < 0.2–0.3 мс (у нас QD1 89 µs — уже выполнено; их 0.14 мс — с RDMA; зафиксировать: RDMA — stretch)
- [ ] Оптимизации по результатам perf: pinned per-CPU workers (SO_REUSEPORT /
      EPOLL_TAG), registered io_uring fds+bufers, отключение GC-давления
      (GOGC off на data-потоках / ballast), hugepages для буферов,
      NUMA-affinity, опционально kernel-bypass (netmap/eBPF) — только если
      perf покажет сеть узким местом.
      => Частично (2026-11-10): ручки GC-давления готовы — `-gogc N`
      (debug.SetGCPercent) и `-ballast-mb N` (удерживаемый балласт поднимает
      floor живучей кучи; проверено: RSS растёт на размер балласта).
      Остальное упирается в профиль: на стенде нужен `perf record` под
      sdsbench-нагрузкой, по нему выбираем следующую точку (per-CPU workers
      vs registered buffers vs hugepages). План замера: 1) perf под 4K
      randread 100K+, 2) grep по GC-фреймам (gcBgMarkWorker, runtime.mallocgc
      share), 3) только при >10% GC-доле — включать gogc/ballast в тестовый
      профиль; при сетевой доле >40% — zero-copy/registered buffers.
- [x] **In-memory мета-индекс объектов** — закрыто 2026-11-10 (ADR:
      `todos/adr-meta-index.md`). `objMeta{size,alloc,allocKnown}` — 24 Б на
      объект, `map` под `Store.mu`; инвариант «один объект — одна запись меты»
      выполняется тривиально (блок меты = объект; durable-меты нет, индекс —
      кэш поверх POSIX-файлов, восстанавливается одним `ReadDir` без per-file
      stat). Цена ~72 Б/объект ≈ **18 МБ на ТБ** (3 порядка против их
      663/371/152 МБ/ТБ — у них индекс на 4K-блок, у нас на объект 4 МиБ).
      OSD не делает `os.Stat` на пути запроса: `Load` (внутри `New`) +
      `openSlow`/запись/рост/punch/удаление ведут индекс под тем же замком,
      что и учёт. В journal-режиме `Stat`/`StatInfo` берут max(индекс, оверлей)
      (alloc — нижняя граница, не кэшируется), новый объект с оверлейными
      записями: `Stat` = not-exist (контракт уровня файла), `ObjectState` =
      present=false/pending=true (NBD base:allocation видит данные до файла).
      Наблюдаемость: `MetaInfo` → `/v1/status` (`meta_objects`/`meta_ram`) и
      gauge `unistor_osd_meta` (существующий экспорт под VictoriaMetrics).
      Тесты: `pkg/store/stats_test.go`, `stat_overlay_test.go`,
      `statinfo_test.go`. Осталось по желанию: per-object версия в индексе
      (версионность/CAS на OSD — только когда понадобится linearizable read).
- [x] **Чтение с любой реплики + выбор primary по номеру PG** — закрыто:
      чтение и так падало на любую живую реплику (fall-through в `readChunk`),
      добавлен порядок предпочтений `Volume.orderReadRefs` (`pkg/client`):
      «локальный OSD (`SetLocalOSDs`, пара к `SetLocalDial`) → наименее
      загруженный (`Pool.Inflight` в `pkg/sdsnet`) → порядок pg-строки
      (primary-by-PG как стабильный tiebreak)». Хуки в `readChunk` и
      `readGenChunk` (снапшоты); `unistord` объявляет свои OSD локальными.
      Тесты: `pkg/client/replica_read_test.go` (выживание при гибели 2/3
      реплик; локальная реплика забирает всё чтение — по счётчику `Pool.Ops`).
      Осталось по желанию: флаг локальности для мультi-DC раскладки.
- [x] **ENOSPC и полный журнал** — закрыто: `store.ErrNoSpace` → `StatusNoSpace`
      на wire (`pkg/sdsnet/server.go`), ENOSPC реплики НЕ считается гибелью OSD
      (не в `noteFailure`), клиент превращает его в `ErrNoSpace` без зависания
      (`pkg/client/client.go`, writeChunk); journal: полный журнал →
      `ErrNoSpace` из `pkg/journal` (sticky poison) наружу, не падение OSD;
      `/v1/status` отдаёт warnings (диск/OSD). Метрика свободного места под
      журнал **закрыта 2026-11-10**: `store.JournalStats` (размер WAL +
      неповыгнанные записи) → `diskStatView.journal` в `/v1/status` и gauge
      `unistor_osd_journal{name=log_bytes|pending_records|free_bytes|total_bytes}`
      (`free/total` — ФС, в которую растёт журнал; журнал живёт в каталоге
      store, поэтому это же `DiskSpace`); тест `TestJournalStats`. Попутно
      исправлен протухший alloc-hint мета-индекса (StatInfo/вакуум видели
      alloc=0 у записанных объектов) — регресс `TestStatInfoTracksAllocation`.
      Тесты: `pkg/sdsnet/nospace_test.go`, `pkg/client/quota_test.go`,
      `pkg/journal/space_linux_test.go`, `pkg/store/statinfo_test.go`.
      `pkg/store` + `pkg/osd` + `pkg/client`. Детали: `todos/vitastor-ideas.md` §4.2
- [ ] **Write Intent Journal + атомарные записи NVMe (цель WA → 1)** — смысл их
      доклада Highload'2025: перед записью данных пишем намерение «обновлю
      блок X, новый CRC=Y»; при восстановлении нет записи / CRC не совпал ⇒
      блок считается старым. Лечит torn writes без двойной записи. Плюс
      детект AWUPF (у Micron 7450/7500 и Kioxia CD7/CD8-R = 256K, у нашего
      PM9A3 = 0 — атомарен только 4K; флаг RWF_ATOMIC в ядре был сломан,
      важна именно атомарность при power-loss). **Замер WA в sdsbench
      закрыт 2026-11-10** (`todos/vitastor-ideas.md` §2.3): счётчики
      `store.WriteStats` → `/v1/status` → gauge `unistor_osd_write_bytes` →
      `sdsbench -admin` печатает `WA=`; на живом journal-кластере randwrite 4K
      даёт WA=2.01. **Intent-журнал закрыт 2026-11-10** (§2.1):
      `pkg/store/intent.go`, флаг `unistord -intent` (только non-journal
      режимы — journal уже имеет WAL), durable-интент 24 Б перед данными +
      буферный commit-маркер, recovery = одноразовый damage report
      (открытые интенты → `regionMatches` → warnings в `/v1/status` →
      truncate лога), delete-маркер на `Remove`, компакция при чистом
      закрытии; gauge `unistor_osd_intent` в том же exporter; тесты
      `pkg/store/intent_test.go` (в т.ч. torn write под `-race`).
      Стоимость ~1% трафика на 4K, WA не меняет (это про целостность,
      не про усиление). Остаётся: AWUPF-детект + `RWF_ATOMIC` (§2.2) —
      именно он даёт WA → 1 на атомарных 4K.
      Детали: `todos/vitastor-ideas.md` §2.1–2.3
- [x] **Клиентские I/O-потоки** (урок Vitastor: 7–8 GB/s линейно) — сделано:
      `sdsnet.Stream` (один конн = десятки горутин-сабмиттеров, один батч-
      флаш на кадры, окно Window, ctx-отмена, protocol-ошибки изолированы);
      `MSG_ZEROCOPY` опцией `ZerocopyThreshold` (аналог их
      `min_zerocopy_send_size`); io_uring в `pkg/store` опцией `AsyncRing`
- [ ] **`cpubench`-аналог** — цена чексумм/шифрования на ядро (у них
      xxhash3 1M ≈ 104 GB/s, AES-XTS+GCM ≈ 12.6 GB/s на EPYC). Без этого
      числа нельзя включать csum/шифрование на горячем пути (Фаза 5b)
- [x] **Ленивый fsync + режимы commit** (урок Vitastor): `-commit
      none|flush|sync|journal` в `pkg/store` (WAL в `journal.log`: ack =
      fsync журнала, фоновый applier разглаживает в объекты, replay+truncate
      на старте, read-your-writes через оверлей, Remove-чекпойнт,
      ENOSPC→StatusNoSpace, backpressure оверлея) и `-fsync-batch` —
      коалесинг гостевых FLUSH в один проход fsync; NBD flush/SYNC → в
      Volume.Flush. Тесты «подтверждённая запись переживает перезапуск» —
      в `journal_test.go`/`commit_test.go`
- [x] **Восстановление пообъектное, не PG-ами** (урок Vitastor; Ceph из-за
      PG-гранулярности получает взрывной recovery) — сделано (Recovery v1):
      recovery-воркер чинит отдельные объекты строки pg: `recovery check`
      сравнивает индексные списки OSD-источника и собирает план дыр
      (`recovery:<том>`: pending|running|done|failed, прогресс, копия в
      etcd), `recovery run` копирует 4M-объекты источник→приёмник
      (дистанционный pull, приёмник пишет себе локально), темп paced-QoS
      ниже клиентского I/O; `recovery ls`, метрики recovery-объектов;
      тесты mock-кластером (`pkg/client/recovery*`, `sds-nbd` e2e recovery)
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
- [x] Первый сравнительный отчёт против Vitastor 3.2.3 на общем стенде
      (unibench, 2×P4510 RAID1 — диск здоров, хост шумит LLM-инференсом):
      `todos/bench-stand.md`.
      Итог: QD1-латентность наша в 2–6× лучше (89/231 µs vs 537/459 µs);
      randwrite rep3 при QD128 — паритет (22.1K vs 22.3K = паритет по
      device-амплификации 3×, оба у потолка устройства ~65K);
      randread QD128 — просадка 3× (41.6K vs 128K) → адресная оптимизация
      пути чтения (цель ≥100K). Для Vitastor потребовались mon (Node.js) +
      lp_solve + etcd --max-txn-ops 10000 + failure_domain=osd + writeback
      (наш деплой = один Go-бинарник). Повторный прогон — скриптами,
      результаты на стенде в `~/bench/results/`

## Фаза 5b — Надёжность, безопасность, наблюдаемость (по урокам Vitastor)

> Статус (2026-11-09): закрыто ядро Ops: HTTP API+OpenAPI, CLI-оболочка, status/healthcheck, метрики, TLS, commit-режимы, SIGTERM-дрейн, онлайн-resize, import/export по HTTP, пообъектный recovery. Открыто: хаос-CI с сидами, каталог багов-инвариантов, Grafana-дашборд, авторегулирование recovery, авто-детект параметров тома.

Источник: полный разбор их ченджлогов 0.5.x → 3.2.3 и доклада Highload'2025 —
`todos/vitastor-ideas.md` (там же нумерация §, релиз-источник каждой позиции и
что у нас уже есть). Это то, что они нашли кровью в 3.0.x–3.2.x; у нас то же
самое дешевле лечить до прода.

- [ ] **Хаос-тестирование с сидами в CI** — тема №1 их свежих релизов (3.2.0–
      3.2.3; в 3.1.0 у них падало 83–97 симуляций из 200). Три шага:
      (1) детерминированная симуляция блочного слоя (`pkg/journal`,
      `pkg/store`, `pkg/osd`): completion'ы io_uring вразнобой/с задержкой,
      «выключение питания» = потеря случайной части volatile-кэша, записи
      целиком/частично/обрезанно, проверка модели данных и битмапов после
      каждого power-loss; сетка 24 конфигов (store × fsync × csum
      none/4K/16K × instant/two-phase), 200 сидов на сборку, repro по
      `--seed N`; (2) PG/replication-хаос с моделью согласованности
      (инварианты: нет потери подтверждённой записи, нет double-claim, нет
      чтения частичного блока как целого); (3) клиент/сеть — обрывы,
      переупорядочение, stale epoch, full journal → EAGAIN. Наш
      `pkg/replication/chaos_test.go` — зачаток: без сидов, без power-loss,
      без модели согласованности. Детали: `vitastor-ideas.md` §1.1–1.3
- [ ] **Каталог их багов как чек-лист инвариантов** (14 классов: PG висит в
      STOPPING/REPEERING, чтение на гонке с write/delete, ложная чексумма на
      частичном блоке, повтор sequence ниже `completed_lsn` после рестарта,
      >255 снапшотов, параллельное удаление снапшота, пустые записи и их
      csum, откат к версии 0, смешивание CAS/не-CAS, пропущенный fsync при
      батчинге, перегрузка etcd при флэппинге PG, ISA-L 2.31 = порча данных) —
      каждый класс закрывается тестом из предыдущего пункта. Полным списком:
      `vitastor-ideas.md` §1.6
- [ ] **Регресс-тест на каждый исправленный баг** + общий harness в
      `storage/tests/chaos/` (у них 62 функциональных скрипта; правило
      «фикс = тест»). Опционально: регулярный LLM-обход `pkg/` на баги с
      требованием минимального repro (у них в 3.0.14 так нашли >50 багов)
- [x] **Метрики Prometheus + дашборд** (первая половина) — экспорт
      `GET /metrics` в текстовом формате Prometheus (только отдельный
      `-metrics host:port`; admin REST API метрики не отдаёт);
      VictoriaMetrics скрейпит нативно. Покрыто: состояние кластера
      (osds/osds_live/healthy/epoch/volumes/raft), участники raft
      (`unistor_raft_member`), статус и диски ВСЕХ OSD кластера
      (up/weight/соединения + stored/free/total через новый протокольный
      зонд OpStatFS — аналог vitastor vitastor_osd_free_bytes), ёмкость
      кластера и томов (total/free/used_raw_bytes, raw_to_usable_bytes,
      space_efficiency — аналог vitastor_pool_*), состояние PG томов
      (pgs/pgs_ok/pgs_degraded/pgs_down — аналог vitastor_pg_count),
      счётчики recovery (copies/bytes), латентность-гистограммы и
      IOPS/BW по операциям (OSD-сервер и экспорты, через hook-колбэки
      `sdsnet.ServerMetrics` — ядро `pkg/` осталось без зависимостей),
      QoS sustained-рейты. Статистика — через count/sum пары
      (rate/histogram_quantile), не «сумма средних». Осталось: длины
      очередей, write amplification, сам дашборд Grafana.
      Детали: `vitastor-ideas.md` §6.1–6.2
- [x] **Graceful SIGINT/SIGTERM** во всех серверах (`sdsd`, `sds-nbd`,
      `pkg/vduse`): дрейн и закрытие сокетов схемой `SO_LINGER` + `shutdown()`
      + `close()` (у них без этого мусор в io_uring); `/healthcheck` в
      meta/мониторе для оркестрации. Детали: `vitastor-ideas.md` §6.3–6.4
- [x] **Безопасность транспорта (дешёвая часть рецепта 3.1.0)**: mutual TLS
      (`-tls-cert/-tls-key/-tls-ca`, `pkg/tlsconf`) на OSD data plane
      (`pkg/sdsnet/server.go` TLS listener), raft-транспорте и admin API;
      токен admin API (`-admin-token`, `Authorization: Bearer`); тесты
      `pkg/sdsnet/tls_test.go`, `pkg/tlsconf/tlsconf_test.go`.
      Осталось (дорогая часть, по запросу): AES-GCM шифрование заголовков в
      sdsnet, транспортные/дисковые чексуммы xxh3, ACL пользователей,
      AES-256-XTS E2E (Phase 6). Детали: `vitastor-ideas.md` §5
- [x] **Режимы `immediate_commit` (flush/journal/none) + fsync-батчинг** —
      сделано: `-commit none|flush|sync|journal` + `-fsync-batch`
      (`pkg/store`, `unistord`); journal = WAL с ack по fsync журнала,
      фоновое разглаживание, replay при старте; тесты power-loss —
      `pkg/store/journal_test.go` (crash survival, torn tail, remove
      checkpoint). Клиентский writeback-буфер не делали (только для бенчей,
      см. Риски п.6). Детали: `vitastor-ideas.md` §4.1
- [ ] **`recovery_tune_interval`** — авторегулирование скорости
      recovery/rebalance по влиянию на клиентский I/O, приоритет клиента над
      восстановлением; стыкуется с «восстановление пообъектное» выше.
      Частично: приоритет клиента уже обеспечен (recovery/vacuum идут через
      paced QoS-лимитер ниже клиентского трафика, лимит задаётся вручную);
      автоматической подстройки темпа по метрикам задержки клиента нет
- [x] **Онлайн-resize экспорта** (`sds-nbd`, `pkg/vduse`) — рост на лету
      реализован: `cs.size` атомарный, обновляется из бэкенда при запросе за
      старую границу; resize без реконнекта, VDUSE уведомляет гостя за ~2 с.
      Осталось: online expand в CSI (внешний проект). Детали:
      `vitastor-ideas.md` §4.5
- [x] **OpenAPI-спека админ HTTP API** — `api/openapi.yaml` (3.0.3): все 25
      путей админ-API unistord (`admin.go`/`exports.go`/`jobs.go`) со схемами
      запросов/ответов, Bearer-авторизация (`-admin-token`), octet-stream для
      импорта/экспорта блобов; `/metrics` — отдельный слушатель, вне спеки.
      HTTP API уже живёт в демоне; CLI поверх него — ниже.
- [x] **HTTP API контролла (идея `unistorage serve`): HTTP API + OpenAPI** —
      CLI (`unistorctl`) — оболочка над HTTP API unistord (это дало
      интеграции с CSI/дашбордами); `status` машинночитаемый
      (`-json`/`-json-summary`) с предупреждениями: диск 95%, нет
      кворум-строки, флэппинг OSD, отставание recovery; OpenAPI
      `api/openapi.yaml` (см. выше). Не сделано: отладочные
      `pg-list`/`osd-tree`/`raw-ls`/**kv-rescue** (дамп блоков меты в обход
      структуры — их способ спасать данные при порче меты; частично
      закрыто `recovery check` + map dump). Детали: §6.5–6.9
      **Форма поставки зафиксирована (2026-11-08): ровно два бинарника** —
      `sdsd` (данные: OSD + экспорт в ВМ + служебный HTTP этой ноды, `/v1/*`
      и `/metrics`) и `unistorage` (единственный контрол, клиент того же
      HTTP API). HTTP живёт внутри демона (каркас — уже используемый `-pprof`),
      отдельного API-демона нет. Реализовано как `unistord` + `unistorctl`
      (переименовано при доставке). Детали: `test-vm-plan.md` §3
- [x] **HTTP import/export диска** (требование полигона тестовых ВМ) —
      реализовано в `sds-nbd/cmd/unistord/admin.go`: `PUT
      /v1/volumes/{name}/import?offset=N` (поток octet-stream, чанки 1 МиБ
      через `Volume.WriteAt`, resume по смещению, xxh3-хэш потока), `POST
      /v1/volumes/{name}/import` `{"url":...}` — асинхронный импорт-джоб с
      докачкой; `GET /v1/volumes/{name}/export` — raw-стрим окнами в
      переиспользуемый буфер, разреженно (дыры → нули). Формат raw;
      qemu-img — на стороне потребителя. Тесты: `admin_exports_test.go`
      (ExportEndpoints, ExportReadOnlyPersisted, ImportJobAsync).
- [ ] **Авто-детект параметров тома клиентом** (block size, immediate_commit,
      гранулярность битмапы) из meta, а не из конфига клиента — у них это
      убрало целый класс рассинхронов; плюс `skip_corrupted_meta_entries` и
      `gc_on_start` как режимы выживания вместо падения OSD

### Изоляция томов (блокер мульти-тенанта) — СДЕЛАНО 2026-11-08

- [x] **Приватный диапазон object-id на том** (`meta.VolumeSpec.OIDBase` +
      кластерный аллокатор `meta.NextOIDBase` на CAS-эволюции `alloc/oids`,
      `Volume.oid(index) = OIDBase+index+1`). До этого `oid = index+1`, OSD
      принимал имя экспорта и **игнорировал** его, а клиент звонил всем OSD с
      export `"vol"`: два тома на общих OSD адресовали одни и те же объекты —
      запись во «вторую ВМ» на смещении 0 затирала «первую». Это делало
      невозможным любой полигон «несколько чужих ВМ на одном кластере» и
      блокировало снапшоты/клоны. Теперь поколение снапшота — это арифметика
      над бейзом (`OIDBase + gen*NumObjects + index + 1`), клон — копия меты
      со своим бейзом: wire-протокол менять не пришлось.
      Регресс: `pkg/client/twovolume_test.go` (два тома на трёх общих OSD,
      одинаковые смещения, тонкие области, trim одного не дырявит другой).
- [x] **`meta.ListVolumes` / `meta.DeleteVolume` + `Cluster.ListVolumes` /
      `Cluster.DeleteVolume`** — без них контрол не может ни показать тома, ни
      удалить диск ВМ. Удаление снимает регистрацию; объекты остаётся
      чистить проходом вакуума (ниже).
- [x] **Вакуум (vacuum) объектов удалённого тома** — реализовано: тумбстон
      `meta.VacuumRecord` + проход `Cluster.VacuumDeletedVolumes` по OSD
      последовательно (по одному OSD, медленный trickle — ниже клиентского
      I/O никогда); триггеры `unistorctl vacuum [ls|run]`, HTTP
      `GET/POST /v1/vacuum`, автовакуум `unistord -vacuum-interval N`.
      Пер-томный режим: `unistorctl vacuum run <vol>` / `POST
      /v1/vacuum/{name}` — тумбстон удалённого тома либо live-vacuum
      живого тома (punch полностью нулевых объектов по всей длине,
      предикат — физическая аллокация через `OpStatInfo`, протокол v3).
      Детали: `test-vm-plan.md` M1.4.
- [x] **Квота тома + честный ENOSPC**: запись за квотой → отдельный
      `StatusNoSpace` в `sdsnet`, ENOSPC от реплики не считается гибелью OSD
      (не в `noteFailure`), полный журнал → форс-флеш/EAGAIN вместо падения;
      тест «диск полон: том A получает IO-error, том B работает, OSD жив».
      Детали: `test-vm-plan.md` §2 (M0.3, M0.4)
- [x] **QoS per-volume (IOPS/BPS)** — без него одна ВМ с fio в госте сажает
      латентность всем; механика уже есть (`Pool.Limit` как ограничитель
      конкурентности на цель). Частично пересекается с «квоты, QoS» Фазы 6

## Фаза 6 — Функциональность уровня Ceph-фич (после 1–5)

> Статус (2026-11-09): сделано: снапшоты/клоны, thin+UNMAP, QoS/квоты, вакуум. Открыто: EC (RS), бекапы (расписание/ротация поверх export/import), AES-256-XTS, balancer.

- [x] снапшоты (copy-on-write по OID-поколениям) + откат — реализованы
      по ADR `todos/adr-snapshots.md`: схема **«forward/redo»**
      (fresh-OID-on-snapshot + чтение по цепочке поколений, без CoW-refcount;
      ADR зафиксирован до кода).
      Грабли из их багов закрыты тестами: generation — uint32 + монотонный
      LastGen (>255 и откаты безопасны, `TestSnapshotLimitAnd300Gens`,
      `TestSnapshotRollback`); создание/удаление снапшотов — атомарные
      CAS-транзакции meta (параллельные не рвут цепочку,
      `TestSnapshotParallelCreateSerializes`, `TestSnapshotDelete`);
      глубина цепочки ограничена 64 (ERR_TOO_MANY_SNAPSHOTS); метаданные
      снапшота (битмап/allocmap) — источник истины, данные не копируются
      (`TestSnapshotChainRead`, TRIM материализуется —
      `TestSnapshotTrimMaterialization`, регрессия punch `
      TestSnapshotFreeTrimPunchRegression`); rename нет вовсе — закоммиченный
      в консенсусе снапшот всегда указывает на уже записанные объекты,
      «сироты» незакоммиченных диапазонов не видны и собираются
      `VolumeGarbage`.
- [x] **клон volume** — копия меты поверх снапшот-цепочки источника
      (`pkg/meta/clones.go`): свой свежий live-диапазон, цепочка
      копируется, расшаренные поколения считаются счётчиками `genrefs:`
      (префикс меты); vacuum/удаление/откат обходят диапазон с
      ненулевым счётчиком. Протокол «Share до CAS клона, Drop после
      CAS удаления» даёт при крахе только утечки, никогда —
      уничтожение расшаренных данных. CLI `vol clone`, HTTP
      `POST /v1/volumes/{name}/clone`. Тесты: `pkg/meta/clones_test.go`,
      `pkg/client/clones_test.go` (E2E с CoW, удалением источника,
      vacuum), `admin_clone_test.go`.
      Детали: `todos/vitastor-ideas.md` §7.1, механика OID-поколений и
      очередь граблей: `test-vm-plan.md` §5 (бейзы томов уже готовы —
      `VolumeSpec.OIDBase`).
- [x] thin provisioning + UNMAP/discard — реальный hole-punch в `pkg/store`
      (fallocate PUNCH_HOLE, Darwin — `F_PUNCHHOLE`/`OpStatInfo` v3),
      разреженные нули сквозь OSD и `sdsnet`, TRIM из `sds-nbd`/VDUSE;
      live-vacuum нулевых объектов — поверх того же механизма.
- [ ] erasure coding (RS) для холодных классов. Их уроки — до написания кода:
      очередь PG с активной записью + pending commit/rollback предыдущей
      (у них это вешало PG в STOPPING/REPEERING), **резерв места в журнале под
      commit**, версия ISA-L запинена + golden-тесты на векторах (2.31 = порча
      данных). Детали: `todos/vitastor-ideas.md` §7.3
- [x] квоты, QoS (IOPS/BPS-шейпинг per-volume) — квота с честным ENOSPC и
      live-обновлением (`vol qos`), шейпинг гостя + админский класс без
      ограничений; менять можно на включённом томе.
- [ ] бекапы/экспорт (qemu-img convert-совместимый raw-стрим): поверх
      `GET /v1/volumes/{name}/export` (Фаза 5b) — расписание, хранение,
      ротация, сверка контрольной суммы, восстановление в новый том через
      import; в бэкапе полезен режим разреженности (дыры → нули).
      Детали: `test-vm-plan.md` §6
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
6. **Честность сравнения с Vitastor (решение 2026-11-08, из разбора их
   ченджлогов).** Их rep3-цифры достигнуты только с `client_enable_writeback`
   (без него — 3.4K IOPS QD32), то есть подтверждение клиенту до fsync. Наша
   hard-семантика (ждём кворум) с ними не сопоставима напрямую. Решение:
   продуктовый режим один — hard; writeback существует только как опция
   бенча (`sdsbench --writeback`), и в сравнительных отчётах каждый режим
   пишется отдельной строкой с явной пометкой семантики. «Мягкий» режим как
   продуктовый по умолчанию — только по явному решению владельца.
7. **Экспорт (решение 2026-11-08).** Порядок: довести до конца Linux/KVM e2e
   на VDUSE (код написан), затем оценивать ublk (у них в CSI ublk — дефолт,
   VDUSE дал 3–4× и переживает обновление пода). Не начинать ublk, пока
   VDUSE-e2e не закрыт.
8. **Шифрование транспорта (решение 2026-11-08, если не решит владелец).**
   Дефолт как у них: AES-GCM только заголовки, данные открыты (цена AES-XTS
   +GCM ≈ 12.6 GB/s на ядро — см. `cpubench`); полносвязное шифрование —
   opt-in. Требование безопасности UniStack имеет приоритет: если нужен
   «всё» — включаем и принимаем цену по замеру, а не на глаз.
9. **Метамодель на 2 нодах.** DirStore на локальном диске не переживает
   потерю ноды; NFS = общая точка отказа без fencing. Любой честный вариант
   (meta-сервис в `sdsd` с пассивной копией, embedded raft, etcd на 3
   узлах) требует третью машину или внешний KV. Это решение владельца, и
   оно определяет, что мы обещаем про «кластер из двух нод» (см. Фазу 4
   и `test-vm-plan.md` §8.2, §10).
10. **RF3 по узлам на двух машинах невозможен.** Дефолт полигона —
    `replicas=2, min_size=1` с failure domain = host: потеря ноды =
    чтение с одной копии, запись — только после возврата ноды или
    появления третьей. Это надо проговаривать арендаторам полигона прямым
    текстом, а не подразумевать: «выживаемость ноды = чтение, не запись».
11. **Мульти-тенант без изоляции = взаимное затирание данных.**
    Приватный OID-диапазон на том сделан (Фаза 5b,
    `pkg/client/twovolume_test.go`), но без квоты/ENOSPC, QoS и
    ограничения входов в HTTP API один гость способен залить диск или
    выжечь IOPS всем. Чужие образы — только из нашего дистрибутива,
    с проверкой контрольной суммы на входе (см. `test-vm-plan.md` §7).
