# ROADMAP: unistack storage — распределённая блочная SDS на Go

Цели (зафиксированы владельцем):
1. **CRUSH-крашмапы как в Ceph** — размещение object/реплик по topology-map
   (region → zone → rack → host → disk), детерминированное, с reweight/
   migration при изменении кластера.
2. **Только Go, скорость не ниже Vitastor** (vitastor.io: ~0.14 мс на 4K,
   миллионы IOPS на узел; выводы из их докладов: CPU — узкое место →
   минимум копирований/аллокаций/сериализаций, io_uring, zero-copy сеть).
3. **Конечная цель: диск внутри ВМ виден как NVMe-устройство**
   (цепочка vhost-user-blk → qemu-storage-daemon → vfio-user зафиксирована
   в `unistack/todos/storage-nvme-vfio-user.md`; путь без патчей QEMU).

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
- [ ] No-alloc data path: пулы буферов (arena/mmap), никаких `make()` на I/O;
      бенчмарк-гейт «0 allocs/op» в CI
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

- [ ] `pkg/net`: свой бинарный протокол (фикс. заголовок 16–32 B: OID, offset,
      len, epoch, flags; без grpc/protobuf на data path)
- [ ] Клиентские пулы соединений, pipeline/batch запросов, `MSG_ZEROCOPY`
      (отправка), приём через io_uring (`SO_ZEROCOPY` позже)
- [ ] Репликация: N-копий, запись у всех реплик (quorum-запись, read-any),
      версии объекта (epoch+generation), flush/fsync-семантика для FUA
- [ ] Отказы: heartbeat, таймауты, деградация при потере реплики;
      recovery-трафик отдельным приоритетом
- [ ] Тесты: chaos (обрывы, задержки, partial write), `go test -race`

## Фаза 3 — Кластер, метаданные, CRUSH (4–6 недель) — «как Ceph»

- [ ] `pkg/mds`: метаданные кластера поверх **etcd** (external; embedded
      `cluster/etcdint` — опция для all-in-one): список OSD, epoch карты,
      inventory объектов (OID → OSDs), VDI/volume-таблица
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
  - [ ] Интеграция в storage: `pkg/crush` — тонкая обёртка: карта в etcd
        (epoch-ами), кэш `pg/объект → OSD`, отправка карты клиентам.
  - [ ] Reweight-валидатор, `rule`-валидатор под наши volume-class (SSD/HDD),
        эмуляция отказов (chi-square равномерность на больших x) — поверх crushkv.
- [ ] `pkg/cluster`: вступление узла, epoch-карты, отправка карты клиентам
      (клиент считает placement сам и шлёт данные напрямую OSD — без прокси,
      как Vitastor/Ceph)
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
- [ ] **4b Целевой: Go vhost-user-blk server (`pkg/vub`)** — unix-сокет,
      handshake (GET/SET_FEATURES, SET_MEM_TABLE, SET_VRING_*), mmap гостевой
      памяти (hugepages), разбор desc/avail/used rings, multi-queue; QSD как
      мост `--export type=vfio-user` → QEMU `-device vfio-user-pci`
      (в госте virtio-blk) + вариант NVMe через QSD NBD-экспорт + QEMU NVMe
- [ ] Интеграция с `unistack/pkg/storage` (Provider-драйвер `unistorage`:
      CreateVolume/DeleteVolume/Attach) и `pkg/qemu` (`Drive.VfioUser`)
- [ ] Отказоустойчивость экспорта: смерть Go-бэкенда → QSD reconnect,
      без висячих сокетов/hugepages
- [ ] e2e-тест: Linux+KVM, гость пишет/читает, fio внутри гостя

## Фаза 5 — Производительность до уровня Vitastor (постоянно, с замерами на каждую фазу)

- [ ] Бенч-стенд: 3–5 нод, NVMe, 25/40GbE; профили из `tests/fio.txt` +
      randread/randwrite 4K/64K/1M, depth 4/16/128
- [ ] Цели (ориентир Vitastor highload-доклады): 4K randread **> 100k IOPS/ноду**
      на 3-реплики по TCP, **> 200k** с MSG_ZEROCOPY; латентность single
      4K read < 0.2–0.3 мс (их 0.14 мс — с RDMA; зафиксировать: RDMA — stretch)
- [ ] Оптимизации по результатам perf: pinned per-CPU workers (SO_REUSEPORT /
      EPOLL_TAG), registered io_uring fds+bufers, отключение GC-давления
      (GOGC off на data-потоках / ballast), hugepages для буферов,
      NUMA-affinity, опционально kernel-bypass (netmap/eBPF) — только если
      perf покажет сеть узким местом
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
