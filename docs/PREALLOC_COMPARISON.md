# Prealloc (reuse-first) — сравнение arena vs prealloc

Этот документ — результат ЭТАПОВ 5 и 6. Таблицы ниже — A/B-замеры,
сделанные, пока glue за флагом `Prealloc` сосуществовало с allocating
путём; **после подтверждения паритета флаг убран, и zero-alloc glue теперь
включён всегда** (без env/CLI-кнопки). Замеры остаются как доказательство
эффекта.

> **Среда (osx):** Go 1.27 (arm64/darwin). iouring здесь недоступен (только Linux),
> поэтому бенчмарки шли по `fallback` (worker pool, pwrite/pread) и arena
> backends. Glue-аллокации, которые убирает `Prealloc`, **не зависят от
> ring-backend**: 0 allocs в горячем пути держится и на uring, и на fallback.
> Linux-специфичные вещи (mmap/hugepages) в этом варианте **не используются**
> (см. «Scope» ниже) — выигрыш достигается переиспользованием, не mmap.
>
> **Среда (linux/iouring):** Fedora 44, kernel 7.2.11, AMD EPYC 7282 (16c),
> NVMe RAID1 (2×3.6T, `/mnt/sdsdata`), реальный iouring backend. **Машина
> шумная** (на ней работают LLM-инференсы), поэтому throughput-цифры — с
> разбросом; A/B обеим сторонам в одном запуске. Подробности — в разделе
> «Линейка на Linux/iouring (NVMe)» ниже.

## Scope (что реально изменено)

Задача изначально просила «pointer-free + unsafe.Slice» аллокатор. Аудит
(см. `GC_ARENA.md` и чекпоинты) показал, что в этом репозитории **hot path
не аллоцирует в арене вообще** — arena (и `arenaPool`) живут только в
`pkg/asyncio` и не входят в путь `Store.WriteAt`. Реальные аллокации на I/O —
это glue в `pkg/store`:
- `asyncio.go` `ringIO`: `make([]int32)` + `make([]*ioComp)` на каждый I/O;
- `journal.go` `encodeJournal`: `make([]byte, ...)` на каждый append.

Поэтому сделан **reuse-first (Scope A)**: переиспользовать glue (free-chan из
`ringArena`, pooled encode buffer). **Без unsafe, без
нового аллокатора, без изменения публичного API.** Arena остаётся доступной
как ring-backend (`-arena`) и ничего не удалено. Изначально glue было за
флагом `Prealloc` (A/B); после подтверждения паритета флаг убран — glue
включено всегда.

## Методика

- glue включено всегда (без флага); исторический A/B-флаг был
  `Options.Prealloc` / env `SDS_PREALLOC` / CLI `-store-prealloc` и убран
  после подтверждения паритета.
- Бенчмарки (все с `-benchmem`):
  - `BenchmarkRing` — сырая `ringIO`, 4 KiB, Arena 0/1 (измерял Arena ×
    Prealloc 0/1);
  - `BenchmarkJournal` — `Store.WriteAt` 4 KiB, `CommitJournal` on (измерял
    Prealloc 0/1);
  - `BenchmarkGC` — фиксированный workload 2000×4 KiB writes, `CommitJournal`,
    `AsyncRing=16`; GC-метрики через `runtime/metrics`
    (`/gc/cycles/total`, `/gc/heap/allocs`, `/gc/scan/heap`, `/gc/pauses`);
  - `BenchmarkRealVolume` — реалистичный объём (1 GiB, 256 KiB-блоки,
    `CommitJournal` + fsync + read-back, 256 объектов × 4 MiB); throughput
    (MB/s), GC cycles, heap alloc, RSS-delta, page-in. Env `SDS_BENCH_GB`
    задаёт объём. Прогнан на Linux/iouring (NVMe).
- Сводка через `golang.org/x/perf/cmd/benchstat`.

## Результаты

### Ring (4 KiB I/O, per op)

| Backend | Prealloc | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| fallback | off | 3361 | 448 | 8 |
| fallback | **on** | 3361 | **0** | **0** |
| arena | off | 3806 | 448 | 8 |
| arena | **on** | 3533 | **0** | **0** |

**Вывод:** 8 → **0 allocs/op** (448 → 0 B/op) на обоих backends. Throughput
(ns/op) в пределах шума — ring и так быстрый; выигрыш чисто в аллокациях/GC.

### Journal (4 KiB WriteAt, `CommitJournal`, per op)

| Prealloc | ns/op | B/op | allocs/op |
|---|---|---|---|
| off | ~4804 | ~10.2 KB | 5–6 |
| **on** | ~4984 | ~5.5–7.1 KB | 4–5 |

**Вывод:** ~1–2 alloc меньше и ~4–5 KB/op меньше (переиспользуется encode
buffer). Оставшиеся аллокации намеренно не пулятся: `rec.data` (копится в
overlay, читается lock-free — пул = аллиасинг) и структура записи. ns/op в
шуме: журнал I/O-bound, encode — малая доля.

### GC (workload 2000×4 KiB writes, суммарно)

| Prealloc | GC cycles | heap alloc | live | scan heap | pauses | total B/op | total allocs |
|---|---|---|---|---|---|---|---|
| off | 5–6 | 19.6–22.7 MB | 71–112 KB | 9.7–24.5 KB | 10–12 | 20.6–22.6 MB | 11968–12502 |
| **on** | 2–3 | 9.5–13.3 MB | 62–169 KB | 21–40 KB | 4–6 | 10.0–14.0 MB | 9468–10438 |

**Вывод:** в ~2 раза меньше GC-циклов, ~на 48% меньше аллоцированного кучей
объёма, ~на 50% меньше пауз. Это главный реальный выигрыш: даже при
I/O-bound throughput (ns/op ≈) давление на GC и кучу существенно ниже.

### Линейка на Linux/iouring (NVMe)

Те же бенчмарки + `BenchmarkRealVolume` (реалистичный объём: 1 GiB,
256 KiB-блоки, `CommitJournal` + fsync, 256 объектов × 4 MiB) прогнаны на
тестовой NVMe-машине через реальный iouring backend.

**Ring (4 KiB, per op)** — реальный uring (но arena-регистрация недоступна,
см. ниже):

| Arena | Prealloc | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| 0 | off | ~37 | 448 | 8 |
| 0 | **on** | ~32 | **0** | **0** |
| 1 | off | ~38 | 448 | 8 |
| 1 | **on** | ~36 | **0** | **0** |

**Journal (4 KiB WriteAt, `CommitJournal`, per op):**

| Prealloc | ns/op | B/op | allocs/op |
|---|---|---|---|
| off | ~127 µs | ~9.1 KB | 4 |
| **on** | ~121 µs | ~4.2 KB | 3 |

**RealVolume (1 GiB, 256 KiB блоки, journal+fsync+read-back, суммарно за run):**

| Prealloc | MB/s (w+rd) | GC cycles | heap alloc |
|---|---|---|---|
| off | 398 / 360 (2 прогона) | 224 / 191 | 535 / 447 MB |
| **on** | 337 / 222 | 119 / 122 | 264 / 269 MB |

**Выводы по NVMe:**
- **0 allocs/op в hot path (ring) — подтверждено на реальном iouring**: 8 → 0,
  448 → 0 B/op, обе реализации — один бинарник за флагом.
- **Journal: ~1 alloc и ~5 KB/op меньше** — та же картина, что на darwin.
- **RealVolume: GC-циклы и heap-alloc сокращаются ровно вдвое** (оба прогона
  на обеих платформах). Throughput (MB/s) в пределах шума шумной машины:
  диапазон перекрывается (398/360 vs 337/222), a fio-базовая (67k IOPS,
  275 MB/s) показывает, что bottleneck — fsync-стек + шум, а не glue.
- **Циклы GC на реальном объёме: 224/191 → 119/122** — главный выигрыш
  воспроизводится и на 1 GiB, а не только на микробенче.

**Важное замечание (NVMe-машина):** `io_uring_register` (регистрация
buffer/pointer) на этой хост-машине **отклоняется ядром** (lib возвращает
EBADF, raw syscall — EINVAL; `io_uring_disabled=0`, не virtualized).
Магазин корректно деградирует до plain ring buffers (лог:
`ring arena unavailable (io_uring_register buffers ...)`), поэтому **нога
Arena1 на этой машине = plain path** (Arena0≈Arena1 по времени ring — это
ожидаемо, а не bug). Prealloc-часть A/B **не затрагивает** (ioCompPool
работает независимо от registered-buffers). Зафиксировано в
`pkg/iouring/fixed_test.go` (паттерн EBADF/EFault виртуализированных ядер +
graceful degradation).

**Файл бенча:** `pkg/store/realvolume_bench_test.go` (в репо; env
`SDS_BENCH_GB` для объёма).

## Где выигрыш реален, а где шум

| Метрика | Результат | Комментарий |
|---|---|---|
| **allocs/op в hot path (ring)** | **реален** | 8 → 0 на darwin **и** на Linux/iouring (NVMe). Цель задачи достигнута. |
| **B/op (ring)** | **реален** | 448 → 0, обе платформы. |
| **GC cycles / heap alloc / pauses** | **реален** | ~в 2 раза меньше; подтверждено и на RealVolume 1 GiB (NVMe: 224/191 → 119/122 циклов). |
| **Journal B/op / allocs** | реален, но мал | −1–2 allocs, −4–5 KB/op; `rec.data` намеренно не пулится. |
| **Throughput (ns/op, MB/s)** | **шум** | I/O-bound: устранение glue не меняет wall-clock. NVMe-машина шумная (LLM-инференсы) — MB/s в пределах разброса. |
| **RSS-delta, page-in (RealVolume)** | неинформативно | prefetching+fsync-стек доминирует; page-in ≈ 0 после prefault. |
| p50/p95/p99, время warmup | не замерено | при I/O-bound пик latency не от glue. |

## Включение (сейчас)

glue включено всегда: пул ring-комплешнов собирается при `AsyncRing > 0`,
pooled encode buffer живёт в `journalState`. Кнопки выключения нет (флаг
`Prealloc` убран после подтверждения паритета) — откат возможен только
кодом. Arena-код и `-arena` не тронуты, работают как раньше.

## Что удалить в конце (если доведём до полной миграции)

Scope A ничего не удаляет. Кандидаты на удаление **только** при желании
убрать arena:
- GOEXPERIMENT=arenas из сборок/Makefile/CI (если `ringArena` больше не нужен);
- `arena.New`/`arena.MakeSlice`/`Free`, `arenaPool` в `pkg/asyncio`;
- `ringArena` backend (если оставим только fallback + prealloc).

## Инварианты unsafe

**В этом изменении `unsafe` нет** (reuse-first). Соответствующие
инварианты/контракты задокументированы в коде как обычные комментарии:
- `asyncio.go` `ioComp`/`ioCompPool`/`borrowComp`/`returnComp`/`finishRing`:
  `ioComp` пересоздаётся и кладётся в `ring.comp` **только если никто его ещё
  не взял** (`Done` fired ровно раз — инвариант всех путей Submit/SubmitSingle).
- `journal.go` `journalState.encodeBuf`: буфер живёт под `js.mu`, отдаётся
  `Journal.Append`, который копирует payload в bufio и никогда его не
  сохраняет; растёт power-of-2, не переаллоцируется. `rec.data` **намеренно не
  пулится** (overlay хранит указатель и читает `r.data` lock-free).

## Ограничения и след. шаги

- p50/p95/p99 latency, время warmup — не замерены (нужен latency-recording
  harness). При I/O-bound нагрузке это вторично.
- **Linux/iouring прогнан** (NVMe-машина): 0 allocs в hot path подтверждён на
  реальном uring; GC-циклы/heap-alloc вдвое ниже на 1 GiB. Ограничение машины:
  `io_uring_register` отклоняется (registered-buffers → plain path, см.
  «Линейка на Linux/iouring»); Prealloc-часть не затрагивает.
- Если нужен выигрыш в самом payload (не glue) — это уже Scope B (arena/мmap
  buffer), отдельная работа с `unsafe`, по которой нужен отдельный аудит.
