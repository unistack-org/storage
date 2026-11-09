# unistack storage

Распределённая блочная СХД на чистом Go. Цели (зафиксированы владельцем):

1. **CRUSH-крашмапы как в Ceph** — размещение объектов по topology-карте
   (region → zone → rack → host → disk). Движок:
   [crushkv](https://github.com/unistack-org/crushkv) — byte-exact порт
   C-ядра Ceph, golden-тесты против настоящего Ceph, 0 аллокаций на
   горячем пути (см. `crushkv/docs/GC_ARENA.md`).
2. **Только Go, скорость не ниже [Vitastor](https://vitastor.io/docs/)**
   (~0.14 мс на 4K, миллионы IOPS на узел). Стратегия: минимум
   копирований/аллокаций, no-alloc data path, io_uring (Linux), свой
   бинарный протокол без gRPC/protobuf на data path.
3. **Диск внутри ВМ виден как NVMe-устройство.**

## Статус

- ✅ **MVP экспорта (фаза 4a)**: [`sds-nbd/`](sds-nbd/) — NBD-сервер на
  stdlib Go + демон `sdsd`. Проверено end-to-end: QEMU 11 отдаёт экспорт
  как `/dev/nvme0n1` гостю, mkfs/mount/запись работают, данные в
  backing-файле. Детали: [`sds-nbd/README.md`](sds-nbd/README.md).
- ✅ CRUSH: готов в отдельном репозитории crushkv (движок + формат карты
  Ceph + арена без GC).
- ✅ Метрики: `unistord -metrics 0.0.0.0:8080` → `GET /metrics` в формате
  Prometheus (скрейп VictoriaMetrics): состояние кластера, raft (состояние
  и участники), статус и диски всех OSD (в т.ч. удалённых — зонд StatFS),
  латентность/IOPS/BW, состояние PG и ёмкость томов, счётчики recovery.
  Экспорт только на отдельном `-metrics`-порту, REST API метрики не отдаёт.
  Детали: [`todos/runbook.md`](todos/runbook.md).
- 🚧 Скелет репозитория: интерфейс-реестры `backend/`, `cache/`,
  `cluster/`, `hash/`, `journal/`, `transport/` (legacy-слой sheepdog-era
  удалён в фазе 0).
- 📋 Полный план: [`todos/roadmap.md`](todos/roadmap.md),
  аудит: [`todos/AUDIT.md`](todos/AUDIT.md).

## Модули

| Модуль | Путь | Назначение |
|---|---|---|
| `github.com/unistack-org/storage` | `.` | ядро SDS (в разработке) |
| `github.com/unistack-org/sds-nbd` | `sds-nbd/` | NBD-сервер + `sdsd` (MVP экспорта) |

## Сборка / тесты

```sh
GOWORK=off go build ./... && GOWORK=off go test -race ./...
cd sds-nbd && GOWORK=off go build ./... && GOWORK=off go test ./...
```

`GOWORK=off` обязателен, если рядом лежит внешний `go.work`.

Быстрый старт экспорта: см. [`sds-nbd/README.md`](sds-nbd/README.md).

