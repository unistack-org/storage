# unistack storage

Распределённая блочная СХД на чистом Go. Цели (зафиксированы владельцем):

1. **CRUSH-крашмапы как в Ceph** — размещение объектов по topology-карте
   (region → zone → rack → host → disk). Движок и формат карты: `crush/`
   (byte-exact порт C-ядра Ceph из репозитория
   [crushkv](https://github.com/unistack-org/crushkv), влит в этот
   репозиторий; golden-тесты против настоящего Ceph, 0 аллокаций на
   горячем пути, см. `docs/GC_ARENA.md`, лицензия — `LICENSE-crushkv`).
2. **Только Go, скорость не ниже [Vitastor](https://vitastor.io/docs/)**
   (~0.14 мс на 4K, миллионы IOPS на узел). Стратегия: минимум
   копирований/аллокаций, no-alloc data path, io_uring (Linux), свой
   бинарный протокол без gRPC/protobuf на data path.
3. **Диск внутри ВМ виден как NVMe-устройство.**

## Статус

- ✅ **MVP экспорта (фаза 4a)**: `pkg/nbd/` — NBD-сервер на stdlib Go +
  демон `cmd/unistord`. Проверено end-to-end: QEMU 11 отдаёт экспорт
  как `/dev/nvme0n1` гостю, mkfs/mount/запись работают, данные в
  backing-файле.
- ✅ **Экспорт-устройства**: `unistord` отдаёт том по NBD (TCP),
  VDUSE (`/dev/vduseX`, Linux) и vhost-user-blk (UNIX-сокет для
  QEMU/Dragonball); управление через REST `POST/GET/DELETE /v1/exports`
  и `unistorctl export …`.
- ✅ CRUSH: `crush/` (движок + формат карты Ceph: чтение/запись text,
  JSON, binary + арена без GC) — влит в этот репозиторий из crushkv.
- ✅ Метрики: `unistord -metrics 0.0.0.0:8080` → `GET /metrics` в формате
  Prometheus (скрейп VictoriaMetrics): состояние кластера, raft (состояние
  и участники), статус и диски всех OSD (в т.ч. удалённых — зонд StatFS),
  латентность/IOPS/BW, состояние PG и ёмкость томов, счётчики recovery.
  Единственный путь — `go.unistack.org/micro-meter-victoriametrics/v5`
  (реестр официальной библиотеки VictoriaMetrics), без prometheus-клиента.
  Экспорт только на отдельном `-metrics`-порту, REST API метрики не отдаёт.
  Детали: [`todos/runbook.md`](todos/runbook.md).
- ✅ Разделение сетей: у OSD второй слушатель для контроля
  (`-osd id@dir@data@ctrl`) — пинги/StatFS/health-пробы идут по нему,
  поэтому забитая дата-сеть не эвиктит живые OSD и не разваливает
  кластер. Пустой ctrl-адрес = одна сеть (обратная совместимость).
- 🚧 Скелет репозитория: интерфейс-реестры `backend/`, `cache/`,
  `cluster/`, `hash/`, `journal/`, `transport/` (legacy-слой sheepdog-era
  удалён в фазе 0).
- 📋 Полный план: [`todos/roadmap.md`](todos/roadmap.md),
  аудит: [`todos/AUDIT.md`](todos/AUDIT.md).

## Модули

Единый Go-модуль `github.com/unistack-org/storage` (корень репозитория):

| Пакет / команда | Назначение |
|---|---|
| `cmd/unistord` | демон SDS: тома, кластер, REST admin API, экспорты (NBD/VDUSE/vhost-user-blk) |
| `cmd/unistorctl` | CLI: тома, кластер, `export create/ls/rm` |
| `cmd/sdsbench` | бенчмарк data path |
| `pkg/nbd` | NBD-сервер (stdlib, zero-alloc data path) |
| `pkg/vduse` | VDUSE (Linux) — userspace virtio-blk бэкенд |
| `pkg/vub` | vhost-user-blk бэкенд (UNIX-сокет, QEMU/Dragonball) |
| `pkg/store`, `pkg/object`, `pkg/volume`, … | ядро SDS |
| `crush/` | CRUSH-движок и крашмапы (порт C-ядра Ceph, ex-crushkv) |

## Сборка / тесты

```sh
GOWORK=off go build ./... && GOWORK=off go test -race ./...
```

`GOWORK=off` обязателен, если рядом лежит внешний `go.work`.

Быстрый старт:

```sh
GOWORK=off go build -o unistord ./cmd/unistord
./unistord -listen 127.0.0.1:10809 -export vol0=/tmp/vol0.img:1G
qemu-img info nbd://127.0.0.1:10809/vol0
```

