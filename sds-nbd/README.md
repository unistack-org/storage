# sds-nbd — NBD-сервер на Go (MVP экспорта «диск внутри ВМ»)

Модуль `github.com/unistack-org/sds-nbd`: сервер протокола
[NBD (Network Block Device)](https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md)
на чистом stdlib Go (без внешних зависимостей) + демон `unistord`.

Назначение (фаза 4a роадмапа): отдать блок-устройство клиенту QEMU так,
чтобы внутри виртуалки оно появилось как **`/dev/nvme0n1`**:

```
гость (Linux) ── /dev/nvme0n1 ──> QEMU NVMe-эмуляция ──> blockdev nbd ── TCP ──> unistord (Go) ──> файл/устройство на хосте
```

## Быстрый старт

```sh
cd storage/sds-nbd
GOWORK=off go build -o unistord ./cmd/unistord

# экспорт vol0 = файл 1 GiB (разрежённый)
./unistord -listen 127.0.0.1:10809 -export vol0=/var/lib/unistord/vol0.img:1G

# на macOS TRIM/WRITE_ZEROES работают (fcntl F_PUNCHHOLE на APFS);
# отключаются флагами, если нужно:
./unistord -listen 127.0.0.1:10809 -export vol0=/tmp/vol0.img:1G \
       -trim=false -write-zeroes=false
```

Флаги демона: `-listen`, `-export name=path[:size]` (повторяемый),
`-data-dir`, `-trim`, `-write-zeroes`, `-max-conns`.

## Проверка утилитами QEMU

```sh
qemu-img info nbd://127.0.0.1:10809/vol0          # размер, формат raw
qemu-img convert -f raw -O raw disk.img nbd://127.0.0.1:10809/vol0   # запись
qemu-img convert nbd://127.0.0.1:10809/vol0 /tmp/readback.bin        # чтение
```

## Виртуалка с NVMe-диском (проверено на macOS arm64, QEMU 11.1)

```sh
qemu-system-aarch64 -machine virt -accel hvf -cpu host -m 2048 \
  -drive if=pflash,format=raw,unit=0,file=$HOME/share/qemu/edk2-aarch64-code.fd,readonly=on \
  -cdrom alpine-standard-3.22.1-aarch64.iso \
  -display none -serial stdio \
  -drive file.driver=nbd,file.host=127.0.0.1,file.port=10809,file.export=vol0,format=raw,if=none,id=n0 \
  -device nvme,drive=n0,serial=vol00001 \
  -netdev user,id=net0 -device virtio-net-pci,netdev=net0
```

В госте (Alpine standard ISO):

```sh
apk add --no-cache e2fsprogs
mkfs.ext4 -F -q /dev/nvme0n1
mount -t ext4 /dev/nvme0n1 /mnt
echo secret > /mnt/secret.txt && sync && umount /mnt
```

## Кластерный режим: admin API, runtime-экспорты, VDUSE

Полноценный режим SDS — `unistord` как узел кластера `storage/`
(см. `todos/roadmap.md` фазы 3–5 и ADR-059 в репозитории unistack):

```sh
./unistord -cluster /var/lib/unistord/meta \
  -osd 1@/var/lib/unistord/osd1 -crushmap crush.json -epoch 1 \
  -http 10.0.0.10:8443 -admin-token "$ADMIN_TOKEN" \
  -metrics 0.0.0.0:8080
```

Административный HTTP API (`-http`, bearer-токен `-admin-token`,
`/healthz` без токена) — контракт контрольной плоскости unistack
(«клетки»):

- `POST/GET/DELETE /v1/volumes…` — тома (`size` — строка в байтах),
  `read_only`/`shared`/`owner` в записи тома (`meta.VolumeSpec`);
  `PUT /v1/volumes/{name}/resize` — только рост (уменьшение → 400 «shrink»);
- `POST /v1/volumes/{name}/import {"url","sha256"?,"offset"?,"async"}` →
  `202 {"job_id"}`; `GET /v1/jobs/{id}` → `state|written|error`.
  Ingest ограничивается QoS-классом тома; sha256 проверяется по записанным
  байтам;
- снапшоты/rollback: `POST /v1/volumes/{name}/snapshot`,
  `GET …/snapshots`, `DELETE …/snapshot/{gen}`, `POST …/rollback`;
- **runtime-экспорты**: `POST /v1/exports
  {"volume","type":"nbd"|"vduse","devname"?,"read_only"?}` → 201/200
  (идемпотентно), `DELETE /v1/exports/{name}?type=…` (409 — устройство
  занято, 503 — нет кворума). `read_only` персистится в записи тома и
  воспроизводится при рестарте;
- VDUSE (Linux): том становится устройством virtio-blk через
  `vdpa dev add name <devname> mgmtdev vduse`; QEMU подключает его
  `-device vhost-vdpa-device-pci,vhostdev=/dev/vhost-vdpa-<minor>`
  (vIOMMU в госте обязателен; read-only — бит `VIRTIO_BLK_F_RO`,
  запись в data-path отклоняется). детали — `deploy/README.md`.

Пример контракта клетки: `POST /v1/exports {"volume":"iso-42","type":"vduse",
"devname":"sdsv-42","read_only":true}` → `vdpa dev add` на узле →
QEMU-диск с ISO.

## Что проверено (MVP-критерии)

- `go test ./... -count=1` и `-race` — все тесты зелёные (7 тестов, 1 skip —
  TRIM/WRITE_ZEROES, Linux-gated).
- `qemu-img info` — рукопожатие newstyle+fixed-newstyle, LIST, GO,
  INFO (EXPORT/BLOCK_SIZE), structured replies.
- Двусторонний `qemu-img convert` 8 MiB случайных данных: md5 бэкинга и
  обратного чтения совпадают.
- Реальный гость: Alpine aarch64 стартует, `nvme nvme0: pci function ...`,
  `/dev/nvme0n1` 1 GiB; mkfs.ext4 + mount + запись файла; magic `53 EF`
  (ext4 superblock) и содержимое файла видны в backing-файле на хосте.

## Реализация протокола

- Handshake: **18 байт** — u64 `NBDMAGIC` + u64 `IHAVEOPT` + u16 флагов
  (FIXED_NEWSTYLE, NO_ZEROES). Старый 10-байтовый hello QEMU 11 не читает.
- Опции: EXPORT_NAME (legacy), ABORT, LIST, INFO, GO, STRUCTURED_REPLY;
  LIST_META/SET_META/STARTTLS/EXTENDED_HEADERS — вежливый отказ (QEMU
  корректно деградирует).
- Transmission: READ, WRITE (+FUA→fsync), FLUSH, DISC; TRIM/WRITE_ZEROES —
  при включённых флагах (Linux: `fallocate punches`, macOS: `F_PUNCHHOLE`;
  на прочих ОС флаги выключают — zero-fill). BLOCK_STATUS — отказ
  (meta-контекстов нет).
- Все ответы передачи учитывают согласованный формат: после
  STRUCTURED_REPLY **каждый** ответ (включая WRITE/FLUSH/TRIM и ошибки)
  идёт structured-чанками; simple-ответ на WRITE QEMU считает протокольной
  ошибкой (гость получает EIO).
- MaxPayload 4 MiB, TCP_NODELAY, пул буферов на соединение, при Close()
  активным соединениям шлётся ESHUTDOWN.
- Отладка: `SDS_NBD_TRACE=1` — hex-трассировка байтов соединения в лог.

## Ограничения MVP и путь дальше

- Один файл-бэкенд на экспорт; репликация/EC появятся с интеграцией в
  `storage/` (OSD-слой).
- NVMe-эмуляция QEMU даёт «честный» `/dev/nvme*` в госте, но с лишними
  копированиями; целевой путь фазы 4b — vhost-user / vfio-user (см.
  `todos/roadmap.md`).
- Продакшн-платформа Linux (io_uring); macOS — dev-хост, hole punching
  там тоже работает (APFS `F_PUNCHHOLE`).
