# Развёртывание демона (OSD + экспорты)

## Файлы

- `unistord.service` — systemd-юнит с keep-alive (`Restart=always`, `RestartSec=1`).
  Аргументы демона берутся из `UNISTORD_ARGS` в `/etc/unistord/unistord.env`
  (EnvironmentFile), права на `/dev/vduse/control` юнит выдаёт через
  `DeviceAllow`. Демон защищён от падения на гостевых memory-fault (см. ниже),
  так что перезапуск — это лечение внешних причин (OOM, systemd-рестарт,
  upgrade), а не защита от собственных багов.
- `modules-load.d/unistord.conf` — автозагрузка ядерных модулей
  (`vduse`, `vdpa`, `virtio_vdpa`, `nbd`) на boot. См. «Модули ядра» ниже.

## Реконнект VDUSE (что гарантировано)

Устройство `/dev/vduse/<имя>` и зарегистрированное vDPA-устройство переживают
смерть демона. Новый процесс при старте:

1. переоткрывает контрольный fd устройства;
2. восстанавливает кольца через `VDUSE_VQ_GET_INFO` (num/desc/avail/used/ready);
3. заново назначает kick-евентфайлы (`VDUSE_VQ_SETUP_KICKFD`) — ядро само
   реплеит накопленные kicks;
4. заново тянет IOTLB: полный `IOTLB_GET_FD` pull + дожим по промаху
   (драйвер virtio-vDPA никогда не шлёт `VDUSE_UPDATE_IOTLB`);
5. гасит («дрейнует») устаревшие записи avail-кольца, отвечая EIO — иначе
   незавершённый запрос намертво вешает гостевой диск.

## Фундаментальное ограничение ядра

При закрытии fd демона `vduse_dev_release` снимает **все** IOTLB-маппинги
(`vduse_dev_dereg_umem` по всем address space). Страницы гостевых буферов
уничтожены, а записи таблицы остаются — любое обращение к «висячему» запросу
даёт SIGBUS. Запрос, бывший в полёте в момент смерти демона, завершить
невозможно **ни одной** реализацией (наш Go-демон, libvduse, Vitastor, QEMU).
Мы это переживаем: дроп идёт под `SetPanicOnFault`+recover, демон живёт
дальше и обслуживает новые сессии.

Практический вывод:

- **Плановый restart безопасен**, когда гостевой I/O в полёте нет
  (unmount/quiesce в госте → `systemctl restart unistord`).
- **Аварийный kill посреди I/O** оставляет гостевой диск залипшим до
  guest-visible сброса устройства:
  - VM: hot-unplug/hot-plug `vhost-vdpa-pci` из QEMU — предпочтительный путь;
  - host-attach: `vdpa dev del <имя>` — **вешается**, пока запрос залип
    (device_del мьютекс), тогда остаётся перезагрузка хоста.
- D-state задачи (`open()` диска, `vdpa dev del`) не убиваются SIGKILL —
  это ядро, не мы.

## Runbook

| Симптом | Действие |
|---|---|
| Демон умер, гость не дергает диск | systemd Restart сам; attach в логе, I/O продолжается |
| Демон умер во время активного I/O | VM: hotplug-цикл устройства; host: reboot окна |
| `vdpa dev add` вешается | залипший запрос с прошлой жизни → только reboot |
| В логе `guest memory fault ... evicting mappings` | норма после смерти старого демона; демон жив |
| Диск гостя читается, но висит `open()` | залипший request держит folio lock → reboot |
| Сертификат TLS истёк/меняется | положить новые файлы, `systemctl kill -s HUP unistord` (или `unistorctl reload -http https://<узел>:8443` / `POST /v1/reload`) — без рестарта и без касания экспортов; CA перечитывается только на старте |
| Поправил `-qos-class`/`-crushmap`/`-create` в конфиге | те же файлы на тех же путях + `systemctl kill -s HUP unistord` (или `unistorctl reload`) — демон перечитывает их без рестарта; слушатели, `-store`/`-osd`, `-admin-token` — только рестарт (см. `restart_required` в отчёте) |

## Сборка

```sh
GOWORK=off GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -o unistord ./cmd/unistord
```

Требования хоста: ядро с `CONFIG_VDUSE` (`modprobe vduse virtio_vdpa vdpa`),
`/dev/vduse` доступен рантайму (юнит выдаёт права через `DeviceAllow`).
Автозагрузка модулей — в разделе «Модули ядра» ниже.

Подключение тома как диска QEMU (после `POST /v1/exports` с `type=vduse`):

```sh
vdpa dev add name <devname> mgmtdev vduse
vdpa -j dev show <devname>                       # minor чардева vhost_vdpa
mknod /dev/vhost-vdpa-<minor> c 10 <minor>       # если udev не создал
qemu-system-x86_64 ... \
  -device virtio-iommu-pci \
  -device vhost-vdpa-device-pci,vhostdev=/dev/vhost-vdpa-<minor>,bootindex=N
```

(создание vDPA-устройства и mknod делает `storageagent` клетки; блок приведён
для ручного воспроизведения).

## Модули ядра

Демон — непривилегированный процесс: `NoNewPrivileges=yes` +
`ProtectSystem=strict` в `unistord.service`, так что `modprobe` из него
невозможен **по замыслу** и добавлять ему привилегии нельзя. Загрузку модулей
делает systemd от root, двумя взаимодополняющими путями:

1. **Статически на boot** — `modules-load.d/unistord.conf` (этот каталог):

   ```sh
   install -Dm644 deploy/modules-load.d/unistord.conf \
     /usr/lib/modules-load.d/unistord.conf
   systemctl restart systemd-modules-load.service
   lsmod | grep -E 'vduse|vdpa'
   ```

   `systemd-modules-load.service` отрабатывает на раннем boot, до
   `unistord.service`, и это единственный путь, который работает, когда
   демон запущен не через systemd (ручной запуск, контейнер с общим ядром).
   Директивы `ModulesLoad=` в `[Service]` в systemd **нет** — механизма
   «загрузи модуль по списку из юнита» не существует; он ровно такой: файл в
   `modules-load.d`.

2. **Перед стартом юнита** — `ExecStartPre=-/usr/sbin/modprobe -q ...` прямо
   в `unistord.service`: systemd исполняет его от root до основного процесса,
   префикс `-` означает, что отсутствие модуля (например, он встроен в ядро)
   не валит юнит.

Плюс автоматика ядра, на которую можно опираться:

- `open("/dev/vduse/control")` — modalias-автозагрузка уже тянет `vduse`,
  если модуль установлен и udev работает;
- `vdpa dev add ... mgmtdev vduse` тянет `virtio_vdpa`.

Нечего грузить, если модули собраны в ядро (`CONFIG_VDUSE=y`): `modprobe -q`
молча ничего не делает, `DeviceAllow` в юните при этом обязателен — он даёт
доступ к чардевам, а не загружает модули.

**Не** надо добавлять `ProtectKernelModules=yes` в юнит: он блокирует
загрузку модулей из процесса, и `ExecStartPre` перестанет работать.

Диагностика: если `/dev/vduse/control` нет, демон отвечает точной подсказкой
(`vduse: /dev/vduse/control is missing: the vduse kernel module is not loaded
- run modprobe vduse ...`), а не общим `open: no such file or directory`.
