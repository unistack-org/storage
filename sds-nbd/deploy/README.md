# Развёртывание демона (OSD + экспорты)

## Файлы

- `sdsd.service` — systemd-юнит с keep-alive (`Restart=always`, `RestartSec=1`).
  Демон защищён от падения на гостевых memory-fault (см. ниже), так что
  перезапуск — это лечение внешних причин (OOM, systemd-рестарт, upgrade),
  а не защита от собственных багов.

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
  (unmount/quiesce в госте → `systemctl restart sdsd`).
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
| Демон умер, гость не дергает диск | systemdRestart сам; attach в логе, I/O продолжается |
| Демон умер во время активного I/O | VM: hotplug-цикл устройства; host: reboot окна |
| `vdpa dev add` вешается | залипший запрос с прошлой жизни → только reboot |
| В логе `guest memory fault ... evicting mappings` | норма после смерти старого демона; демон жив |
| Диск гостя читается, но висит `open()` | залипший request держит folio lock → reboot |

## Сборка

```sh
cd sds-nbd && GOWORK=off GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -o sdsd ./cmd/sdsd
```

Требования хоста: ядро с `CONFIG_VDUSE` (`modprobe vduse virtio_vdpa vdpa`),
`/dev/vduse` доступен рантайму (юнит выдаёт права через `DeviceAllow`).
