# Drain OSD: план и спецификация

> Дата: 2026-11. Связано: `vitastor-ideas.md` §6.7, `roadmap.md` Фаза 5b,
> `runbook.md` (секции «Смерть ноды», «Recovery: починка реплик»).
> Исходники Vitastor 3.2.3 (`/tmp/vitastor-full`) + наша кодовая база.

---

## 1. Что делает Vitastor (исходники)

У Vitastor **нет отдельной команды drain** — это процедура из трёх шагов,
которую оператор выполняет руками:

1. `vitastor-cli modify-osd --reweight 0 <ID>` — OSD уходит из placement
   (вес 0 в CRUSH). Данные с него **не переезжают сами**: новые записи
   ложатся на других, а существующие реплики на этом OSD становятся
   «недостающими» и ждут recovery.
2. Оператор ждёт, пока auto-rebalance / recovery перенесут данные
   (у Vitastor recovery — фоновый, у нас — явный `recovery run`).
3. `vitastor-disk purge <device>` — физическое удаление диска.

Мёртвый (недоступный) OSD: `vitastor-cli rm-osd <ID>` — удаление из
реестра, данные считаются потерянной репликой и восстанавливаются
recovery с других.

Флаги `no_rebalance` / `no_recovery` у `modify-osd` отключают фоновые
процессы; PG-состояние `left_on_dead` помечает данные, оставшиеся на
мёртвом OSD.

**Вывод:** у нас есть все кирпичи (`osd out/in`, `map reweight`,
`rebalance`, `recovery ls/check/run`, `status`), но нет:
- `osd rm` — удаления OSD из реестра (аналог `rm-osd`);
- единого оркестратора `osd drain`, который связывает шаги в одну команду.

## 2. Что у нас уже есть

| Кирпич | Где | Статус |
|---|---|---|
| `osd out <id>` (weight 0) | `cmd/unistorctl/main.go` osdCmd | ✅ |
| `osd in <id> [-weight w]` | там же | ✅ |
| `osd ls` | там же | ✅ |
| `map reweight <id> <w>` (alias) | mapCmd | ✅ |
| `rebalance [-once]` (free-space reweighter) | `pkg/rebalance` + rebalanceCmd | ✅ |
| `recovery ls/check/run` | `pkg/client/recovery.go` + recoveryCmd | ✅ |
| `status` (evicted-предупреждения) | statusCmd | ✅ |
| `meta.SetOSDWeight` / `GetOSD` / `PutOSD` / `ListOSDs` | `pkg/meta/records.go` | ✅ |
| `meta.RemoveOSD` (удаление из реестра) | — | ❌ **добавляем (C)** |
| `osd drain <id>` (оркестратор) | — | ❌ **добавляем (B)** |
| runbook-секция «Drain OSD» | `todos/runbook.md` | ❌ **добавляем (A)** |

## 3. Сcope: три задачи

### A. Runbook: секция «Drain OSD» + фикс бага

- Новая секция в `todos/runbook.md` с пошаговой процедурой drain
  (команды, что ждать, как проверить, как удалить из реестра).
- Кросс-ссылка из «Смерть ноды» и «Recovery: починка реплик».
- **Фикс бага**: в секции «Recovery: починка реплик» (~строка 642)
  написано `unistorctl node out <osd>` — подкоманды `node` не существует.
  Правильно: `unistorctl osd out <id>`.

### B. `unistorctl osd drain <id>` — оркестратор

Одна команда делает то, что оператор делал руками:

```
osd drain <id> [-rm] [-iops N] [-bps S] [-timeout D] [-dry-run]
```

Поток:
1. **out**: `meta.SetOSDWeight(id, 0)` — OSD уходит из placement.
2. **rebalance**: один проход `rebalance.New(cl, opts).Once(ctx)` —
   пересчитывает веса по free-space и публикует новый epoch, чтобы
   данные начали переезжать с выведенного OSD.
3. **recovery loop**: цикл `cl.RecoveryScan(ctx)` → для каждого
   нездорового тома `v.Recover(ctx, rep, opts)` (paced по -iops/-bps),
   пока все тома `Healthy()` или не истёк `-timeout`.
4. **report**: итог — что перенесено, что осталось, healthy ли кластер.
5. **`-rm`** (опционально): если всё healthy — `meta.RemoveOSD(id)`
   (удаление из реестра, шаг C).

Флаги:
- `-rm` — удалить OSD из реестра в конце (только если всё healthy).
- `-iops N` / `-bps S` — темп recovery (по умолчанию 200 iops / 200M,
  как у `recovery run`).
- `-timeout D` — общий дедлайн цикла recovery (по умолчанию 10m).
- `-dry-run` — показать план (out + rebalance plan), ничего не писать.

Технические детали:
- `osdCmd` сейчас получает только `ms meta.Store`; для drain нужен
  `cl *client.Cluster` (для RecoveryScan и rebalance). Меняем сигнатуру
  `osdCmd(ctx, cl, ms, args)` и dispatch-строку.
- `RecoveryScan` возвращает псевдо-отчёт для неоткрываемых томов
  (`Items: [{OSD:-1, Source:-1}]` → `Healthy()==false`). Цикл drain
  должен такое считать ошибкой/условием остановки, а не крутиться
  вечно: если том не открывается, recovery его не починит — выходим
  с ошибкой.
- `RecoveryOpts.Limit` = `qos.NewPair(0, 0, iops, bps)` (write-lane,
  как в `recovery run`).

### C. `meta.RemoveOSD` + `unistorctl osd rm <id>`

- `meta.RemoveOSD(ctx, s, id)` в `pkg/meta/records.go` рядом с
  `SetOSDWeight`: `s.Delete(ctx, PrefixOSDs, strconv.FormatInt(int64(id),10), -1)`
  (паттерн `DeleteVolume`).
- `unistorctl osd rm <id>` в osdCmd:
  - **guard**: если `info.Weight != 0` — отказ (нельзя удалить из
    реестра OSD, который ещё в placement: данные на нём «потеряются»
    без переноса). Сначала `osd out` / `osd drain`.
  - если weight 0 — `meta.RemoveOSD`.
- Юнит-тест `RemoveOSD` в `pkg/meta/meta_test.go` (паттерн
  `TestPutGetDelete`).

## 4. Порядок работы

1. Написать этот план (сейчас).
2. C: `meta.RemoveOSD` + тест.
3. C: `osd rm` в CLI (guard weight 0).
4. B: `osd drain` в CLI (сигнатура osdCmd + оркестратор).
5. A: runbook-секция + фикс `node out` → `osd out`.
6. Обновить `roadmap.md` и `vitastor-ideas.md`.
7. `go build ./...`, `go test ./pkg/meta/... ./pkg/rebalance/... ./pkg/client/...`,
   smoke `unistorctl osd` usage.

## 5. Приёмка

- `unistorctl osd rm <id>` отказывает при weight != 0, удаляет при weight 0.
- `unistorctl osd drain <id>` выводит OSD, запускает rebalance, крутит
  recovery до healthy/timeout, по `-rm` удаляет из реестра.
- `unistorctl osd drain <id> -dry-run` ничего не пишет.
- Runbook-секция «Drain OSD» с рабочими командами; `node out` исправлен.
- `go build ./...` и тесты pkg/meta, pkg/rebalance, pkg/client зелёные.
