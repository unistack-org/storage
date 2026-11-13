# Erasure Coding: разбор Vitastor, наш дизайн и оценка объёма работ

> Источник: исходники Vitastor 3.2.3 (`/tmp/vitastor-full`, github.com/vitalif/vitastor,
> полная git-история, 3219 коммитов) + наша кодовая база (состояние на 2026-11).
> Связано: `vitastor-ideas.md` §7.3, `roadmap.md` Фаза 6, `test-vm-plan.md` §11, `bench-stand.md`.

---

## 1. Как EC устроен у Vitastor

### 1.1 Схема пула (их ответ на вопрос «где параметры EC?») — ДА, на уровне пула

`docs/config/pool.en.md`, `src/cmd/cli_pool_create.cpp`:

```json
{ "name": "ecpool", "scheme": "ec", "pg_size": 3, "parity_chunks": 1,
  "pg_minsize": 2, "pg_count": 256, "failure_domain": "host" }
```

- `scheme`: `replicated` | `xor` (N+1 RAID4-лайт) | `ec` (Reed-Solomon Vandermonde, ISA-L,
  деградация на jerasure без ISA-L). `jerasure` — псевдоним `ec`.
- `pg_size` = k+m (3..64), `parity_chunks` = m, CLI: `create-pool --ec N+K` / `-s R`.
- **`pg_minsize` для EC = k (число DATA-чанков)** — не «минимум живых OSD». Семантика:
  свежая запись всегда ложится минимум на `pg_minsize` OSD; PG с курсайзом < pg_minsize
  неактивен и для чтения, и для записи. В `osd_rmw.h` прямо прокомментировано.
  В доках их собственный FIXME: «поведение pg_minsize может измениться — делать PG
  read-only вместо деактивации».
- `pg_stripe_size` = размер страйпа по образу = `block_size * k` (по умолчанию), т.е.
  первые k·block_size байт тома → один PG, следующие → другой PG. Один PG держит ВСЕ
  k+m чанков страйпа (формула O(1) + таблица pg_mapping в etcd, LP-планировщик).
- `failure_domain` / `level_placement dc=NNNN` — разброс шардов по хостам/ДЦ;
  primary OSD для EC/XOR всегда ставится на DATA-OSD («they can actually read
  something locally»); parity чанки при начальной раскладке балансируются round-robin
  (коммит 66fe1a4, 2020-11).
- Схему пула (replicated↔ec) менять нельзя — только пересозданием пула
  (явно в комментарии peering: «It's not allowed to change the replication scheme
  for a pool other than by recreating it»). **Прямой ответ на наш вопрос: да,
  EC-политика = уровень пула; том фиксирует resolved-значения при создании.**

### 1.2 Где живёт EC-логика: primary OSD, не клиент

Ядро — `src/osd/osd_rmw.{h,cpp}` (1424 строки) + write-машина в
`osd_primary_write.cpp` (co-routine-стиль: `switch` из 12 `resume_N` меток).
Всего «erasure» в `src/` встречается 36 раз — EC у них втиснут в общий RMW-конвейер
XOR/EC, специализация минимальна. Клиент (libvirt/QEMU) шлёт обычные read/write
весь образ; вся страйп-геометрия, RMW, паритеты — на primary OSD, он веером шлёт
subops вторичным OSD (roles 0..pg_size-1, роль = индекс в PG-сете; OID чанка =
(inode, stripe|role) — роль в младших битах stripe).

### 1.3 Протокол OSD↔OSD (`src/client/osd_ops.h`)

Вторичные операции: `SEC_READ`, `SEC_WRITE` (в журнал, нестабильная),
`SEC_WRITE_STABLE` (для реплик — стабильная сразу), `SEC_STABILIZE` (commit),
`SEC_ROLLBACK`, `SEC_DELETE`, `SEC_READ_BMP` (батч битмап), `SEC_LIST`, `SEC_LOCK`.
Запрос 0x80 байт, в `sec_rw` есть **version** (epoch<<bits|seq) — версионирование
объектов вшито в протокол. Двухфазность: write (unstable, в журнал) → SYNC →
STABILIZE (commit) / ROLLBACK. Для реплик фаза unstable не нужна (WRITE_STABLE),
для EC — всегда двухфазна: паритеты должны появиться атомарно с данными.

### 1.4 Путь записи (EC), по `osd_primary_write.cpp`

1. `check_write_queue`: PG активен; нет flush_actions по объекту; нет активной
   записи в тот же объект (очередь per-object). Источник будущих висяков (см. §2).
2. `calc_rmw`: для каждого из k+m ролей — что читать/писать; union-диапазон по
   затронутым страйпам; missing-роли расширяют чтение соседей (нужны k живых);
   чтение битмап даже когда данные не читаются; отказ (`NULL`) при <k живых
   чанков и частичной перезаписи → `-EIO` («Refuse partial overwrite of an
   incomplete object»).
3. `SUBMIT_RMW_READ` — fan-out чтений (k−1 соседей + parity), один общий буфер.
4. Ошибки `-EIO/-EDOM` → `mark_object_corrupted` и **повтор всей фазы 1–3**
   (сброс not_exists/read_error — коммит 2026 года: «stale not_exists would erase
   a chunk that answers fine now»).
5. `pg.ver_override[oid] = fact_ver` — на время записи фиксирует версию для
   параллельных чтений, чтобы чтение не намешало версии данных и паритетов.
6. `calc_rmw_parity_ec`: реконструкция missing → кодирование затронутого
   диапазона (`ec_encode_data` ISA-L / jerasure fallback; кэш суб-матриц для
   частичных наборов parity) → **битмап дырок кодируется тем же RS-кодом**
   (bitmap = кодовое слово: parity-bitmap = encode(data-bitmaps)). Это база
   thin-provisioning в EC — и самый источник их corruption-багов (см. §2).
7. CAS-версия (версия объекта в заголовке; несовпало → `-EINTR` + фактическая).
8. Если epoch PG вырос — **обязан репортнуть состояние/историю PG в etcd ДО
   записи** (иначе переир не соберёт кorum). Блокирующая точка.
9. `SUBMIT_WRITE` (unstable) всем k+m ролям текущего набора.
10. Частичный сбой (ENOSPC/EAGAIN/EIO, часть subops успешна): EC →
    `submit_primary_rollback_subops` (откатившийся набор, ошибки ролбэка
    игнорируются — ведёт в repeer); реплики → mark_partial_write. Без этого
    следующая запись в ту же версию дала бы EEXIST («next writes... will fail
    because they'll try to overwrite the same version number»).
11. Снять ver_override (после write, до stabilize) → remember_unstable_write →
    SYNC/STABILIZE → удаление лишних чанков (misplaced) после sync, PG помечается
    active+clean только когда лишнее удалено.

### 1.5 Peering (`osd_peering_pg.cpp`)

Список версий со всех peers → по каждой версии объекта правило выбора target:
**версия становится целевой, только если она stable, либо unstable но ПОЛНАЯ
(n_roles ≥ pg_size)** — в коде прямо: иначе «client can't be sure that the
pg_minsize guarantee actually holds» (запись, подтверждённая клиенту, могла лечь
на меньшинство набора). Нестабильные хвосты → flush_actions (make_stable /
rollback_to). Объект с числом data-ролей < k при target-версии = INCOMPLETE
(PG_HAS_INCOMPLETE); роли не на своих OSD = misplaced. `PG_HAS_INVALID`
(чанк с номером ≥ pg_size — признак смены схемы) → PG гасится целиком.
EC PG не стартует без связей минимум с k OSD каждой исторической set'ы
(0.9.2). В `osd_peering.cpp` висит их вечный FIXME: «Incomplete EC PGs may
currently easily lead to write hangs ("slow ops")».

### 1.6 Чтение и degraded-режим

- Чтение: primary читает роли; missing/degraded → чтение k чанков и on-the-fly
  реконструкция (`reconstruct_stripes_ec`); клиентские chained-чтения снапшотов
  поверх той же механики. Retry упавших чтений с других реплик (0.8.x).
- Degraded-запись: пропущенный чанк можно перезаписать «на месте» только полным
  оверрайтом; частичная запись в incomplete → EIO. FIXME в коде: «Allow to do
  small writes to the old (degraded/misplaced) OSD set for lower performance
  impact» — не сделано за 6 лет.
- Scrub (`osd_scrub.cpp`): собрать k+m чанков + битмапы, сравнить, дедуп
  идентичных вариантов, при рассогласовании — `ec_find_good`: brute-force
  локатор ошибок по C(live_roles, k) комбинациям (капа `scrub_ec_max_bruteforce`,
  флаг `scrub_find_best`), при ambiguous — два «правильных» подмножества →
  сдаться и репортить. Репорт: «inconsistent: parity chunks don't match data.
  Use vitastor-cli fix». Починка — CLI `fix --bad-osds ... [--part N]`
  (регенерация чанков на новые OSD).

### 1.7 Их модель производительности EC (`docs/performance/theoretical.en.md`)

- Latency EC-записи+fsync: 3.5 сетевых RTT + 1 дисковое чтение + 2 записи
  (immediate commit); 0.5 = (k−1)/k — локальная выдача RMW-чтения.
- Насыщенные randwrite IOPS: `min(bw·k/(k+m), Σ disk_iops / (2 + (k−1) + 2m))`
  (новый стор). EC 2+1 → ~20% iops / 5; EC 6+3 → ~38% / 13.
- Линейная запись: `bw, Σ disk_write · k/(k+m)`; чтение — без потерь.
- Практическая цель: T1Q1 EC write ~2200 IOPS (0.45 мс) против 5000 (0.2 мс)
  у реплик. **Мелкие рандомные записи в EC — всегда ~3–7× хуже реплик; EC у них
  позиционируется для ёмкости/холода, не для горячего IOPS.**
- Их бенч 1.3.1 (3 нод × 8 PM9A3, 25 GbE, `docs/performance/bench2.en.md`),
  write 4K: T1Q1 — 3 реплики 8126/11672 (TCP/RDMA) против EC 2+1 3525/4903
  (EC ≈ 43 % реплик); T12Q128 — 433k/993k против EC 2+1 278k/**821k**
  (RDMA даёт EC-записям ×2.95).

### 1.8 Как они ускорили EC randwrite (Highload'2025 + доки) — и что из этого наше

Структура стоимости EC-randwrite 4K (их знаменатель `2+(k−1)+2m` ops/диск):
RMW-чтение целевого блока (1) + запись целевого блока (1) + (k−1) чтений
старых данных для re-энкода + m чтений паритетов + m записей паритетов;
сверху запись журнала/меты (старый стор: знаменатель `3+(k−1)+3m`, WA 3–5×).
Их арсенал ускорения, по убыванию эффекта:

1. **WA 3–5× → 1×: отказ от двойной записи** (главная тема hl2025).
   Шаг 1 — LSMeta: журнал и есть буфер записи, данные пишутся в область
   журнала один раз, меты 1× в памяти (NonLSM-ветка: 176 000 randwrite,
   ×1.75). Шаг 2 — Write Intent + атомарные записи NVMe (AWUPF): запись
   намерения "блок X будет CRC=Y" (24–48 B) → данные прямо в объект →
   при падении CRC сверяет; атомарность 4K есть у всех NVMe, 128–256 КБ
   (AWUPF>0) у Micron 7450/7500 и Kioxia CD7-R/CD8-R (у Samsung PM9A3
   awupf=0). Итог: **WA ~1.0×, "2× задержка" превращается в ~29 мкс**.
   ⇒ У нас это **уже реализовано** в `pkg/store` (intent.go = их Write
   Intent, atomic.go = RWF_ATOMIC/AWUPF-пробирование, режим CommitJournal =
   их "журнал как буфер"). EC-путь обязан это наследовать: OpWriteV пишет
   unstable-чанк в журнал (append, 1 fsync), commit-marker буферизуется
   (в journal-режиме он не точка durability — у нас это уже семантика
   intent-лога), flatten в объекты — фоном. Прямых 4K-randwrite в объекты
   и RMW паритетов на критическом пути нет → EC-акк ≈ репликационный
   акк + commit-RTT.
2. **Дельта-обновление паритетов — НАШЕ улучшение, которого у них НЕТ.**
   Их `calc_rmw_parity_ec` читает старые данные всех k чанков (в формуле —
   `(N-1)` чтений данных) и пересчитывает паритет с нуля по полному набору.
   Но EC линеен: изменение одного data-чанка на `delta` сдвигает паритет на
   `coef_i·delta`, где `coef_i` — i-я колонка матрицы RS. Значит
   `new_parity = old_parity ⊕ coef_i·delta`, и старые (k−1) чанки читать
   НЕ нужно: читаем целевой блок (за delta) + m старых паритетов, записываем
   целевой + m новых. Знаменатель `2+(k−1)+2m` → `2+2m`: EC 2+1 5→4 (−20 %),
   4+2 9→6 (−33 %), 6+3 13→8 (−38 %). Требование: запись — один
   contiguous-диапазон в одном data-чанке (у нас так и есть, 4 MiB-чанк),
   degraded-группа (k−1 и менее живых) — старый полный путь.
   **Стоимость:** `klauspost/reedsolomon` (наш E4-зависимый) НЕ раскрывает
   матрицу энкодинга и не имеет инкрементального API (только Encode/Reconstruct
   по полному набору). Значит дельта-путь требует **своего** GF(2⁸) + матрицы
   (Vandermonde/Cauchy детерминированы, ~100–200 LOC на той же infra
   golden-векторов E0). Реализуем в E4 как отдельное ADR: (a) свой GF(2⁸)+
   матрица — берём экономию k−1 чтений уже в v1; (b) полный re-encode (как у
   Vitastor) в v1, дельта в v2 — проще, но без экономии. Рекомендую (a) —
   матрица и так нужна для scrub/локации ошибок.
3. **RDMA** (их 1.3.0 без ODP, 1.10 автонастройка): EC 2+1 T12Q128 write
   278k→821k IOPS (×3), CPU сети на OSD ×2 меньше; T1Q1 3525→4903. Для
   насыщенных EC-записей — самый большой единичный множитель. У нас TCP;
   RDMA — дальнейший рычаг вне плана EC (зафиксировать в bench-стенде
   TCP-цифры как базовые).
4. **Пайплайнинг фаз** (их resume_N-машина): не ждать все RMW-чтения перед
   веером записей — encode стримом, write-веер асинхронный, commit
   piggyback'ом. Плюс **group commit**: fsync батча commit-маркеров на OSD
   (у нас journal уже батчит fsync — EC-коммиты должны попадать в тот же
   батч, отдельный commit-fsync запрещён).
5. **Coalescing на клиенте**: несколько записей в одну группу страйда за
   окно → один RMW (стоимость `1+m` чтений амортизируется). Опциональное
   окно коалесинга (default 0 = latency-preserving; ~100 мкс для
   IOPS-профиля). У них аналог — journal на SSD абсорбит 4K и секвенсит
   для HDD.
6. **Full-stripe fast path**: запись размером в страйд (append, provisioning,
   стриминг) — RMW нет вообще, знаменатель = 1 запись на шард, bandwidth
   `bw·k/(k+m)` (у них EC 2+1 linear write 7.74 GB/s — 95 % пропускной
   способности одной линии 25 GbE).
   Максимально использовать наш геометрический инвариант (4 MiB объекты).
7. **Локальные чтения** (их 2.2.0): RMW-чтение на своём OSD = −0.5 RTT
   (формула 0.5=(k−1)/k). У нас применимо в CSI (клиент-под рядом с OSD,
   общий файл) — низкий приоритет.
8. **Диски**: PLP-SSD (суперкапациторы) — fsync дешёвый (1–2k → 60k IOPS
   T1Q1 с fsync); AWUPF-модели (Micron/Kioxia) — WA 1 для 8–124K записей
   (atomic.go их пробывает). Масштабирование числом дисков: знаменатель
   фиксирован, Σdisk_iops растёт линейно.
9. **Выбор k+m**: знаменатель растёт с k+m (2+1→5, 4+2→9, 6+3→13). Для
   IOPS-чувствительного EC — только 2+1/4+2 (наша капа maxShards 12
   совпадает); 6+3+ — чисто холодные ёмкости.
   Итого old→new store у них: EC 2+1 `(14%)/7 → (20%)/5` ≈ ×2.0;
   EC 6+3 `(30%)/17 → (38%)/13` ≈ ×1.55.

**Наша целевая формула (фиксируем в bench-стенде):** EC-запись на OSD в
CommitJournal-режиме = 1 append + 1 fsync (данные), commit — буферизованный
маркер (не fsync) + group-commit-flatten фоном. Плюс дельта-паритет (п. 2)
снижает знаменатель `2+(k−1)+2m` → `2+2m`. Ожидаемо: T1Q1 EC 2+1
write ≈ 0.5–0.7 × репликационного (у нас нет двойной записи на критическом
пути + дельта-паритет, а у них на старом сторе было 0.43×); T12Q128 ≈ 0.4–0.7 ×
(нас тормозит TCP+CPU клиента на encode/веер — не диски). Guardrail матрицы
§5: ±20 % от этих отношений, отдельно замерить полный vs дельта-путь.

---

## 2. Каталог их нюансов и проблем (по полной git-истории)

122 коммита с EC/parity/RMW/ISA-L за 2019-11 … 2026-10. Раскладка по классам —
это и есть наш risk-реестр. Год = дата фикса.

### A. Порча данных (самый дорогой класс)
- **ISA-L 2.31 + EC N+K (K>1) на CPU с GF-NI → потеря данных при EC recovery.**
  ISA-L 2.31 изменила размер элемента EC-таблицы (8→32 байта в GF-NI пути);
  Vitastor ≤2.1.0 читал таблицу по старому. В доках — капслоком: «FORBIDDEN to
  use ... it WILL lead to data loss», инструкция «остановить OSD как можно
  быстрее». Фикс 2025-05: runtime-разведка формата таблицы + abort при
  all-zero строках. **Урок: EC-библиотека = фатальная зависимость; пиннинг +
  golden-векторы обязательны (наш случай: klauspost/reedsolomon, чистый Go).**
- «Do not skip RMW at the end of the file — skipping it may lead to data
  corruption» (issue #116, дек. 2025) + два follow-up фикса оптимизированного
  RMW в shared inode на следующей неделе. Оптимизации пропуска RMW = класс
  corruption-багов.
- «Fix fully-degraded EC writes destroying bitmaps» (2026-06),
  «parity-less EC writes do not destroy bitmaps» (regression-тест),
  «Only update bitmaps when writing data in primary subops» (2026-06),
  «Fix bitmap calculation for EC N+1 & the new store and EC N+2+ for the old
  store» (2025-08), «Fix snapshotted bitmap reads not working in EC pools»
  (issue #92, сент. 2025 — EC+снапшоты починили через 5 лет после обеих фич!).
- «Fix incorrect parity RMW calculation when writing into a lost chunk» (0.8.6),
  «Fix missing parity chunk calculation for EC n+k, k>1 and first parity chunk
  missing» (0.8.6), «Fix crash when calculating a parity chunk with previous
  parity chunk missing» (0.8.1), «Fix degraded parity writes for EC N+K when
  K>1» (0.8.1), «Fix cached EC decoding of multiple stripes with ISA-L» (2026-06),
  «Fix jerasure crashing on bitmap calculation due to lack of 16-byte alignment».
- «Check bitmaps when grouping identical EC chunks in scrub» (2026-07) — scrub
  без сравнения битмап давал ложные консистенсии.

### B. Зависания / висячие операции
- «Fix EC PGs hanging in REPEERING when flush error is last in the batch» (2026-06).
- FIXME в osd_peering.cpp про write hangs на incomplete EC PG — жив до сих пор.
- «Apply recovery pause before writes, after commits, and do not apply it to
  syncs to not block EC pools from functioning» (1.4.4) — пауза восстановления
  вешала EC-пулы.
- «Prioritize EC (non-instantly-stable) operations under journal pressure» (0.8.4)
  — журнал+EC = взаимные блокировки. Резерв места в журнале под commit-фазу —
  их урок, который мы уже записали в `vitastor-ideas.md` §7.3.
- «Fix stop condition in osd_flush» (2024-11).

### C. Полный диск / ENOSPC
- «Handle ENOSPC during write — rollback partial EC writes» (1.6.0, 2024-04) —
  полную семантику ENOSPC для EC дописывали 4 года.
- «Fix OSDs still crashing when the cluster is full with EC» (1.10.1, 2024-12):
  «ENOSPC handling was introduced in 1.6.0 but it was not complete; now it is».
- «Fix "unexpected state during flush: 0x51" with EC» (1.4.7).

### D. Peering / версии / quorum-семантика
- «Accept unstable EC versions on peering only if full to avoid INCOMPLETE on
  subsequent OSD crashes» (2026-10!) — тонкость pg_minsize-гарантии (§1.5) они
  формализовали 7 лет.
- «Do not start EC PGs without at least pg_data_size connections to old OSDs
  from each set» (0.9.2).
- «No need to check for PG intersection if a history set is smaller than EC
  data part count» (2024-07).
- «Forget unstable writes when re-peering» (2020-03) — базовая инвариантность.

### E. Rebalance / миграция + частичная запись
- «Fix that EC segfault (1234 → 5030 partial overwrite)» (1.4.2, 2024-02) +
  regression-тест на тот же сценарий. Гонка смены набора OSD и частичной
  записи в старый набор — segfault в RMW.
- «(Almost) Implement misplaced recovery, integrating it into calc_rmw()» (2020).
- «Fix two potential read/write ordering problems (even though not yet seen in
  tests)» (2021).

### F. Прочее
- «Fix brute-force error locator for EC n+k with k>2» (0.8.9) после «Implement
  brute-force error locator» (0.8.7) — локатор битых чанков переделывали.
- «Fix ISA-L version EC recovery with first missing data chunk not being read»
  (1.2.0) — расхождение поведения ISA-L и jerasure путей.
- «Fix EC just-bitmap reads (len=0)» (0.8.9), «Return EIO instead of EINVAL for
  incomplete EC objects» (0.8.9), «an idiotic bug leading to failed reads with
  -ERANGE with EC :D» (0.9.1), memleak EC-матриц (0.8.7).
- Статистика EC (df/status) была неверной ~полтора года (0.8.4, 0.9.6).

### Выводы из каталога
1. **RMW+битмапы — генератор №1 corruption-багов.** Половина класса A —
   «битмап/паритетDestroy при редком состоянии» (деградация, missing chunk,
   K>1, снапшоты). Нужны property-тесты на каждую ветку RMW с самого начала.
2. **Двухфазная запись (write→commit/rollback) — генератор №2 висяков.**
   Очередь per-object + pending commit/rollback + flush ошибок = REPEERING hangs.
3. Всё «редкое» (ENOSPC, rebalance×partial-write, peering с unstable-версиями)
   они доделывали годами — и чинят до сих пор (2026: 8 EC-фиксов, из них 5 —
   corruption/hangs). EC — это не «написать RS-код», это **протокол координации
   с полным перебором сбоев**; тесты этого перебора и есть основная работа.
4. EC+снапшоты — отдельный подкласс (#92), у них починен последним.
5. Мелочь, но показательно: каждый серьёзный фикс сопровождается regression-тестом
   (`osd_rmw_test.cpp` 1555 строк — больше ядра). Тестовый код ≈ коду.

---

## 3. Наш дизайн (unistack storage)

### 3.1 Параметры — на пуле (решение)

`pkg/meta/pools.go` (PoolRecord):
```go
Mode     string // "" | "replicate" (default) | "ec"
ECData   int    // k, >=2 (1 запрещаем: 1+m бессмысленно, и.geometry упрощается)
ECParity int    // m, >=1; k+m <= 32 (row width placement.Snapshot уже ограничен 32)
MinSize  int    // для EC обязан равняться k (semantics Vitastor: k data chunks)
```
`Validate()`: режимы взаимоисключающи; для EC: k≥2, m≥1, k+m≥3, k+m≤32,
min_size==k, pg_num ст. степени 2 (как сейчас), правило размещения обязательно
и его max_size == k+m (row width таблицы); presets `EC2Plus2AcrossDCs` уже есть
в `crush/presets.go`. `PutPool` CAS (rev 0) → политика неизменяема (как сейчас);
смена режима пула = delete+recreate пустого пула (как у Vitastor).
`VolumeSpec`: резолвит Mode/ECData/ECParity/MinSize из пула при создании
(существующий инвариант «правки пула не двигают данные»). Конверсия
replicate↔EC существующих томов — вне объёма (v1).

### 3.2 Геометрия (без нового объекта-формата)

- 4 MiB объект остаётся. **Страйд = группа из k последовательных объектов
  тома + m паритетных «объектов»**: логический индекс G-й группы занимает
  OID'ы `OIDBase+1 + G*(k+m) .. +G*(k+m)+(k+m-1)`; первые k = data-шарды
  (объекты Gi..Gk-1 как есть), последние m = parity-шарды. OID-диапазон тома
  растёт: OIDCount' = ceil(OIDCount/k)*(k+m) — считаем при создании тома.
- PG назначаем на **группу** (страйд): `PG(G)` = fibonacci-hash от OIDBase+G —
  все k+m шардов группы живут на k+m OSD строки PG-таблицы (row width k+m);
  pg_stripe_size = k·4 MiB (как у них block_size·k).
  Placement-движок менять не надо: `Lookup` возвращает строку k+m рефов,
  роль шарда = индекс строки (как role в Vitastor).
- Паритет = Reed-Solomon Vandermonde, **klauspost/reedsolomon** (Go, SIMD;
  чистый Go = нет нативной зависимости = класс ISA-L-2.31 у нас невозможен
  принципиально). Версия пинится в go.mod, golden-векторы (ISA-L-совместимые,
  4+2/6+3/2+1/8+3) + fuzz в тестах. XOR-фаст-path для m=1 — опционально (v2).
  **Нюанс §1.8 п.2:** библиотека не раскрывает матрицу и не умеет
  инкрементальный update — дельта-паритет (ADR §7.6 вариант a) требует
  собственного GF(2⁸) + той же Vandermonde-матрицы; тогда `reedsolomon`
  остаётся референсом/фолбэком полного re-encode и reconstruct.

### 3.3 Протокол v4 (`pkg/sdsnet/protocol.go`)

В `ReqHeader` (32 B — есть место после flags/pad): `Version uint64` +
`Role uint16`. Новые опкоды: `OpWriteV` (версионная запись шарда в journal,
unstable), `OpCommit` ( OID, Version → stable), `OpRollback` (OID, Version →
удалить unstable), `OpStatVersion` (max stable/unstable версии). Существующие
OpRead/OpWrite остаются (реплицированные тома). OSD: unstable-запись в
write-intent journal (`pkg/osd/intent.go` расширяется стабильностью записи),
commit переводит unstable→stable, нестабильные версии невидимы чтениям;
сборка мусора осиротевших unstable-версий — фоновая (аналог их flush_actions,
только в OSD, без PG-машин).

### 3.4 EC в клиенте (главное архитектурное отличие: EC без primary-OSD)

У Vitastor EC-машина в primary OSD (у них есть peering, PG-состояния,
per-PG owner). У нас клиент напрямую (replication/quorumWrite), peering нет.
Решение: **EC-логика в клиенте**, OSD остаётся «глупым версионным объектным
стором». Клиентский путь (в `pkg/client`, рядом с quorumWrite):

- `ecWriteFullStripe(G, data[k])`: encode → `OpWriteV` на все k+m (версия =
  stripe LSN = monotonically increasing per-volume counter, выдаём с epoch
  MDS-обновлением — CAS на VolumeSpec.Gen достаточно для v1) → ждём k+1? НЕТ:
  **ждем подтверждения от всех k+m (hard) / не менее k+m (soft-режим = ждать
  k+m, ниже k+m = rollback)**; семантика min_size=k: подтверждение = stable
  ≥k data... Детальная семантика — ADR (E0):
  commit-фазу шлём best-effort всем, commit засчитан при k+m подтверждений,
  stragglers — как detachedCtx; подтверждение клиенту — после commit у ≥k+m
  (полный набор; неполные unstable-наборы никогда не видимы — их же правило
  «accept unstable only if full», защищаемое тем, что reader'ы читают только
  stable-версии, а commit требует всех).
- `ecWritePartial`: RMW = прочитать **целевой блок** + m паритетов затронутого
  диапазона (наши обычные OpRead), `new_parity = old_parity ⊕ coef·delta`
  (EC линеен — дельта-обновление, НАШ подход; они re-энкодят по полному
  набору k старых данных), полное обновление затронутой группы: 1 запись
  данных + m паритетов.
  Частичная запись в группу с <k живыми шардами → EIO (их правило).
  **Опциональный coalescing**: несколько записей в одну группу за окно
  (~100 мкс, default 0) → один RMW (стоимость 1+m чтений амортизируется).
- `ecRead(G, off,len)`: k data-шардов; пропущенный/битый → reconstruct по k
  живым (роль-агностично). Деградированное чтение = +k−1 сетевых чтений.
- Параллельные записи в одну группу сериализуются per-stripe мьютексом в
  клиенте + версионным CAS на OSD (CAS-отказ → перечитать). Очередь per-object
  = их check_write_queue, у нас тривиальнее (нет PG-машин).
- Откат: любой сбой фазы write → OpRollback по подтверждённым; ENOSPC
  посреди фазы = частичный набор unstable → видимость не пострадала (readers
  видят старую stable-версию), GC уберёт; повтор записи — новой версией.
  (У них без rollback была EEXIST-порча — у нас версия обходит проблему.)

### 3.5 Смежные подсистемы

- **Снапшоты** (OID-поколения, Gen/LastGen): CoW гранулярностью СТРАЙД —
  запись в шард снапшотнутой группы = копируем всю группу в новые OID
  (k+m объектов) с пересчётом паритетов (RMW-чтение). Частичный CoW
  невозможна с инвариантом parity=RS(data). EC+снапшоты = их баг #92;
  у нас включаем только с фазы E6, с матрицей тестов.
- **TRIM/WRITE_ZEROES**: обнуление группы (полного страйда) = быстрый путь —
  удалить k+m шардов (все нули); частичный trim = RMW. Полная нулевая группа
  при записи = delete (thin-инвариант).
- **Recovery/rebuild** (`pkg/client/recovery.go` v2): фоновый сканер PG-строк;
  потеря роли → выбрать новый OSD по правилу (CRUSH, исключить живые роли) →
  reconstruct из k живых → OpWriteV+Commit (QoS-paced, общий шейпер с recovery v1).
- **Scrub/repair**: stripe-верификация (parity vs data), сравнение + brute-force
  локатор с капающей комбинаторикой (их ec_find_good как референс, C(live,k) ≤
  10⁶); репорт в метрики; чинка — админ-команда `unistorctl vol fix` (регенерация
  шардов; авто-решение «какой шард верный» = rebuild большинства).
- **Клиентский лимит**: `maxReplicas 8 → maxShards 12` (4+2; 8+3 потом — raise
  константы + scratch-массивы).
- **QoS/квоты**: квота считает k+m шардов (учёт как у реплик ×(k+m)); EC-трафик
  rebuild'а в отдельном классе (их урок с recovery pause).

### 3.6 Стенд

4+2 требует ≥6 target'ов, 6+3 — ≥9. `bench-stand.md` (3 file-OSD) годится только
для 2+1 smoke. **Матрица EC возможна только на polygon ≥6–8 target'ов**
(подтверждает `test-vm-plan.md` §11). Требуется расширение полигона: +3–5
file-OSD (бюджет ~0, работы — конфигурация).

---

## 4. План и оценка объёма работы

Оценка: один разработчик, темп как в Фазах 1–5. Итого **~8–10k LOC кода +
~4k LOC тестов**; календарно **22–32 недели**, из них первые ~12 недель дают
usable EC 2+1/4+2 без снапшотов.

| Фаза | Содержание | LOC (prod/test) | Недели |
|---|---|---|---|
| **E0 Spike/ADR** | ADR: семантика min_size/quorum, протокол v4 (write/commit/rollback, LSN), геометрия OID-групп, EC+снапшоты (страйд-CoW), **дельта-паритет (a: свой GF(2⁸)+матрица / b: полный re-encode, см. §1.8 п.2)**; bench reedsolomon (klauspost vs свой, 4 MiB чанки); golden-векторы ISA-L-совместимые; полигон 6 targets | 0 / ~1k | 2–3 |
| **E1 Метаданные+CLI** | PoolRecord Mode/k/m, Validate, VolumeSpec resolve+OIDCount', `unistorctl pool create --ec N+K`, `vol create` контроль, HTTP API, ошибки/док-страница | 0.8k / 0.5k | 1.5–2 |
| **E2 Протокол+OSD версионность** | sdsnet v4: Version/Role в заголовке, OpWriteV/Commit/Rollback/StatVersion; OSD journal stable/unstable, GC осиротевших unstable; batch-варианты; compat v3↔v4 | 1.5k / 1k | 3–4 |
| **E3 Клиентский EC happy path** | геометрия групп, encode/decode обёртка, full-stripe write+commit, полное чтение, EC 2+1 на 3–4 target'ах стенда, метрики | 1.5k / 1.5k | 3–4 |
| **E4 RMW + degraded** | частичные записи (read-modify-write) с **дельта-обновлением паритета** (`new_parity=old⊕coef·delta`, экономит k−1 чтений — наш апгрейд к их `calc_rmw_parity_ec`), EIO-правило incomplete, degraded-чтение/реконструкция, откат/ENOSPC-матрица, per-stripe сериализация, TRIM (zero-stripe fast path), maxReplicas→12 | 1.5k / 1.5k | 4–5 |
| **E5 Rebuild + scrub + fix** | фоновый rebuild (CRUSH-выбор цели, QoS-пейсинг), scrub-верификация страйпов, локатор + `vol fix`, интеграция с recovery v1-очередью | 1.5k / 1k | 3–4 |
| **E6 Снапшоты/клоны + полигон + прод** | страйд-CoW снапшотов, клон/flatten EC-томов, квоты×EC, chaos-матрица на 6+ target'ах, DST-посевы, тюнинг (параллельность RMW, batch commit), runbook | 1.2k / 1.5k | 4–6 |

Промежуточные точки полезности: E3 = «EC читается и пишется целыми страйпами»
(2+1 на стенде); E4 = «частичные записи + деградация» (демо-пригодно);
E5 = «переживает потерю OSD без руками» ; E6 = production EC 4+2.

Что НЕ входит (осознанно): конверсия реплика↔EC, LRC/локально-реcoverable коды,
EC 8+3 (>12 шардов), EC-снапшот-режим «read-only пул», шифрование поверх EC.

## 5. Матрица тестов (обязательная, по мотивам их каталога)

1. Golden-векторы RS (k∈{2,4,6,8}, m∈{1,2,3}) против ISA-L-таблиц + fuzz
   encode↔decode.
2. Матрица сбоев записи: kill OSD в фазах {write-веер, после k-ack, commit-веер,
   частичный commit}; ENOSPC у 1..m targets; rollback-ошибки; повтор записи после
   сбоя (новая версия); каждый seed = DST-посев.
3. RMW-матрица: offset×length × {полный страйд, x-в-средне, граница группы,
   последняя группа (неполная), x в пропущенный шард, x при <k живых (EIO)} —
   сверка всего тома побайтно после каждой серии (их fio_blockstore-подход).
   **Отдельно: дельта-паритет vs полный re-encode — оба дают идентичный
   побайтовый результат на каждом случае (regression на апгрейд E4).**
4. Degraded: потеря r=1..m ролей → чтение ok, запись (полная ok / частичная по
   правилу), rebuild → scrub clean.
5. Rebalance-гонки: смена epoch (reweight/out) во время частичной записи в
   группу (их 1234→5030).
6. Снапшоты EC: снапшот группы с дырками, запись в снапшотнутую группу,
   клон, flatten, vacuum (их #92).
7. Производительность регрессия: EC 4+2 randwrite 4K/128K/4M против реплик
   (цели §1.8: T1Q1 EC ≈ 0.5–0.7× реплик, T12Q128 ≈ 0.4–0.7× — фича, не баг;
   guardrail ±20 %; отдельно: дельта- vs полный ре-энкод, coalescing on/off).

## 6. Риски (свои, из их каталога)

1. **RMW/битмап-корректность** — mitigation: битмап-слов в v1 нет (наша дырка =
   отсутствие объекта, гранулярность 4 MiB) — снимаем их класс A на 80%;
   property-тесты каждой ветки RMW.
2. **Координация write→commit (зависания)** — mitigation: версии+full-set
   commit, нестабильные невидимы, GC в OSD; таймауты и откат обязательны в E4.
3. **EC+снапшоты** — самый поздний срез (E6), риск переноса на E7.
4. **Производительность** — EC randwrite медленнее реплик кратно (модель §1.7);
   EC позиционируем для холодных пулов (как и в roadmap Фаза 6); ожидаемые
   цифры фиксируем в бенч-стенде, не гоняемся за 5000 IOPS.
5. **Библиотека RS** — mitigation: чистый Go + пиннинг + golden-векторы
   (ISA-L-класс исключён).
6. **Стенд** — без 6+ target'ов E4–E6 блокируются; расширить полигон на E0.
7. **Оценка** — их история: 7 лет, 122 EC-коммита, последние фиксы 2026. Наша
   оценка реальна для happy-path+деградации; «последние 10%» (редкие гонки)
   закладываем как постоянный хвост качества (DST-ежедневно, как в Фазах 3–5).

## 7. Решения, требующие подтверждения (E0 ADR)

1. min_size для EC = ровно k (рекомендую, vitastor-семантика).
2. Commit = все k+m стабильны (рекомендую; «k достаточно» = риск INCOMPLETE,
   их октябрьский 2026 фикс).
3. Паритетные шарды — псевдо-объекты в OID-пространстве тома (рекомендую,
   ноль изменений в pkg/store) vs отдельное пространство (проще квоты,
   сложнее снапшоты).
4. EC v1 только 4+2 и 2+1 (капа maxShards=12) — ок?
5. Снапшоты EC = страйд-CoW (дорого, E6) vs запрет снапшотов EC-томов в v1
   (рекомендую начать с запрета, CoW включить в E6 при готовности).
6. Дельта-паритет: (a) свой GF(2⁸)+матрица с первого дня — экономит k−1
   чтений на запись (§1.8 п.2, рекомендую) vs (b) v1 полный re-encode как у
   Vitastor, дельта в v2 (проще, но знаменатель без −33 %).
