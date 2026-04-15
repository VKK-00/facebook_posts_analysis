# PROJECT_ANALYSIS.ru.md

## Цель проекта

`social_posts_analysis` — это локальный пайплайн для сбора постов, комментариев, реплаев, репостов, форвардов и discussion-thread данных из социальных платформ, их нормализации, аналитики и выпуска отчётов.

Проект уже не Facebook-only. Сейчас он поддерживает:

- `facebook`
- `telegram`
- `x`
- `threads`
- `instagram`

Главная цель каждого запуска — собрать данные по одному настроенному источнику, сохранить сырой снимок, затем построить нормализованный snapshot и аналитический отчёт.

## Что делает проект

Проект умеет:

- собирать данные из API и web collectors;
- сохранять сырой результат каждого запуска в `data/raw/<run_id>/`;
- нормализовать данные в parquet и DuckDB;
- разделять origin posts и propagation instances;
- строить связь между origin post, propagation copy и комментариями под ними;
- определять язык;
- кластеризовать тексты в narrative-группы;
- размечать stance/support по заданным сторонам;
- готовить review-export для ручных override;
- строить Markdown/HTML/CSV/XLSX отчёты.

## Структура репозитория

Основные директории:

- [config](C:\Coding projects\facebook_posts_analysis\config) — шаблонный конфиг и локальные конфиги запуска.
- [src/social_posts_analysis](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis) — код приложения.
- [tests](C:\Coding projects\facebook_posts_analysis\tests) — unit и integration tests.
- [data](C:\Coding projects\facebook_posts_analysis\data) — raw/processed данные локальных запусков.
- [review](C:\Coding projects\facebook_posts_analysis\review) — CSV/JSON для ручной корректировки narrative/stance.
- [reports](C:\Coding projects\facebook_posts_analysis\reports) — итоговые Markdown/HTML/табличные отчёты.

## Ключевые модули и их роли

### Конфигурация

- [src/social_posts_analysis/config.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\config.py)
  Публичная точка импорта конфигурации.
- [src/social_posts_analysis/config_models.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\config_models.py)
  Pydantic-модели для всего `project.yaml`.
- [src/social_posts_analysis/config_validation.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\config_validation.py)
  Cross-platform validation.
- [src/social_posts_analysis/config_env.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\config_env.py)
  Подстановка environment variables.

### CLI и пути

- [src/social_posts_analysis/cli.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\cli.py)
  Команды CLI: `collect`, `normalize`, `analyze`, `review-export`, `report`, `export-tables`, `run-all`, `run-many`.
- [src/social_posts_analysis/paths.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\paths.py)
  Резолв project root и output paths. Здесь недавно добавлена централизованная логика для temp-config файлов вне `config/`.

### Сбор данных

- [src/social_posts_analysis/pipeline.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\pipeline.py)
  `CollectionService` и `PipelineRunner`. Выбирает collector по `platform + mode`, запускает multi-pass и orchestration всего пайплайна.
- [src/social_posts_analysis/collectors](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors)
  Все collectors.

Важные collectors:

- [meta_api.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\meta_api.py) — Facebook Graph/Meta API.
- [public_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\public_web.py) — Facebook public/authenticated web collector.
- [telegram_mtproto.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\telegram_mtproto.py) — Telegram MTProto collector.
- [telegram_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\telegram_web.py) — Telegram public web collector.
- [telegram_bot_api.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\telegram_bot_api.py) — Telegram Bot API update-queue collector.
- [x_api.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\x_api.py) — X API v2 collector.
- [x_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\x_web.py) — X public web collector.
- [threads_api.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\threads_api.py) — Threads API collector.
- [threads_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\threads_web.py) — Threads public web collector.
- [instagram_graph_api.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\instagram_graph_api.py) — Instagram Graph API collector.
- [instagram_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\instagram_web.py) — Instagram public web collector.

Общая инфраструктура collectors:

- [web_runtime.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\web_runtime.py)
  Общий Playwright runtime, authenticated browser snapshot, fallback launch logic.
- [range_utils.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\range_utils.py)
  Общая логика date-range.
- [value_utils.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\value_utils.py)
  Общие parse helpers.

### Контракты и propagation

- [src/social_posts_analysis/contracts.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\contracts.py)
  Контракты raw snapshots: `CollectionManifest`, `PostSnapshot`, `CommentSnapshot`, `SourceSnapshot`.
- [src/social_posts_analysis/propagation.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\propagation.py)
  Общие правила origin/propagation/comment scope.

### Нормализация

- [src/social_posts_analysis/normalize.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalize.py)
  Вход в normalization stage.
- [src/social_posts_analysis/normalization/records.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalization\records.py)
  Строит записи таблиц из `CollectionManifest`.
- [src/social_posts_analysis/normalization/merge.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalization\merge.py)
  Merge raw runs и выбор совместимых `source_run_ids`.
- [src/social_posts_analysis/normalization/schemas.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalization\schemas.py)
  Схемы parquet/DuckDB таблиц.
- [src/social_posts_analysis/normalization/persistence.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalization\persistence.py)
  Запись parquet и синхронизация DuckDB.

### Аналитика

- [src/social_posts_analysis/analysis/service.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\analysis\service.py)
  Полный analysis stage.
- [src/social_posts_analysis/analysis/metrics.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\analysis\metrics.py)
  Support/stance агрегаты.
- [src/social_posts_analysis/analysis/cache.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\analysis\cache.py)
  Run-level caching для embeddings и stance.

### Reporting

- [src/social_posts_analysis/reporting/service.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\reporting\service.py)
  Сбор report context и выпуск Markdown/HTML/табличных export-ов.
- [src/social_posts_analysis/reporting/summaries.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\reporting\summaries.py)
  Summary-функции и overview-таблицы.
- [src/social_posts_analysis/reporting/exports.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\reporting\exports.py)
  CSV/XLSX export logic.
- [src/social_posts_analysis/templates/report.md.j2](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\templates\report.md.j2)
  Markdown-шаблон отчёта.

## Как система работает end-to-end

Полный поток такой:

1. CLI читает конфиг через [cli.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\cli.py).
2. `ProjectPaths` вычисляет каталоги raw/processed/review/report.
3. [pipeline.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\pipeline.py) выбирает collector по платформе и режиму.
4. Collector возвращает `CollectionManifest` и пишет raw snapshots в `data/raw/<run_id>/`.
5. [normalize.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalize.py) читает raw manifests, при необходимости делает merge нескольких source runs и пишет parquet/DuckDB.
6. [analysis/service.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\analysis\service.py) делает language detection, narrative clustering и stance labeling.
7. [reporting/service.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\reporting\service.py) строит Markdown/HTML и табличные export-ы.
8. [review](C:\Coding projects\facebook_posts_analysis\review) содержит файлы для ручных override narrative и stance.

## Какие данные использует проект и как они текут

### Источники данных

- Facebook public DOM и Meta API.
- Telegram MTProto, Telegram public web, Telegram Bot API.
- X API v2 и public DOM.
- Threads API и public DOM.
- Instagram Graph API и public DOM.

### Сырой слой

Raw run живёт в:

- `data/raw/<run_id>/manifest.json`
- дополнительные snapshot-файлы collector-а, например `web_post_pages/*.json`

`CollectionManifest` хранит:

- источник (`source`)
- список `posts`
- вложенные `comments`
- `warnings`
- `status`
- `fallback_used`
- cursors и служебные поля запуска

### Нормализованный слой

Текущие таблицы:

- `posts`
- `propagations`
- `propagation_edges`
- `comments`
- `comment_edges`
- `authors`
- `media_refs`
- `collection_runs`

Все они лежат в parquet под processed root и синхронизируются в DuckDB.

Важно: `collection_runs` теперь хранит не только `warning_count`, но и `warning_messages`, чтобы warnings из merged raw runs не терялись после normalization.

### Аналитический слой

Analysis stage создаёт дополнительные таблицы, включая:

- language detection
- narrative clusters
- cluster memberships
- stance labels
- support metrics
- analysis run metadata

### Отчётный слой

Report stage строит:

- Markdown report
- HTML report
- CSV/XLSX export tables

В export tables теперь есть отдельные поверхности `source_run_trace` и `source_warnings`, чтобы после merge было видно:

- какой raw `source_run_id` каким collector-ом и mode был собран;
- был ли fallback;
- какой status получил каждый исходный run;
- какие warnings пришли из каждого конкретного raw run.

## Важные архитектурные решения

### 1. Generic source model вместо Facebook-only схемы

Выбран единый `source.platform + source.url/source_id/source_name`, чтобы один и тот же pipeline работал для разных платформ.

Почему выбран этот путь:

- меньше platform-specific CLI ветвления;
- проще добавлять новые collectors;
- проще держать единый reporting/analysis слой.

Не выбран путь с отдельным продуктом на каждую платформу, потому что это бы дублировало нормализацию и аналитику.

### 2. Propagation — это отдельная сущность

`share`, `forward`, `quote`, `repost` не считаются просто флагами поста. Они выделены в отдельный propagation layer.

Почему:

- это позволяет отдельно считать support/stance по origin post и по propagated copies;
- это позволяет хранить комментарии под propagated copies отдельно от origin comments.

### 3. API first, web second

Если есть официальный API и он применим, проект предпочитает его. Web collectors используются как best-effort fallback или как единственный вариант для публичных поверхностей.

### 4. Local-first storage

Все raw/processed outputs лежат локально. Проект не зависит от внешней managed database.

### 5. Deterministic merge semantics

Normalization merge идёт только между совместимыми `source_run_ids`. Несовместимые raw runs не должны случайно попадать в один snapshot.

## Зависимости между модулями

- `collectors` зависят от `config`, `contracts`, `utils`, `web_runtime`.
- `pipeline.py` зависит от `collectors`, `raw_store`, `normalize`, `analysis`, `reporting`.
- `normalize.py` зависит от `normalization/*`, `contracts`, `propagation`.
- `analysis` зависит от processed parquet tables.
- `reporting` зависит от processed tables и analysis tables.

## Внешние интеграции

- Meta / Facebook APIs
- Telegram MTProto
- Telegram Bot API
- X API v2
- Threads API
- Instagram Graph API
- Playwright / Chromium
- DuckDB
- parquet / polars
- embedding/LLM backends через настроенные providers

## Конфигурация, env vars и runtime assumptions

Главный шаблон:

- [config/project.yaml](C:\Coding projects\facebook_posts_analysis\config\project.yaml)

Основные env vars:

- `META_ACCESS_TOKEN`
- `SOCIAL_BROWSER_USER_DATA_DIR`
- `SOCIAL_BROWSER_PROFILE_DIRECTORY`
- `TELEGRAM_SESSION_FILE`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_BOT_TOKEN`
- `X_BEARER_TOKEN`
- `THREADS_ACCESS_TOKEN`
- `INSTAGRAM_ACCESS_TOKEN`
- `EMBEDDING_BASE_URL`
- `EMBEDDING_API_KEY`
- `LLM_BASE_URL`
- `LLM_API_KEY`

Runtime assumptions:

- Python 3.12+
- Playwright Chromium для web collectors
- валидный browser profile для authenticated web scenarios
- токены и session files не хранятся в репозитории

## Команды запуска, проверки и отладки

Основные команды:

```powershell
.\.runvenv\Scripts\social-posts-analysis.exe collect --config config/project.local.yaml
.\.runvenv\Scripts\social-posts-analysis.exe normalize --config config/project.local.yaml
.\.runvenv\Scripts\social-posts-analysis.exe analyze --config config/project.local.yaml
.\.runvenv\Scripts\social-posts-analysis.exe review-export --config config/project.local.yaml
.\.runvenv\Scripts\social-posts-analysis.exe report --config config/project.local.yaml
.\.runvenv\Scripts\social-posts-analysis.exe export-tables --config config/project.local.yaml
.\.runvenv\Scripts\social-posts-analysis.exe run-all --config config/project.local.yaml
.\.runvenv\Scripts\social-posts-analysis.exe run-many --config config/project.local.yaml --passes 3
```

Проверки:

```powershell
.\.runvenv\Scripts\ruff.exe check .
.\.runvenv\Scripts\mypy.exe src
.\.runvenv\Scripts\pytest.exe -q
```

Отладка raw run:

- открыть `data/raw/<run_id>/manifest.json`
- открыть `data/raw/<run_id>/.../*.json`
- сравнить raw `warnings` и normalized/report output

## Как проект развивался и развивается

Система начиналась как Facebook-only project, затем была переведена на generic social source model.

Ключевые этапы, которые уже произошли:

- rebrand на `social_posts_analysis`;
- добавление Telegram, X, Threads, Instagram;
- выделение propagation-model;
- разделение `config`, `normalize`, `reporting`;
- вынос общего web runtime;
- частичная стабилизация Facebook web collector;
- deterministic merge semantics;
- caching в analysis;
- локальные улучшения data-quality по `telegram_web`, `telegram_mtproto`, `x_web`, `x_api`.
- локальные улучшения data-quality по `telegram_web`, `telegram_mtproto`, `x_web`, `x_api`, включая:
  - сохранение visible discussion counter в `telegram_web`
  - фильтрацию embedded origin/quote status из reply extraction в `x_web`
  - parent-before-child ordering в `telegram_mtproto`, чтобы nested discussion replies не сплющивались, если child пришёл раньше parent
  - adaptive fallback scan limit в `telegram_mtproto`, который теперь учитывает ожидаемый размер discussion thread, если Telegram его отдал
  - локализованный `comments_count` extraction в `public_web` по английским, украинским и русским comment-label строкам
  - более агрессивную фильтрацию локализованных UI/control lines в `public_web`, чтобы строки вроде `Відповісти`, `1 відповідь`, `Ответить`, `Ответы` не попадали в author/message эвристики как содержательные comments
  - общий список локализованных author/control-line исключений между [facebook_web_content.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\facebook_web_content.py) и [facebook_web_extraction.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\facebook_web_extraction.py), чтобы DOM author-selection не расходился с Python cleanup
  - дополнительный DOM timestamp-hint fallback в [facebook_web_extraction.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\facebook_web_extraction.py), чтобы `published_hint` чаще сохранялся прямо из comment block, а не только через поздний разбор текста
  - поддержку локализованных yesterday-hints (`Вчора`, `Вчера в 14:03`) в [facebook_web_timestamps.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\facebook_web_timestamps.py), чтобы cleaner удалял их как timestamp, а не как часть message
  - двухслойный comment-text flow в `facebook_web`: extractor теперь сохраняет `raw_text` и более чистый `text`, а normalization использует `raw_text` для author/timestamp fallback и `text` для итогового message
- live smoke-run без токенов 13 апреля 2026 года для:
  - `facebook_web` на `https://www.facebook.com/VolodymyrBugrov/`
  - `telegram_web` на `https://t.me/s/durov`
  - `x_web` на `https://x.com/OpenAI`

## Актуальные ограничения и риски

- `threads_web` остаётся нестабильным: UI может вернуть ноль постов даже для валидного публичного профиля.
- `instagram_web` пока в основном post-level collector, а не полноценный comment collector.
- Facebook heavy reels могут упираться в login wall или нестабильный comment surface даже в authenticated mode. Локализованный visible `comments_count` теперь чаще сохраняется, а comment snapshots стали чище на украинских и русских UI surface, но это всё равно не гарантирует полноту самих comment snapshots.
- Даже после нового DOM timestamp fallback `facebook_web` всё ещё best-effort: если Facebook не отдаёт сам comment block или заменяет его login wall, ни `author_name`, ни `published_hint`, ни message оттуда локально не восстановить.
- Важно: чистка и author-selection для `facebook_web` теперь используют один и тот же список локализованных control terms, поэтому если снова появится drift между JS extraction и Python cleanup, это уже будет регрессией именно в этой общей таблице исключений.
- Важно отдельно: если в будущем у `facebook_web` снова останется только один `text` без `raw_text`, это будет регрессией качества, потому что cleaner потеряет часть сигналов для fallback-восстановления `author_name` и `published_hint`.
- Это подтверждено live-run от 13 апреля 2026 года: `facebook_web` собрал `10` posts и только `3` comments, при этом в отчёте явно виден gap `visible=99, extracted=1` для reel `https://www.facebook.com/reel/1919764451982763`.
- `telegram_mtproto` fallback scan теперь лучше учитывает ожидаемый размер discussion thread и устойчивее строит parent-chain, но scan всё ещё ограничен верхним лимитом и может не покрыть очень большие discussion threads.
- `telegram_web` теперь лучше сохраняет `comments_count` из видимого discussion counter, но полнота самих comment texts всё равно зависит от наличия публичного discussion feed.
- Это тоже подтверждено live-run от 13 апреля 2026 года: `telegram_web` на `https://t.me/s/durov` собрал `11` posts и `0` comments, потому что для source не был виден linked discussion chat.
- `x_api` может видеть `reply_count`, но не вернуть реальные replies из search coverage; это надо явно доносить до отчёта.
- `x_web` больше не должен классифицировать embedded origin status как reply на quote detail page, но public DOM всё равно может скрывать реальные replies.
- Live-run от 13 апреля 2026 года это подтвердил: `x_web` на `https://x.com/OpenAI` собрал `2` posts, сохранил visible reply counters `386` и `762`, но не увидел ни одного reply article в public DOM и корректно вынес это в warnings/report.
- warnings из merged source runs теперь должны доходить до processed/reporting слоя через `collection_runs.warning_messages`; если это когда-то перестанет происходить, это уже будет регрессией.
- `source_warnings` строится в первую очередь по raw manifests каждого `source_run_id`; если raw manifests отсутствуют, reporting использует fallback на merged `warning_messages`.
- `source_run_trace` тоже строится в первую очередь по raw manifests каждого `source_run_id`; если raw manifests отсутствуют, reporting использует fallback на агрегированную запись `collection_runs`.
- в [facebook_web_timestamps.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\facebook_web_timestamps.py) нельзя снова возвращаться к `strptime` без года для строк вида `March 15`, потому что это уже помечено как проблемный путь в новых версиях Python.
- Пока нет token-based live validation для `threads_api` и `instagram_graph_api`.

## Что уже было заменено или переработано

- Старый Facebook-only mental model заменён на generic social source model.
- Большие модули `config`, `normalize`, `reporting` были разрезаны на более узкие части.
- Общий browser runtime вынесен из отдельных web collectors.
- Propagation rules вынесены в отдельный модуль, чтобы origin/propagation логика не дублировалась в нескольких слоях.

## Что нужно обновлять в этом файле при изменениях проекта

При любом существенном изменении нужно обновлять этот файл, если изменилось хотя бы одно из следующего:

- добавлены новые collectors, режимы или платформы;
- удалены или заменены старые implementation paths;
- изменились parquet/DuckDB таблицы или их смысл;
- изменился data flow от raw snapshot до report;
- изменились warning semantics, merge semantics или caching behavior;
- изменились команды запуска, тестирования или отладки;
- появились новые внешние зависимости, env vars или runtime assumptions;
- обнаружен разрыв между ожидаемым поведением и фактическим live behavior.

## Текущий фокус развития

Сейчас ближайший полезный фокус такой:

1. довести data-quality без внешних токенов там, где это возможно локально;
2. сделать coverage gaps и collector warnings явными в reporting;
3. после появления credentials — провести live validation для `threads_api` и `instagram_graph_api`.

## Обновление: режим person_monitor

С апреля 2026 года в проекте появился новый режим источника:

- `source.kind = "feed"` — старое поведение, когда проект собирает один конкретный page/channel/account как primary source;
- `source.kind = "person_monitor"` — новый режим наблюдения за профилем человека на одной выбранной платформе.

### Что делает person_monitor

`person_monitor` решает две задачи сразу:

- ищет упоминания профиля человека в чужих постах и комментариях;
- собирает собственные посты и комментарии этого человека на чужих внешних поверхностях.

Важно: это всё ещё **одна платформа на один run/config**. Кросс-платформенное объединение пока не встроено в один запуск и должно строиться поверх нескольких run-ов.

### Какие новые файлы и модули добавлены

- [src/social_posts_analysis/person_monitoring.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\person_monitoring.py)
  Новый orchestration и matching layer для `person_monitor`.

### Какие существующие файлы были расширены

- [src/social_posts_analysis/config_models.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\config_models.py)
  Добавлены:
  - `source.kind`
  - `source.aliases`
  - `source.watchlist`
  - `source.search`
- [src/social_posts_analysis/config_validation.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\config_validation.py)
  Добавлена отдельная validation-ветка для `person_monitor`, включая проверку discovery path и platform/mode compatibility.
- [src/social_posts_analysis/contracts.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\contracts.py)
  Расширены raw contracts:
  - `source_kind`
  - `container_source_*`
  - `discovery_kind`
  - `raw_text`
  - `request_signature`
  - `observed_sources`
  - `match_hits`
- [src/social_posts_analysis/pipeline.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\pipeline.py)
  `CollectionService` теперь умеет запускать отдельную ветку `person_monitor`, но старый `feed` flow не заменён и не удалён.
- [src/social_posts_analysis/normalization/schemas.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalization\schemas.py)
  Добавлены новые normalized tables:
  - `observed_sources`
  - `match_hits`
  Также в `posts`, `comments`, `propagations`, `collection_runs` добавлены новые provenance-поля.
- [src/social_posts_analysis/normalization/records.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalization\records.py)
  Raw `CollectionManifest` теперь materialize-ится не только в старые tables, но и в `observed_sources` / `match_hits`.
- [src/social_posts_analysis/normalization/merge.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\normalization\merge.py)
  Merge key теперь опирается на `request_signature`, а не только на старую комбинацию `platform/source/date_range`.
- [src/social_posts_analysis/reporting/service.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\reporting\service.py)
  Добавлены person-monitor context, matched exports и защита от пустых отсутствующих analysis tables.
- [src/social_posts_analysis/reporting/summaries.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\reporting\summaries.py)
  Добавлены агрегаты для authored/mentioned activity и top external sources.
- [src/social_posts_analysis/templates/report.md.j2](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\templates\report.md.j2)
  Добавлены секции `Person Monitor Summary`, `Top External Sources`, `Observed Surfaces`.

### Как теперь работает data flow в person_monitor

Поток для `person_monitor` такой:

1. Конфиг описывает **наблюдаемый профиль** через `source.url`, `source.source_id`, `source.source_name`, `source.aliases`.
2. Конфиг описывает **внешние поверхности** через:
   - `source.watchlist`
   - `source.search`
3. [person_monitoring.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\person_monitoring.py) строит список discovery surfaces.
4. Для каждой внешней поверхности проект создаёт временный **feed-style sub-config** и запускает обычный platform collector.
5. Сырые sub-run snapshots пишутся в подпапки под raw run:
   - `data/raw/<run_id>/person_monitor_surfaces/<surface>/...`
6. После сбора project не сохраняет все найденные items подряд. Он фильтрует только те items, где есть хотя бы один match:
   - `authored_by_subject`
   - `profile_url_mention`
   - `profile_id_mention`
   - `handle_mention`
   - `alias_text_mention`
7. В итоговый root manifest попадают:
   - `posts` с уже проставленным `container_source_*`
   - matched comments
   - `observed_sources`
   - `match_hits`
8. Нормализация materialize-ит это в parquet/DuckDB.
9. Reporting строит отдельные person-monitor exports и summary sections.

### Почему выбран именно orchestration layer, а не переписывание collectors

Были рассмотрены два пути:

- добавить отдельную логику matching внутрь каждого collector;
- сделать один общий orchestration layer поверх уже существующих collectors.

Выбран второй путь, потому что:

- он сохраняет старый `feed` режим почти без изменений;
- не размножает одинаковую matching-логику по `facebook`, `telegram`, `x`, `threads`, `instagram`;
- позволяет постепенно добавлять search adapters, не переписывая raw collectors снова.

Не выбран путь с переписыванием каждого collector-а под `person_monitor`, потому что это увеличило бы риск platform-specific regressions и усложнило бы поддержку.

### Что сейчас считается supported в person_monitor v1

- `facebook`
  - supported path: `public_web`
  - `meta_api` остаётся `feed`-only
- `telegram`
  - supported path: `telegram_mtproto`, `telegram_web`
  - `telegram_bot_api` остаётся `feed`-only
- `x`
  - supported path: `x_api`, `x_web`
- `threads`
  - supported path: `threads_api`, `threads_web`
- `instagram`
  - supported path: `instagram_graph_api`, `instagram_web`

Важное ограничение: search-discovery дизайн добавлен в v1, но default implementation пока честно возвращает `unsupported` warning и продолжает `watchlist` path. Это осознанное промежуточное состояние, а не silent drop.

### Какие новые normalized tables появились

#### observed_sources

Одна строка на внешнюю поверхность или search-query surface, которую orchestrator попытался обработать.

Ключевые поля:

- `run_id`
- `container_source_id`
- `container_source_name`
- `container_source_url`
- `container_source_type`
- `discovery_kind`
- `status`
- `warning_count`

#### match_hits

Одна строка на одно доказательство совпадения.

Ключевые поля:

- `match_id`
- `run_id`
- `item_type`
- `item_id`
- `match_kind`
- `matched_value`
- `platform`
- `container_source_id`

### Какие новые export tables появились

- `observed_sources.csv`
- `match_hits.csv`
- `matched_posts.csv`
- `matched_comments.csv`

### Что важно знать про dedupe и provenance

- Один и тот же post/comment, найденный и через `watchlist`, и через `search`, не должен дублироваться в итоговом `posts/comments`.
- При этом `observed_sources` всё равно сохраняет обе discovery paths.
- Для самого item сейчас при дедупликации предпочитается `watchlist`, если item пришёл и из `watchlist`, и из `search`.
- `match_hits` не схлопываются до одного «лучшего» match kind. Если у item одновременно есть `authored_by_subject` и, например, `profile_url_mention`, обе строки сохраняются.

### Новые ограничения и открытые вопросы

- Search adapter infrastructure уже есть в дизайне, но default behavior сейчас warning-only. То есть `search-only` config валиден, но без platform-specific adapter-а может не найти ни одной реальной внешней поверхности.
- `person_monitor` не делает cross-platform entity resolution между разными run-ами.
- Для платформ с ограниченным public DOM те же старые ограничения остаются:
  - `facebook_web` heavy reels
  - `x_web` shallow reply DOM
  - `telegram_web` discussion completeness
  - `threads_web` нестабильный публичный UI
- Если в будущем появятся реальные search adapters, в этом файле нужно обновить:
  - supported/unsupported status по платформам;
  - data flow для `source.search`;
  - ограничения по smoke validation.

### Что было проверено при внедрении person_monitor

Проверки после внедрения:

```powershell
.\.runvenv\Scripts\ruff.exe check .
.\.runvenv\Scripts\python.exe -m mypy src
.\.runvenv\Scripts\python.exe -m pytest -q
```

Новые регрессии добавлены в:

- [tests/test_person_monitoring.py](C:\Coding projects\facebook_posts_analysis\tests\test_person_monitoring.py)
  - config validation
  - orchestration dedupe
  - match hit preservation
  - reporting/export surfaces
