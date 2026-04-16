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
- `instagram_web` пока остаётся best-effort collector: comment visibility зависит от public DOM, но post/comment snapshots теперь сохраняют `raw_text`, чтобы `person_monitor` не терял handle/profile URL/alias signals, если очищенный `text` оказался пустым или неполным.
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

Исключение из этого правила уже есть:

- для `telegram` при `collector.mode = "mtproto"` добавлен реальный search adapter, который умеет:
  - выполнять global MTProto message search по `source.search.queries`
  - строить discovery surfaces по `chat/channel`, в которых найдены сообщения
  - различать `posts` и `comments` через тип Telegram surface: `channel` против `group/chat`
  - исключать сам наблюдаемый профиль уже на уровне orchestrator, как и для других adapters

- для `telegram` при `collector.mode = "web"` добавлен ограниченный, но реальный public-web search adapter, который умеет:
  - принимать explicit public Telegram handles и `t.me` / `t.me/s/...?...` URLs из `source.search.queries`
  - открывать public `t.me/s/...` surface или channel-local `?q=` search URL
  - считать surface найденной только если `?q=` URL реально вернул сообщения
  - резолвить external surface без fake global search по всему Telegram web

- для `x` при `collector.mode = "web"` добавлен real browser search adapter, который умеет:
  - открывать `https://x.com/search?...&f=live`
  - собирать видимые tweet cards со search page
  - строить discovery surfaces по авторам найденных tweet-ов
  - разделять `posts` и `comments` через card-level признак `is_reply`

- для `x` при `collector.mode = "x_api"` добавлен реальный search adapter, который умеет:
  - выполнять X search по `source.search.queries`
  - строить discovery surfaces по авторам найденных tweets
  - исключать сам наблюдаемый профиль из найденных external surfaces
  - фильтровать `posts` / `comments` через `source.search.include_posts` и `source.search.include_comments`

То есть для `telegram_mtproto`, `telegram_web`, `x_api` и `x_web` `source.search` больше не warning-only.

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
- Одна и та же внешняя surface, найденная и через `watchlist`, и через `search`, тоже не должна сканироваться дважды.
- При этом `observed_sources` всё равно сохраняет обе discovery paths.
- Для самого item сейчас при дедупликации предпочитается `watchlist`, если item пришёл и из `watchlist`, и из `search`.
- Для самой surface тоже сейчас приоритет у `watchlist`: если один и тот же `container_source` найден и в `watchlist`, и через search adapter, orchestrator сканирует его один раз как `watchlist` source.
- `match_hits` не схлопываются до одного «лучшего» match kind. Если у item одновременно есть `authored_by_subject` и, например, `profile_url_mention`, обе строки сохраняются.

### Новые ограничения и открытые вопросы

- Search adapter infrastructure уже есть в дизайне, но default behavior сейчас warning-only. То есть `search-only` config валиден, но без platform-specific adapter-а может не найти ни одной реальной внешней поверхности.
- Для `telegram_mtproto` этот пробел частично закрыт: search adapter уже есть, но он строит discovery surfaces по `chat/channel`, в которых найдено сообщение. Это хорошо покрывает mentions и authored activity в публично-доступных Telegram surfaces, но не гарантирует полный coverage всех закрытых или недоступных чатов.
- Для `telegram_web` этот пробел закрыт только частично и более узко: public web умеет искать `?q=` только внутри уже заданного `t.me/s/<channel>` feed. Поэтому `telegram_web` adapter не делает global content discovery по всему Telegram, а только резолвит explicit public handles / `t.me` URLs и channel-local search URLs.
- Для `x_web` этот пробел тоже частично закрыт: search adapter уже есть, но он зависит от public browser search surface и видимого DOM. То есть он полезен для discovery внешних аккаунтов, но не гарантирует стабильный coverage всех релевантных results без authenticated browser.
- Для `x_api` этот пробел частично закрыт: search adapter уже есть, но пока он строит discovery surfaces только по авторам найденных tweets. Это полезно для mentions, но не гарантирует полное покрытие всех authored replies на чужих thread surfaces.
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
  - `telegram_mtproto` search discovery
  - `telegram_web` search discovery
  - `x_web` search discovery
  - `x_api` search discovery
  - dedupe одной и той же surface между `watchlist` и `search`
- [tests/test_collectors.py](C:\Coding projects\facebook_posts_analysis\tests\test_collectors.py)
  - `telegram_mtproto` search discovery filters `posts` vs `comments` по типу найденной surface
  - `telegram_web` search discovery resolves explicit public handles and `/s/...?...` URLs
  - `x_web` search discovery filters `posts` vs `comments` по card-level reply signal

## Обновление: Threads search adapters для person_monitor

В апреле 2026 года `person_monitor` был расширен ещё на два search path для Threads:

- [src/social_posts_analysis/collectors/threads_api.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\threads_api.py)
  - добавлен реальный search adapter поверх официального `GET /keyword_search`;
  - adapter использует `q`, `search_type=RECENT`, `search_mode`, `since`, `until`, `limit`, `fields`;
  - `source.search.queries` теперь могут реально открывать discovery surfaces в `threads_api`, а не только порождать warning;
  - фильтрация `posts` против `comments` делается по полю `is_reply`;
  - если приложение не одобрено для `threads_keyword_search`, API может вернуть только посты самого аутентифицированного пользователя, и тогда orchestrator увидит ноль внешних surfaces.

- [src/social_posts_analysis/collectors/threads_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\threads_web.py)
  - добавлен public-web search adapter через `https://www.threads.net/search?q=...`;
  - adapter строит external surfaces по авторам видимых search result cards после Playwright-рендера;
  - URLs нормализуются к каноническому виду `https://www.threads.net/@<username>`, чтобы совпадать с остальными Threads collectors;
  - этот adapter сейчас честно считается `posts`-only: он умеет находить внешние аккаунты по найденным search result posts, но не гарантирует надёжное разделение replies/comments на public search surface.

- [src/social_posts_analysis/person_monitoring.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\person_monitoring.py)
  - добавлены ветки `_discover_threads_api_sources()` и `_discover_threads_web_sources()`;
  - для `threads_web` orchestrator теперь явно пишет warning, если конфиг просит `include_comments`, потому что current public search adapter не поддерживает reply/comment-only discovery.

Почему выбран именно такой путь:

- Для `threads_api` выбран официальный endpoint `keyword_search`, а не reverse-engineered private surface. Это снижает риск быстро ломающегося кода и лучше согласуется с текущей архитектурой API collectors.
- Для `threads_web` не выбран агрессивный parser с попыткой восстанавливать reply semantics из нестабильного DOM. Сейчас search page надёжно отдаёт author links и post links, но не даёт такого же явного reply signal, как `x_web` или `threads_api`. Поэтому текущий web adapter ограничен discovery по posts и явно предупреждает о своём coverage gap.

Что изменилось в проверках:

- [tests/test_person_monitoring.py](C:\Coding projects\facebook_posts_analysis\tests\test_person_monitoring.py)
  - добавлены branch-level тесты для `threads_api` search discovery;
  - добавлены branch-level тесты для `threads_web` search discovery;
  - добавлен тест, что `threads_web` comment-only discovery не молчит, а возвращает явный warning.

- [tests/test_collectors.py](C:\Coding projects\facebook_posts_analysis\tests\test_collectors.py)
  - добавлен collector-level тест, что `threads_api` search discovery действительно разделяет `posts` и `comments` по `is_reply`;
  - добавлен collector-level тест, что `threads_web` search discovery дедуплицирует authors и канонизирует profile URLs к `threads.net`.

Новая фактическая граница поведения:

- `threads_api` больше не warning-only для `source.search`;
- `threads_web` тоже больше не warning-only для `source.search`, но только в post-level discovery режиме;
- для `threads_web` reply/comment discovery по public search UI всё ещё остаётся известным ограничением и должно сохраняться как warning в report.

## Обновление: усиление profile-feed extraction в threads_web

После live smoke `person_monitor` на Threads стало видно, что search discovery уже работает, но следующий шаг может терять найденный внешний профиль на стадии profile-feed extraction.

Проблема была в DOM:

- public profile page Threads часто не отдаёт посты через `article`;
- реальные post cards на profile surface лежат в `data-pressable-container="true"`;
- из-за этого [src/social_posts_analysis/collectors/threads_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\threads_web.py) мог успешно найти внешний аккаунт через search, но затем вернуть `posts: []` при чтении его профиля.

Что изменено:

- `ThreadsWebCollector._extract_profile_payload(...)` теперь после старого `article` path собирает и fallback-кандидатов из `data-pressable-container="true"`;
- fallback сохраняет `raw_text`, `permalink`, `status_id`, `created_at`, `author_name`, `author_username`, media-признаки и возможный `origin_permalink`;
- `ThreadsWebCollector._merge_profile_post_candidates(...)` объединяет старый и новый paths по `status_id` и выбирает более содержательный вариант;
- `ThreadsWebCollector._extract_visible_post_text(...)` очищает `raw_text` до реального тела поста и отрезает:
  - username/display name;
  - time hint вроде `1d`;
  - metric tail вроде `120`, `12`, `5` или строки вида `120 replies 5 likes`;
  - footer/UI noise.
- `ThreadsWebCollector._build_posts_from_payload(...)` теперь также протаскивает `raw_text` в `PostSnapshot`, чтобы raw manifest и downstream debug не теряли исходный profile-card текст.

Почему выбран именно этот путь:

- не выбран вариант «полностью отказаться от `article` и оставить только новый DOM path», потому что Threads public UI нестабилен и старый path всё ещё может быть полезен на части surfaces;
- не выбран вариант «просто писать весь `raw_text` в `message`», потому что это загрязняет downstream normalization и person-monitor reports метриками и UI-текстом.

Фактический эффект:

- найденные внешние profiles в `person_monitor` теперь могут вернуть реальные `posts`, даже если profile page не содержит `article`;
- detail/reply extraction в `threads_web` по-прежнему остаётся best-effort и зависит от текущего public DOM.

Когда этот раздел нужно обновлять дальше:

- если Threads снова изменит profile DOM и fallback перестанет видеть post cards;
- если detail/reply extraction тоже будет переведён на `pressable-container` fallback;
- если live smoke покажет новый тип DOM-шума в `raw_text`, который нужно вычищать из `message`.

## Обновление: detail/reply extraction в threads_web

Следующим шагом после profile-feed fix был усилен detail-page extractor в [src/social_posts_analysis/collectors/threads_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\threads_web.py).

Проблема была похожая:

- на реальных public Threads post pages `article` тоже часто отсутствует;
- при этом сама страница всё равно содержит main post и видимые replies в `data-pressable-container="true"`;
- из-за этого `_extract_detail_payload(...)` мог вернуть пустой `replies`, хотя в браузере ответы были видны.

Что изменено:

- `_extract_detail_payload(...)` теперь собирает и старый `article` path, и новый `pressable_rows` fallback;
- для fallback сохраняются `status_id`, `permalink`, `reply_to_status_id`, `created_at`, `author_name`, `author_username`, `raw_text`, очищенный `text` и `like_count`;
- `_merge_detail_rows(...)` объединяет article и pressable candidates по `status_id`, чтобы не дублировать один и тот же reply;
- `_attach_detail_reply_targets(...)` гарантирует, что main post не попадает в `replies`, а replies без явного parent связываются хотя бы с root post;
- `_order_detail_rows(...)` переставляет reply rows только по явному `reply_to_status_id`, чтобы parent reply гарантированно обрабатывался раньше child reply, даже если DOM вернул child карточку выше parent карточки;
- `_collect_replies_for_post(...)` теперь протаскивает `raw_text` в `CommentSnapshot`, чтобы дебаг и downstream review не теряли исходный detail-card текст.

Почему выбран именно такой путь:

- не выбран вариант переписать весь detail extractor только под новый DOM path, потому что старый `article` path всё ещё может давать более точный `reply_to_status_id`, если Threads его отдаёт;
- не выбран вариант усложнять nested reply inference по нестабильному визуальному DOM, потому что public Threads UI не даёт надёжного и стабильного parent signal для всех ответов;
- не выбран вариант угадывать parent по layout-эвристикам вроде x/y координат, порядка `pressable`-блоков или глубины вложенности DOM-узлов, потому что live inspection на public detail page этого не подтвердил.

Фактическая граница поведения после этого:

- если public detail DOM виден, collector теперь чаще получает реальные visible replies даже без `article`;
- если у reply есть явный `reply_to_status_id`, collector теперь надёжнее восстанавливает nested depth даже при `child-before-parent` порядке DOM-карточек;
- если у reply нет явного parent signal, он привязывается к root post, а не теряется совсем;
- полноценное восстановление сложной nested reply topology всё ещё остаётся best-effort и ограничено самим public DOM.

Что подтверждено live:

- targeted feed smoke от `2026-04-15` на `https://www.threads.net/@arianvzn` после этого изменения собрал `4` posts и `44` comments/replies, тогда как раньше detail path мог вернуть пустой `replies` при `article=0`;
- person-monitor smoke на этом же публичном surface больше не ломается на шаге profile -> detail handoff: внешний профиль остаётся найденным и реально отдаёт посты с replies;
- в raw detail payload этого smoke-run все `44` visible replies имели `reply_to_status_id == main_status_id`, то есть текущий public DOM на этом surface не дал ни одного подтверждённого non-root parent signal;
- дополнительная live DOM-проверка `2026-04-16` по `DXGnV_UDC9L`, `DXLKhKqCM-X`, `DXDj7POCJrS`, `DXAFgt4DBSc` подтвердила то же ограничение: `article_count=0`, visible rows есть, но у reply cards только собственный `/post/...` link, `nested_pressables=0`, одинаковые `data-*` keys и `non_root_targets=[]`;
- при этом root `person_monitor` report всё ещё может показывать `0 match_hits`, если найденные external posts и replies не содержат релевантного совпадения с наблюдаемым профилем. Это не регрессия extractor-а, а нормальный результат match layer.

Когда этот раздел нужно обновлять дальше:

- если появится надёжный DOM signal для nested reply parent-child связей;
- если authenticated browser path начнёт стабильно давать более богатый detail DOM и его придётся описать как отдельный enhancement path;
- если live smoke покажет, что fallback начал тащить в `message` новый тип UI noise, который текущая очистка не режет.

## Обновление: Threads profile URL matching в person_monitor

После усиления `threads_web` стало видно другой data-quality risk: Threads public web может отдавать ссылки как `threads.com`, а конфиг и часть collector output используют `threads.net`.

Проблема была в match layer:

- [src/social_posts_analysis/person_monitoring.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\person_monitoring.py) сравнивал profile URLs слишком буквально;
- `https://www.threads.com/@openai` и `https://www.threads.net/@openai` могли считаться разными профилями;
- из-за этого `profile_url_mention` и `authored_by_subject` могли не появиться в `match_hits`, хотя речь шла об одном Threads профиле.

Что изменено:

- `normalize_profile_url(...)` теперь канонизирует Threads hosts `threads.com`, `threads.net`, `www.threads.com`, `www.threads.net` к одному canonical host `threads.net`;
- `_contains_profile_url(...)` теперь проверяет набор допустимых URL-вариантов, чтобы raw text или permalink с `threads.com` совпадал с subject profile URL из конфига на `threads.net`;
- это изменение находится в `person_monitoring.py`, а не в `threads_web.py`, потому что это правило matching/data-quality, а не правило DOM extraction.

Почему не выбран другой путь:

- не выбран вариант переписывать все Threads URLs внутри collector-а, потому что raw payload должен сохранять фактические ссылки, которые отдал public DOM;
- не выбран fuzzy matching display names, потому что v1 person monitor должен оставаться точным: прямые URL/handle/id/alias signals без approximate name search.

Фактический эффект:

- authored activity и profile URL mentions для Threads теперь устойчивы к `threads.com`/`threads.net` доменному расхождению;
- для других платформ URL matching сохраняет прежнее поведение.

## Обновление: Instagram web raw_text для match quality

Следующий data-quality gap был найден в [src/social_posts_analysis/collectors/instagram_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\instagram_web.py).

Проблема:

- `person_monitoring.py` уже использует `PostSnapshot.raw_text` и `CommentSnapshot.raw_text` как fallback при поиске `profile_url_mention`, `handle_mention`, `profile_id_mention` и `alias_text_mention`;
- `instagram_web` сохранял только очищенный `text`;
- если Instagram public DOM отдавал handle/profile URL/alias в сыром block text, но cleaned `text` был пустым или неполным, `match_hits` мог не появиться.

Что изменено:

- `_extract_profile_payload(...)` теперь добавляет `raw_text` для post payload item;
- `_extract_post_payload(...)` теперь добавляет `raw_text` для comment payload item;
- `_build_posts_from_payload(...)` протаскивает `raw_text` в `PostSnapshot`;
- `_collect_comments_for_post(...)` протаскивает `raw_text` в `CommentSnapshot`.

Почему это сделано в collector-е:

- схема уже поддерживала `raw_text`, а match layer уже умел его использовать;
- проблема была в потере данных на extraction boundary;
- не выбран вариант писать `raw_text` в `message`, потому что это загрязнило бы отчёты UI-шумом.

Фактический эффект:

- `instagram_web` стал совместимее с person-monitor matching path;
- raw payload остаётся доступен для debug, а cleaned `message` остаётся отдельным полем.

## Обновление: Instagram web comment extraction и explicit discovery

После сохранения `raw_text` следующий gap был в том, что `instagram_web` всё ещё слишком широко выбирал DOM-узлы комментариев и не имел собственного search discovery path для `person_monitor`.

Что изменено:

- [src/social_posts_analysis/collectors/instagram_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\instagram_web.py) теперь использует strict comment candidates вместо старого широкого selector `ul ul, article ul ul li`;
- comment candidate принимается только при наличии содержательного raw text и хотя бы одного реального сигнала: author profile link, `time[datetime]` или explicit `data-comment-id`;
- `_normalize_comment_payload_item(...)` нормализует comment payload перед `CommentSnapshot`, сохраняет `raw_text`, убирает простые UI-noise строки вроде `Reply`, `See translation`, `1d`;
- `canonical_instagram_permalink(...)` убирает query/fragment и сохраняет canonical `/p/<id>/` или `/reel/<id>/`;
- `profile_url_from_name(...)` и `instagram_username_from_reference(...)` нормализуют Instagram profile URL к `https://www.instagram.com/<username>/`;
- `InstagramWebCollector.discover_person_monitor_sources(...)` поддерживает только explicit public profile discovery: `@username`, `username`, `https://www.instagram.com/username/`;
- [src/social_posts_analysis/person_monitoring.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\person_monitoring.py) теперь routes `platform="instagram" + collector.mode="web"` в этот discovery path.

Что принципиально не изменено:

- это не global Instagram search;
- generic text queries по Instagram web не ищут внешние surfaces и должны возвращать warning;
- nested comments не угадываются по layout: `reply_to_comment_id` используется только если DOM явно его отдал;
- normalized table names и public config schema не менялись.

Фактический эффект:

- `instagram_web` стал чище на comment extraction boundary;
- `person_monitor` теперь может использовать explicit Instagram profiles как search-discovery surfaces;
- watchlist и search discovery одного и того же Instagram profile дедуплицируются общим orchestrator flow.

Что подтверждено live:

- smoke-run `20260416T100506Z` с временным config `person_monitor + instagram_web` нашёл `2` observed surfaces: `nasa` через watchlist и `natgeo` через search query `@natgeo`;
- root report и exports были построены, включая `observed_sources.csv` и `match_hits.csv`;
- public Instagram profile DOM для `nasa` и `natgeo` отдал `posts: []`, поэтому итоговые `posts=0`, `comments=0`, `match_hits=0`;
- это считается внешним ограничением public Instagram DOM/login-wall surface, а не регрессией discovery path.

## Обновление: Instagram web profile-feed script fallback и diagnostics

После live smoke `20260416T100506Z` стало видно, что `instagram_web` discovery уже находит explicit external profiles, но сам profile-feed extraction может вернуть `posts: []` без достаточного объяснения причины.

Что изменено в [src/social_posts_analysis/collectors/instagram_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\instagram_web.py):

- `_extract_profile_payload(...)` оставляет старый DOM-anchor path первым источником post candidates;
- рядом добавлен fallback `script_posts`, который читает JSON из inline/script application-json блоков и рекурсивно ищет media objects с `shortcode`/`code`, caption, owner username, counts и timestamps;
- `_merge_profile_post_candidates(...)` дедуплицирует DOM и script candidates по `status_id` и выбирает более богатый вариант, если оба источника нашли один и тот же post;
- raw `profile_feed.json` теперь содержит `page_state` и `extraction_sources`, чтобы debug видел `dom_posts`, `script_posts`, `merged_posts`, `login_wall_detected`, `profile_unavailable_detected` и короткий `body_text_sample`;
- `_profile_payload_warnings(...)` добавляет явный warning, если Instagram вернул login/signup UI, unavailable profile surface или ноль post candidates.

Почему выбран именно этот путь:

- не выбран вариант менять public config schema: `authenticated_browser` уже есть в `collector.instagram_web`, поэтому новый код только лучше объясняет, что именно вернул текущий browser context;
- не выбран fake global search через Instagram UI или внешние поисковики: discovery остаётся explicit-profile only;
- не выбран вариант считать пустой DOM ошибкой collector-а: Instagram может реально скрыть public feed за login wall, поэтому это фиксируется как coverage warning и raw diagnostic signal.

Фактический эффект:

- если public или authenticated browser DOM содержит сериализованные media данные, collector может собрать posts даже без видимых `<a href="/p/...">` cards;
- если Instagram снова возвращает пустой profile DOM, report и raw manifest теперь явно показывают, это `dom_posts=0/script_posts=0`, login wall, unavailable page или другой empty-feed сценарий;
- normalized schema, table names, person-monitor matching rules и fuzzy matching не менялись.

## Обновление: Instagram web post-detail comment JSON fallback

Следующий gap был уже не в profile-feed, а в detail pages: Instagram может показать post или иметь comment counter, но не отдать comment DOM в стабильных `ul li` блоках.

Что изменено в [src/social_posts_analysis/collectors/instagram_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\instagram_web.py):

- `_extract_post_payload(...)` оставляет strict DOM comment candidates как первый источник;
- рядом добавлен `script_comments` fallback, который читает inline/application-json scripts и рекурсивно ищет comment-like objects с `id/pk/comment_id`, `text/body/message/comment_text` и `owner/user/author/from.username`;
- `_merge_comment_candidates(...)` объединяет DOM и JSON comments по `comment_id`, выбирая более богатый payload, если один и тот же comment найден двумя путями;
- `reply_to_comment_id` переносится только из явных DOM/JSON полей вроде `data-parent-comment-id`, `parent_comment_id`, `parent_comment.id`, `replied_to_comment_id`; layout-based nested inference не добавлялся;
- raw detail payload теперь содержит `comment_extraction_sources` и `page_state`, чтобы было видно `dom_comments`, `script_comments`, `merged_comments`, login wall и наличие serialized comment data;
- `_post_payload_warnings(...)` добавляет явный warning, если post имел `comments_count > 0`, но DOM и script fallback вместе вернули `comments: []`.

Почему выбран именно этот путь:

- не выбран вариант расширять selector обратно до широкого `ul ul`, потому что он уже давал UI-noise и ложные comment blocks;
- не выбран fuzzy/nested inference по визуальному layout, потому что Instagram DOM не гарантирует стабильный parent signal;
- не выбран вариант считать empty comments ошибкой run-а: public Instagram может скрывать comments за login wall или не сериализовать их в HTML.

Фактический эффект:

- `person_monitor` получает больше шансов увидеть mentions/authored activity в Instagram comments, если они есть в serialized page data;
- если comments всё равно скрыты, raw payload и manifest warnings теперь явно показывают, что именно было доступно extractor-у;
- config schema, normalized tables и match rules не менялись.

## Live smoke: Instagram web public-only после JSON fallback

После profile-feed и post-detail JSON fallback был запущен public-only smoke `20260416T110934Z` для `person_monitor + instagram_web`.

Конфигурация smoke:

- subject profile: `https://www.instagram.com/openai/`;
- watchlist surface: `https://www.instagram.com/nasa/`;
- search query: `@natgeo`;
- authenticated browser: disabled;
- output root: `%TEMP%\spa_instagram_web_smoke_after_comment_fallback`.

Результат:

- root status: `partial`;
- observed surfaces: `2`;
- collected posts: `0`;
- collected comments: `0`;
- match hits: `0`;
- warnings: `4`;
- exports были построены, включая `observed_sources.csv`, `match_hits.csv`, `matched_posts.csv`, `matched_comments.csv`, `source_warnings.csv`.

Raw diagnostics по surfaces:

- `watchlist-nasa`: `login_wall_detected=true`, `dom_posts=0`, `script_posts=0`, `merged_posts=0`, `serialized_data_detected=false`;
- `search-natgeo`: `login_wall_detected=true`, `dom_posts=0`, `script_posts=0`, `merged_posts=0`, `serialized_data_detected=false`;
- обе страницы отдавали только public profile shell с `Log In`, `Sign Up`, profile metadata и related accounts, но без post/detail candidates.

Вывод:

- последние fallback-и работают как диагностический и extraction path, но public Instagram в этом smoke не отдал данных, на которых они могли бы сработать;
- это не regression в `person_monitor` discovery: surfaces `nasa` и `natgeo` были найдены и отражены в `observed_sources`;
- следующий полезный шаг не selector tuning, а authenticated-browser smoke для `collector.instagram_web.authenticated_browser` с явно выбранным logged-in browser profile.

## Live smoke: Instagram web authenticated browser

После public-only smoke был проверен authenticated-browser path для `instagram_web`.

Что изменено перед повторным запуском:

- [src/social_posts_analysis/collectors/instagram_web.py](C:\Coding projects\facebook_posts_analysis\src\social_posts_analysis\collectors\instagram_web.py) теперь передаёт `best_effort_profile_copy=True` в общий `open_web_runtime(...)`;
- это сделано по той же причине, что и для Facebook web collector: полный Chrome profile может быть большим или содержать locked/cache файлы, а smoke не должен зависать на копировании несущественных browser cache artifacts;
- public config schema не менялась.

Фактический smoke:

- первый authenticated attempt с Chrome `Default`, двумя surfaces и обычным copy profile не уложился в `5` минут и был остановлен timeout-ом до raw результата;
- после best-effort copy был запущен scoped smoke `20260416T112419Z`: Chrome `Default`, `copy_profile=true`, one watchlist surface `nasa`, search disabled;
- run завершился за примерно `172` секунды;
- root status: `partial`;
- observed surfaces: `1`;
- collected posts: `0`;
- collected comments: `0`;
- match hits: `0`;
- warnings: `4`.

Raw diagnostics:

- runtime подтвердил snapshot: `Using authenticated browser profile snapshot from ... Google\Chrome\User Data (Default)`;
- `login_wall_detected=true`;
- `dom_posts=0`, `script_posts=0`, `merged_posts=0`;
- `serialized_data_detected=false`;
- body sample снова начинался с `Log In`, `Sign Up`, profile metadata и related accounts.

Вывод:

- authenticated runtime теперь технически запускается и не зависает на полном копировании профиля;
- текущий Chrome `Default` не снимает Instagram login wall для `nasa`;
- следующий шаг должен быть не selector tuning, а проверка реально logged-in Instagram browser profile: другой Chrome/Edge profile, `headless=false`, либо ручная подготовка отдельного profile directory, где Instagram точно открыт как logged-in session.
