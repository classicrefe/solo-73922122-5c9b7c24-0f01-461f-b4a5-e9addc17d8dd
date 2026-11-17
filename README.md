# 机构库书目采集（OAI-PMH ListRecords / oai_dc）

一套可实际启动的采集演示：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集网页 | http://localhost:8000 | 目录浏览/搜索、进度、开始/取消/取消并重开、轮次记录、采集溯源 |
| 演示 OAI 源 | http://localhost:8001 | OAI-PMH 端点 `/oai`，控制面板 `/`（慢页、令牌失效、撤回等） |

```bash
docker compose up --build
```

首次启动后打开采集网页，点“开始新一轮”即为首次全量采集（无 `from`，`until` 为源端 `Identify` 返回的当前 UTC 秒）。之后每轮都以“上次成功水位 → 源端当前时刻”的闭区间取数。

## 业务约定与实现

### 时间窗
- 每轮开始时调用 `Identify`，把 `<responseDate>` 规范化为秒级 UTC，作为 `until`。
- `from` 固定取“上次成功提交的水位”；首次全量为 `null`，请求中不带 `from`。
- 两端都是闭区间（OAI `from`/`until` 语义）。边界那一秒会被重复读到，但暂存区
  对同一标识只保留一条、提交写入是幂等的，因此不会产生重复或丢数据。
- 若源端当前时刻早于已发布水位（时钟回拨），本轮直接失败，不构造倒置时间窗。

### 分页与令牌
- 每轮从窗口第 1 页开始，沿 `resumptionToken` 取到没有令牌为止。
- 令牌请求只带 `resumptionToken`，不再带 `from`/`until`，分页严格绑定最初的时间窗。
- 一旦源端返回 `badResumptionToken`（过期/注入失效），或分页途中
  `completeListSize` 发生变化（结果集在翻页时变动会让游标分页漏记录）：
  1. **整次尝试的暂存记录全部删除**（`run_records` 按 `run_id` 清空），
     不会把已取到的旧分页与新结果混在一起；
  2. 用**原来的 `from`/`until`** 从第 1 页重新取；
  3. 最多 `HARVEST_MAX_ATTEMPTS`（默认 3）次；仍失败则整轮失败、不发布。
- 记录时间戳必须落在窗口内；越窗记录会让本轮失败（宁可不发布，也不污染目录）。
  分页中途出现 `noRecordsMatch` 同样按协议异常处理。

### 一条标识一条书目，撤回即删除
- 源端 `header/@status="deleted"` 的记录会从已发布目录中删除对应标识。
- 暂存表主键是 `(run_id, identifier)`，同一轮内重复出现时保留 `datestamp` 更新的
  一条（相同时间戳保留后出现的）。
- 成功提交时对非删除记录做 UPSERT，因此“同一标识只保留一条”在目录表层面也成立。

### 提交的原子性
- `commit_run()` 在**一个 `BEGIN IMMEDIATE` 事务**里：校验本轮仍是活动轮次 →
  把暂存记录写入/删除 `catalog` → 更新 `published_state.watermark` → 标记本轮成功
  → 清理暂存。任何一步抛错都会 `ROLLBACK`，目录、水位、成功计数都不变。
- 失败、取消、进程崩溃都不会改变已发布内容：
  - 失败/取消：`finish_failed` / `finish_cancelled` 只改本轮状态并清空暂存；
  - 进程崩溃：下次启动时 `Store` 把所有残留的 `running` 轮次标记为失败并清空暂存，
    同时发布状态完全不动。

### 采集溯源（原文存档）
排查误更正或撤回时，需要回查当时收到的源响应。启用溯源后：

- **原文存卷、索引存库**：每个 `ListRecords` 响应页的**原字节**写入持久卷
  `<DATA_DIR>/raw/pages/<run_id>/attempt-<尝试>/page-<页号>.xml`
  （临时文件 + fsync + 原子改名 + 目录 fsync）；SQLite 只存索引
  （`run_pages` 页索引：路径/SHA-256/字节数/状态，`provenance` 依据：
  标识 → 轮次/尝试/页号/页内位置/datestamp/撤回标记）。
- **依据与目录同规则**：暂存去重沿用“同标识取最新、同秒取后出现”，
  依据记录的就是每轮最终采用的那一条及其出处；令牌失效重取时，
  废弃尝试的页被标记 `abandoned`，不会混入依据。
- **共同发布**：一轮的全部页原文落盘齐备后，目录 UPSERT、依据插入、
  页状态置 `published`、水位推进在**同一个事务**里提交；写盘失败本轮失败、
  发布中途进程退出时 SQLite 回滚——重启后只能看到完整的旧版或新版。
- **回收**：失败、取消轮次的全部页与废弃尝试的页由 sweeper 回收
  （轮次结束时在线程中异步执行，启动时续清）；`running` 轮次与
  `published` 页永不触碰，已发布历史不会被误删。
- **慢盘不阻塞**：写盘/清理都在工作线程执行，查询与取消只走 SQLite；
  取消并立即重开后，旧轮的迟到写入只会落在旧轮自己的目录并被随后回收，
  不影响新轮与已发布原文。
- **溯源页**（`/provenance`）：按标识查看启用后各成功轮次最终采用的记录
  （轮次、尝试、页号、页内位置、采用/撤回），查看该页记录清单与 XML 原字节，
  下载原字节及 `.sha256`；无查询时展示最近撤回历史。启用溯源前采集的
  旧目录在目录页与溯源页都标明“无原文”。
- 只有成功轮次最终采用的页可查看/下载（`state='published'`），
  进行中和废弃的页不对外暴露。

### 取消：不等待慢页
- `POST /api/harvest/cancel`：先在 SQLite 里把该轮置为 `cancelled` 并清空暂存（本地事务，
  毫秒级），再 `task.cancel()` 让正在等待的 HTTP 读取立刻放弃——**不等待慢页返回**。
- `POST /api/harvest/restart`（网页上的“取消并立即重开”）：完成上述取消后立即创建
  带新 `run_id` 的新轮次，无需等旧任务退出。
- 旧轮即便迟到：所有写入都以 `run_id` 为键；提交事务会再次校验
  `active_run_id == run_id` 且状态仍为 `running`，否则直接 `ROLLBACK` 拒绝发布，
  更不会触碰新轮次的状态/进度。
- 数据库层还有 `WHERE status='running'` 的唯一索引，跨进程也最多一个活动轮次。

### 进度可见性
- 网页每秒轮询 `/api/status`：阶段、时间窗、已取页数、已取记录 / `completeListSize`、
  令牌失效重取次数、当前尝试次数、已暂存唯一标识数、最近错误。
- 前端轮询用“代号”防止旧响应覆盖新渲染；轮次开始/结束时整页刷新，使目录和水位
  立即反映已发布结果。

## 演示源的能力
- `Identify` / `ListRecords`（`metadataPrefix=oai_dc`）/ `ListMetadataFormats`；
- 秒级 UTC `datestamp`，闭区间 `from`/`until`，`resumptionToken` 分页；
- 控制面板（`http://localhost:8001/`）：
  - **慢页**：`slow_seconds`，每个响应延迟 N 秒（取消演示）；
  - **令牌失效**：`token_failures`，接下来 N 次令牌调用返回
    `badResumptionToken`，并使所有未用令牌作废（重取演示）；
  - 分页大小、令牌 TTL（设为 0 可让令牌立即过期）；
  - 新增、更正（更新 `datestamp`）、撤回（`status="deleted"`）、恢复、撤回全部。
- 演示数据保存在独立 SQLite 卷 `demo-data`，采集网页保存在 `harvester-data`。

## 配置（docker-compose.yml）
| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `OAI_URL` | `http://demo-oai:8001/oai` | 源端地址 |
| `OAI_READ_TIMEOUT` | `20` | 单页读取超时（秒） |
| `HARVEST_MAX_ATTEMPTS` | `3` | 令牌失效后最多重取次数 |
| `HARVEST_RETRY_BACKOFF_MS` | `300` | 重取前等待 |
| `RAW_DIR` | `$DATA_DIR/raw` | 原文页存档目录（持久卷内） |
| `DEMO_PAGE_SIZE` | `3` | 演示源初始分页大小 |
| `APP_PORT` / `DEMO_OAI_PORT` | `8000` / `8001` | 宿主机端口 |

## 本地开发与测试
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

DATA_DIR=./data uvicorn demo_oai.main:app --port 8001
DATA_DIR=./data OAI_URL=http://127.0.0.1:8001/oai uvicorn app.main:app --port 8000

pytest -q
```
测试使用 `httpx.ASGITransport` 在同一进程内调用真实的演示 FastAPI 应用，
覆盖 XML 解析、时间窗、分页/令牌失效重取、撤回、原子提交、取消/重开、
迟到响应、进程崩溃恢复，以及溯源（原文落盘、依据定位、废弃/失败/取消页回收、
启动续清、慢盘下取消与重开隔离、旧目录“无原文”标记、页面查看与下载）。
