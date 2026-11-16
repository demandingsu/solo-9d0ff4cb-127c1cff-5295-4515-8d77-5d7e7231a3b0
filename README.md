# 沿海潮位卫星短报文采集

沿海潮位站经卫星短报文回传分钟潮位，网关侧会留下**拆段、乱序、重投**的 NDJSON
记录。本服务（FastAPI + SQLite）按 `站号 + 设备启动号 + 报文号` 归组报文段，
收齐后按段号拼接字节、校验整报 SHA-256，解析其中的 UTC 分钟潮位 CSV 并发布，
供浏览器查询与下载。

## 快速启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据库持久化在命名卷 `tide-data`（容器内 `/data/tide.db`），重启不丢状态。

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload          # 默认库文件 ./data/tide.db，可用 DB_PATH 覆盖
```

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest
```

## 使用流程

1. 浏览器打开 `http://localhost:8000`，在「1. 上传」选择 NDJSON 文件；
2. 在「2. 应收清单与补传」填写站号、启动号并上传岸台清单（同站同启动号**整版替换**），
   随后可随时「下载补传 CSV」向网关催传缺失数据；
3. 「3. 报文状态」列出每个报文的已收/总段数、**缺段清单**、状态与**待核原因**；
4. 「4. 已确认观测」预览并可按站号过滤、下载 CSV —— 只含组装校验通过的完整报文；
5. 待核报文处理完后，点「复位重采」清除该报文，等待岸台重新上送。

命令行等价操作：

```bash
curl -F "file=@samples/sample_upload.ndjson" http://localhost:8000/api/upload
curl -F "file=@samples/sample_manifest.csv" http://localhost:8000/api/manifests/101/7
curl -OJ "http://localhost:8000/api/retransmit.csv?station_id=101&boot_id=7"
curl http://localhost:8000/api/messages
curl -OJ "http://localhost:8000/api/observations.csv?station_id=101"
```

## NDJSON 记录格式

每行一个 JSON 对象：

| 字段 | 类型 | 说明 |
|---|---|---|
| `station_id` | int | 站号 |
| `boot_id` | int | 设备启动号 |
| `msg_id` | int | 报文号 |
| `seg_no` | int | 段号，1..seg_total |
| `seg_total` | int | 总段数（≥1） |
| `payload_b64` | string | 本段正文的 Base64 |
| `sha256` | string | **整报**（全部段按段号拼接）的 SHA-256 十六进制 |

整报正文为 UTF-8 的分钟潮位 CSV：每行 `YYYY-MM-DDTHH:MM:00Z,<潮位米>`，
允许空行与 `#` 注释行（样例首行即注释）。观测按 `(站号, 分钟)` 唯一。

## 应收清单与补传 CSV

岸台把每个 `(站号, 启动号)` 的**应收报文清单**交给监测员，监测员上传后据此生成
补传请求，向网关催传缺失数据。

**清单格式**（UTF-8 文本，每行一条）：

```
msg_id,seg_total,sha256
1003,3,a5734af2…        # 报文号,总段数,整报SHA-256（允许表头、空行与 # 注释）
```

- 同一 `(站号, 启动号)` 上传新清单即**整版替换**旧清单（单事务：先删后写，
  失败整体回滚）；不同站/启动号的清单互不影响；
- 清单**整体校验**（列数、整数、总段数 ≥ 1、64 位十六进制摘要、报文号不重复、
  至少一条），任一错误即拒绝并**保留旧清单**。

**补传 CSV**（`GET /api/retransmit.csv?station_id=&boot_id=`，按清单条目逐行）：

```
station_id,boot_id,msg_id,request,segments,seg_total,sha256,reason
101,7,1003,segments,2,3,a5734af2…,缺段
101,7,1004,message,,2,913507cf…,报文未到
```

| 情形 | 请求 |
|---|---|
| 报文未到 / 待核 / 登记的总段数或摘要与清单不符 | `message`（整报） |
| 登记一致但尚缺段 | `segments`（`segments` 列为 `;` 分隔的缺段号） |
| 已发布且登记与清单一致（完整匹配） | 不请求 |

只按当前清单条目逐条主键核查，不扫描历史报文；迟到段、冲突、复位等状态变化
在下次下载时即反映（每次下载基于同一已提交快照计算）。

## 一致性与并发语义

- **重投幂等**：完全相同的段只入库一次；观测按 `(站号, 分钟)` 去重，
  重复上传同一文件不会产生重复数据。
- **乱序**：段按归组键随意到达，收齐即组装。
- **待核（pending_review）**：同一报文出现以下任一情况即转待核，并**立即撤下
  该报文已发布的观测**——段内容冲突（同段号不同正文）、总段数冲突、整报摘要
  冲突、段号越界、组装后摘要与登记摘要不符、CSV 解析失败。待核报文后续到达
  的段一律忽略，复位后方可重新采集。
- **并发**：上传在写锁 + 单事务（`BEGIN IMMEDIATE`）内串行落库，WAL 模式 +
  `busy_timeout` 保证多请求/多进程下不错乱；一批上传要么全部生效要么回滚。
- **清单**：整版替换同样在写锁 + 单事务内完成，校验或保存失败保留旧清单；
  补传 CSV 在写锁 + 单读事务内计算，并发上传、替换与下载不会混入未提交
  数据或不同清单版本。
- **重启**：全部状态在 SQLite 中，观测只在报文校验通过后才写入，因此重启后
  查询天然只显示完整报文的观测，重投依旧幂等。

## 样例文件

- `samples/sample_upload.ndjson` —— 乱序 + 重投的正常样例：报文 1001（4 段）、
  1002（3 段）可发布；1003 缺第 2 段，网页「缺段」列显示 `2`。
- `samples/sample_conflicts.ndjson` —— 冲突样例：段内容冲突（2001）、总段数
  冲突（2002）、组装摘要校验不符（2003）、**先发布后收到篡改段而撤下观测**
  （2004）、整报摘要冲突（2005）。
- `samples/sample_manifest.csv` —— 站 101/启动 7 的应收清单：1001–1003 与上传
  样例登记一致（1003 只缺段），1004 未到（补传 CSV 请求整报）。
- `python3 samples/generate_samples.py` 可重新生成以上文件。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 采集网页 |
| POST | `/api/upload` | 上传 NDJSON（multipart 文件 `file` 或裸文本），返回逐行结果 |
| GET | `/api/messages` | 全部报文：已收段、缺段、状态、待核原因、观测数 |
| GET | `/api/observations?station_id=&limit=` | 已确认观测 JSON |
| GET | `/api/observations.csv?station_id=` | 已确认观测 CSV 下载 |
| DELETE | `/api/messages/{station}/{boot}/{msg}` | 复位报文（删除其段与观测） |
| POST | `/api/manifests/{station}/{boot}` | 上传应收清单（multipart 文件 `file` 或裸文本），整版替换 |
| GET | `/api/manifests` | 全部清单版本：条目数、更新时间 |
| GET | `/api/manifests/{station}/{boot}` | 当前清单明细 |
| GET | `/api/retransmit.csv?station_id=&boot_id=` | 补传请求 CSV 下载 |

## 目录结构

```
app/
  main.py            FastAPI 路由与生命周期
  core.py            归组、组装、校验、解析、待核/撤下逻辑
  db.py              SQLite 连接与 schema
  static/index.html  采集网页
samples/             样例生成器与样例 NDJSON
tests/               端到端测试（乱序/重投/冲突/撤下/重启/并发）
Dockerfile, docker-compose.yml
```
