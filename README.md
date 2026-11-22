# DataPilot · 指标口径与受控问数 Agent

DataPilot 让分析员用中文查询模拟电商经营数据，并核对每个数字是怎么算出来的。比如问「十月华东净额是多少」，系统可以先请用户确认「期间商品净流入额」。九月支付、十月才退的钱会计入十月退款。在随仓库提供的 `demo_v1` 里，十月华东支付 **214.69 元**，成功退款 **136.00 元**，净流入 **78.69 元**。[演示源数据](fixtures/datapilot/demo_v1.source.json)

工作台展示已确认的指标、时间窗、权限范围、SQL、精确数表和图表。用户可以在同一条查询里回答澄清，也可以基于已确认的规格追问「改看十一月」，随后在历史页分别打开两次结果。

## 能问什么

| 经营问题 | 计算口径 |
| --- | --- |
| 支付商品金额是多少？ | 按支付成功时间，合计商品明细实付金额 |
| 有多少笔支付订单？ | 在指定范围内按订单 ID 去重 |
| 成功退款商品金额是多少？ | 按退款成功时间，合计成功退款金额 |
| 期间商品净流入额是多少？ | 本期支付额减本期成功退款额，保留当期只有退款的分组 |
| 支付客单价是多少？ | 商品实付金额除以去重支付订单数，零元订单和没有订单分别处理 |

五张业务表对应客户、商品、订单、订单明细和退款。每次查询支持一个指标，可按品类或销售区域分组，按日、周、月或全期汇总，并做基期比较和全期 Top K。日期采用上海时区。[指标目录](apps/datapilot/catalog/metrics.v2.yaml) · [表结构与关联](apps/datapilot/catalog/schema.v1.yaml)

## 一条查询怎样执行

1. **确认口径。** 模型把问题解析成 `QuerySpec`。服务端核对指标、时间窗、比较方式和账号权限，遇到指标歧义可以暂停等待用户选择。确认后只向模型提供所需表列和许可的关联路径。[规格确认](datapilot/specification.py) · [LangGraph 工作流](datapilot/pipeline/graph.py)
2. **分别约束两期。** 例如「十月每周各品类净流入，与九月同品类整月相比」。支付和退款在十月按周和品类分别聚合，在九月只按品类聚合。九月输出的时间列是 `NULL` 占位，因此只能用品类关联两侧的聚合结果。[两期 SQL 形状](datapilot/model/prompts.py)
3. **检查并执行 SQL。** SQLGlot 校验语法树、列来源和授权对象，指标守卫再检查事件时间、筛选、分组与金额或订单数组件。PostgreSQL 通过授权视图，在同一只读快照内读取两期数据，每条语句限时 2 秒。执行前记录 EXPLAIN 估算，成本拒绝阈值尚未校准。[结构校验](datapilot/sql_guard.py) · [指标校验](datapilot/metric_guard.py) · [数据库执行器](datapilot/executor.py)
4. **返回可复核的结果。** FastAPI 接收命令，Celery 与 Redis 异步投递和处理任务。React 工作台通过 SSE 跟踪状态，读取最终快照并展示结果。本期和基期分别保留计算组件，全期客单价用全期金额和订单数重算。[任务服务](datapilot/tasks/service.py) · [结果计算](datapilot/results.py) · [前端工作台](apps/datapilot/web/src/workbench.tsx)

## 本地运行

需要 Git、Python 3.12 和带 Compose v2 的 Docker（Linux 容器）。以下命令在克隆后的仓库根目录执行。若下载的是 ZIP，先在解压目录运行 `git init`，以便本地配置工具检查凭据文件已被 Git 忽略。

### 初始化并浏览工作台

```sh
python scripts/init-local-env.py
docker compose -p datapilot --env-file .env.local -f apps/datapilot/compose.yaml up -d --wait postgres redis
docker compose -p datapilot --env-file .env.local -f apps/datapilot/compose.yaml --profile manage run --rm --build manage initialize --dataset demo_v1
docker compose -p datapilot --env-file .env.local -f apps/datapilot/compose.yaml --profile application up -d --build api web
```

打开 [本地工作台](http://localhost:18081)，可先登录浏览指标目录。华东分析员为 `east_a`、`east_b`，全域分析员为 `all_a`，评测报告管理员为 `report_admin`。账号密码由初始化工具随机生成，保存在本机 `.env.local` 的 `DP_SEED_PASSWORD` 中。首次初始化可选 `demo_v1` 或 `tiny_v1`，已有库再次初始化时会核对原数据版本。[初始化工具](scripts/init-local-env.py)

### 启用真实模型问数

运行以下命令，在隐藏提示中输入自己的 DeepSeek API 密钥。模型调用使用提供方账户额度。本地工具会把模型模式设回 `none`，所以在录入密钥后再设置开关。

```sh
python scripts/configure-deepseek.py --env-file .env.local --set-key
```

在本机 `.env.local` 中将 `DP_MODEL_MODE` 改为 `live`、`DP_QUERY_PROCESSING_ENABLED` 改为 `1`，再启动问数服务。当前网关使用 `deepseek-flash`，需要账号具有该模型的访问权限。[模型网关](datapilot/model/deepseek.py)

```sh
docker compose -p datapilot --env-file .env.local -f apps/datapilot/compose.yaml --profile application --profile processing up -d --build api web worker control
```

用 `east_a` 问「2026 年 10 月华东净额是多少」，选择「期间商品净流入额」后可核对 **78.69 元**。再试「2026 年 10 月华东各品类支付客单价是多少」，体育品类的两笔订单实付 2.35 元和 2.34 元，客单价保留两位小数后为 **2.35 元**。[演示数据](fixtures/datapilot/demo_v1.source.json)

### 停止与 HTTPS

结束使用前先等待活动和排队任务完成，再停止服务并保留数据卷。

```sh
docker compose -p datapilot --env-file .env.local -f apps/datapilot/compose.yaml --profile application --profile processing stop
```

HTTPS 部署使用独立的本机 `.env.deploy`、精确的 HTTPS Origin 和本机证书目录。生成部署配置后，使用对应 env 文件并同时加载基础与部署两个 Compose 文件。[配置工具](scripts/configure-deploy.py) · [HTTPS Compose](apps/datapilot/compose.deploy.yaml)

## 验证记录

下表来自完整开发工程在 2026 年 11 月 15 日对 v6 模拟业务的本机验收。评分端按原始订单、明细和退款记录独立计算预期结果。普通问数服务只访问授权数据，评分程序和原始实验记录保存在开发工程中。

| 验证对象 | 结果与适用范围 |
| --- | --- |
| 真实模型问数 | 12 个预设场景符合预期。其中 11 个查询的规格和结果都正确，1 个越权请求被拒绝。通过 API、worker 和 SSE 联调，共 24 次模型调用。尚无独立盲测准确率。 |
| 固定 SQL 容量 | 1 万及 10 万订单、两种分布、两档并发等共 96 个配置。480 次正式执行与 96 次预热均与源行对账一致。 |
| 固定 SQL 稳态 | 600 秒内 889/889 个任务结果正确。容量与稳态均未调用模型。 |
| 软件与安装 | 1,777 个 Python 测试节点、8 项前端状态测试、19 项安装检查通过。另有 1 个使用模型替身的原生浏览器场景，覆盖登录、问数和三页联动。 |

本仓库分发普通运行所需的前后端、数据迁移、锁定依赖、两份合成数据、配置工具和前端状态测试。源项目记录为 AI 编写与审核，尚无独立人工验收。前端可在 Node 22.12 至 24.x 环境中本地验证。[前端依赖与命令](apps/datapilot/web/package.json)

```sh
cd apps/datapilot/web
npm ci
npm run test:state
npm run build
```
