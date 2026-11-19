# DataPilot｜指标口径与受控问数 Agent

面向模拟电商经营分析的完整前后端 Agent：自然语言 → 指标规格确认 → 模型生成 SQL → 受控只读查询 → 口径、SQL、表格与图表。支持一次澄清、追问、基期比较、Top K、取消、历史与 SSE 状态同步。

技术栈：Python 3.12、FastAPI、LangGraph、SQLGlot、PostgreSQL、Celery、Redis、React、TypeScript。

## 业务范围

五表：customers、orders、order_items、products、refunds。五指标：支付商品金额、支付订单数、成功退款商品金额、期间商品净流入额、支付客单价。固定 all/east 权限，时间窗采用上海时区左闭右开区间，两期分别约束聚合和关联粒度。

SQL 执行受 AST、视图权限和指标契约校验；使用只读 REPEATABLE READ、2 秒语句超时和结果上限。全局最多 2 个活动任务、10 个排队任务。EXPLAIN 记录估算，尚无经校准的成本拒绝阈值。

## 环境与配置

安装 Git、Python 3.12 和 Docker Compose v2；Docker 构建阶段自行安装 Python/Node 依赖。本机额外安装 uv 用于安全配置工具：

```sh
python -m pip install uv==0.13.7
uv sync --locked --no-dev --no-install-project
uv run --locked python scripts/init-local-env.py
```

工具生成被 Git 忽略的 `.env.local`，内含随机数据库密码、会话密钥和演示账号密码，不覆盖已有文件、不输出秘密。ZIP 下载用户先在解压目录执行 `git init`；通过 git clone 获取则无需这一步。

API 密钥只保存在本机 `.env.local`。使用隐藏输入配置：

```sh
uv run --locked python -m scripts.configure-deepseek --env-file .env.local --set-key
```

然后在本机 `.env.local` 设置：

```dotenv
COMPOSE_PROJECT_NAME=datapilot
DP_MODEL_MODE=live
DP_QUERY_PROCESSING_ENABLED=1
```

默认配置关闭模型调用和查询处理。启用后提交问题会调用 DeepSeek 并产生费用；模型名由 `.env.local` 中的 `DP_MODEL_NAME` 配置，需供应商账号实际支持。不要将密钥、数据库密码、证书私钥或该环境文件提交到 Git。

## 启动

在仓库根目录执行：

```sh
docker compose --env-file .env.local -f apps/datapilot/compose.yaml up -d --wait postgres redis
docker compose --env-file .env.local -f apps/datapilot/compose.yaml --profile manage run --rm --build manage initialize --dataset demo_v1
docker compose --env-file .env.local -f apps/datapilot/compose.yaml --profile application --profile processing up -d --build api web worker control
```

打开 http://localhost:18081 。可用账号 `east_a`、`east_b`、`all_a`、`report_admin`；密码是本机 `.env.local` 的 `DP_SEED_PASSWORD`。报告管理员没有隐式问数权限；历史与评测页不会预载源项目的私有评测报告。

`demo_v1` 是 6 客户、9 商品、17 订单、19 商品行、12 退款的合成数据。2026 年 10 月华东五指标基线为：214.69 元、7 笔、136.00 元、78.69 元、30.67 元。可问：

- 2026年10月华东各品类支付客单价是多少？
- 2026年10月华东每周各品类期间商品净流入额是多少？与2026年9月同品类全期汇总相比如何？
- 2026年10月华东净额是多少？（进入口径澄清）

首次初始化可选择 `tiny_v1` 或 `demo_v1`；已有数据版本不同会拒绝替换，同版本重复初始化保留账号、源数据和历史。已有库先停止接单、排空任务并停止应用，再运行 `manage upgrade`，不通过删卷解决版本冲突。

## 停止和 HTTPS

先关闭接单、等待活动任务和队列完成，再停止服务；保留卷：

```sh
docker compose --env-file .env.local -f apps/datapilot/compose.yaml --profile application --profile processing stop
```

HTTPS 可增加 `apps/datapilot/compose.deploy.yaml`：用 `scripts/configure-deploy.py` 创建独立忽略文件 `.env.deploy`，配置精确 HTTPS Origin 和本机证书目录（fullchain.pem / privkey.pem），随后使用同一 env-file 和两个 compose 文件初始化及运行。不要上传证书私钥。公网部署需配置有效证书和访问边界。

## 源码说明与验证边界

本仓库是运行源码导出，包含前后端、迁移、锁定依赖、两份合成数据及配置工具。运行代码取自 2026-11-15 验证快照，2026-11-19 导出；Compose 仅保留运行服务。源项目原有评测目录、测试工具、日志、本机环境和 Git 历史未纳入。

源快照本机记录：1777 项 Python 测试、8 项前端状态测试、1 个原生浏览器场景、19 项安装检查通过；真实模型 12/12 已见场景通过；固定 SQL 容量 576/576、600 秒稳态 889/889 与独立源行 oracle 一致。这些是源项目历史结果，本运行包不附原始评测材料，不能在本包重跑全部验收。

原生浏览器使用 stub，真实模型场景通过 HTTP/API/worker/SSE 单独验证；容量与稳态 provider0。AI 编写/审核、human_reviewed=false，未完成独立盲测、新宿主/hosted CI 和标准 P95。模拟业务不代表生产质量或客户收益。普通执行不在线读取 oracle，独立源行计算属于源项目测试与评测。
