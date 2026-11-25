# 怎样核对 DataPilot 的验收数字

在仓库根目录用 Python 3.12 运行下面的命令。它会打开已提交的真实模型结果，用公开的 `demo_v1` 订单及退款明细重算 11 道成功查询，再核对第 12 题的权限拒绝。随后会按原始执行的配置、重复序号和结果汇总固定 SQL 实验。全程无需 PostgreSQL、Redis 或模型密钥。Windows 使用 `uv run python docs/verification/check.py` 可取得锁文件中的 IANA 时区数据。

```sh
python docs/verification/check.py
```

要运行仓库本身的 Python 回归，请在根目录执行 `uv run pytest`。下面的 1,777 个节点等数字来自 2026-11-15 完整开发工程的本机记录。当前 checkout 的测试数以 pytest 当次收集的结果为准。[核账脚本](verification/check.py) · [独立源行算法](../tests/datapilot/semantic_oracle.py)

## 哪道题让首轮停在 11/12？

在 [逐题结果](verification/model-cases.json) 搜索 `deny-west`。华东账号要求查询西部，两次运行都被拒绝。首轮唯一差异是期望的内部异常名 `SCOPE_FORBIDDEN` 和实际公开错误码 `FORBIDDEN_SCOPE`。第二轮在调用前固定公开错误码，将同样的 12 题各自重新提交一次，11 道题的规格和源行结果都正确，还有 1 次预期拒绝、数据库尝试 0 次。每题可看到模型阶段、最终状态和 SQL 修正次数。第二轮共 24 次模型调用，SQL 自动修正 0 次。

可以先翻 `clarify-net`。题目是“2026 年 10 月华东净额是多少？”，记录了等待澄清后的再次解析和最终展示结果 78.69 元。核账脚本依据每题调用前冻结的规格从源数据重算，逐项比较整期值、时序和 Top K 顺序。规格中的参考时间在提交时由服务端赋值，核对其余规格字段时使用调用前的原值。两个批次使用过这些题，尚无独立盲测准确率。[调用前冻结的 12 份预期](verification/expected-precall.json) · [演示数据](../fixtures/datapilot/demo_v1.source.json)

## 十万订单的超时是怎样从 20 次降到 0 次的？

[固定 SQL 运行记录](verification/sql-runs.json) 的 `capacity.wide_aov_before_after` 有 20 对正式执行。同一配置、同一重复序号的两条记录并排出现，覆盖十万订单下的均匀和偏斜两种分布、并发 1 和 2、各 5 次。两批都保留 PostgreSQL 2 核、2 GiB、单条语句 2 秒上限。原批次这道宽窗客单价查询全部超时，修正事务内连接计划偏好后全部完成。另有 4 次预热超时，未计入这 20 对。

展开 `capacity.configurations` 可以找到 96 个配置，每个配置 1 次预热加 5 次正式执行。查询规模为 1 万或 10 万订单，另分布、并发、时间窗各两档，固定 SQL 六条。正式 480/480 正确，另 96/96 次预热正确。每个配置带冻结预期的摘要，每次执行带数据库的原结果摘要、行数、源行比较标记及实际业务字段摘要。摘录时从原生结果里重新取出整期、时序和排名，与该配置冻结的预期直接比较。想看两组数字本身，可以展开 `capacity.wide_aov_example`，其中保留了一次宽窗客单价的预期和实际业务字段。修复前后的请求、数据清单、固定 SQL、预期在四套数据上共核对 16 对输入。[容量验收汇总原件](verification/capacity-verified.json)

## 600 秒、1,777 个节点和 19 项安装各覆盖什么？

- **固定 SQL 稳态。** `sql-runs.json → steady` 按分钟列出提交数、完成且正确数、超时和未完成数。按分钟相加得到 889/889，与按 SQL、时间窗统计的原生终态数相符。测量窗口 600.007 秒，前 120 秒并发 1、之后并发 2，模型调用 0 次。摘录时把 889 个任务的终态逐个与源行评分的实际值及预期值核对。
- **开发工程软件回归。** `sql-runs.json → development_ci.units` 的 70 行分别对应一个测试文件。逐文件读取原生 JUnit XML，核对冻结节点 ID 和失败、跳过记录，再依照对应的冻结源码统计 integration 标记，合计得到 1,777 个 Python 节点，其中 589 个带 integration 标记。前端另列 8 个原始状态测试名称。上述是软件测试节点数。
- **安装检查。** `model-cases.json → installation.checks` 列出 19 件检查的名字与判定，包括同一宿主新卷装载、重复初始化后的五表记录数、已有账户与数据保持等。涉及账户或运行摘要的检查只给出两值是否相等。初次安装及再初始化时，演示库均有 6 个客户、9 个商品、17 个订单、19 条明细和 12 条退款。

## 哪些文件保留了原始字节？

以下两份文件保留采集时的换行与内容，可对原文件字节直接算 SHA-256。

| 验收原件 | SHA-256 |
| --- | --- |
| [expected-precall.json](verification/expected-precall.json) | `384b25525ed7fb11290e4092d2263ff9f997142b229737033f9baa61d208d85f` |
| [capacity-verified.json](verification/capacity-verified.json) | `774d5b385e982578a01f5ad1a6c6af2690d88b8e0a57925885a3f458cee00f11` |

其余两份 JSON 是可公开字段的摘录，附有 `source_files_sha256`，用原文件相对名称和 SHA-256 标明出处。模型结果中保留每题的实际业务字段。容量记录中的 `matched_projection_sha256` 则是摘录时从实际结果的 `periods`、`ranked_groups`、`series` 三个字段计算的摘要，序列化采用 UTF-8、按键排序、无额外空白。原始运行的 `result_hash` 覆盖另一套完整结果字段。两者分别标注。

首轮和重跑的开发源码快照摘要在 `model-cases.json` 的 `initial`、`rerun` 下。它们标识当时用于测试的文件集合。历史响应的数据版本标签还有一处已知差异，详见[影响和修复计划](known-issues.md)。
