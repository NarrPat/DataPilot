# 部署 HTTPS 和验证前端

默认的 HTTP 工作台和演示数据可以按 [README](../README.md#跑起来) 启动。下面的命令均从仓库根目录开始。

## 在本机用 HTTPS 打开工作台

准备浏览器信任且适用于 `localhost` 的证书，把 `fullchain.pem` 和 `privkey.pem` 放在仓库外的同一目录，让 Nginx 容器能够读取。将第一条命令里的示例路径换成该目录的绝对路径。

```sh
python scripts/configure-deploy.py --origin https://localhost:18443 --tls-directory "/absolute/path/to/certs"
docker compose -p dp-deploy --env-file .env.deploy -f apps/datapilot/compose.yaml -f apps/datapilot/compose.deploy.yaml up -d --wait postgres redis
docker compose -p dp-deploy --env-file .env.deploy -f apps/datapilot/compose.yaml -f apps/datapilot/compose.deploy.yaml --profile manage run --rm --build manage initialize --dataset demo_v1
docker compose -p dp-deploy --env-file .env.deploy -f apps/datapilot/compose.yaml -f apps/datapilot/compose.deploy.yaml --profile application up -d --build api web
```

打开 [HTTPS 工作台](https://localhost:18443)。这个 Compose 项目有独立的数据卷，使用本机 `.env.deploy` 里的 `DP_SEED_PASSWORD` 登录。配置工具只在该文件尚不存在时创建它。

需要真实模型时，运行下面的命令并在终端提示中输入自己的 DeepSeek 密钥。

```sh
python scripts/configure-deepseek.py --env-file .env.deploy --set-key
```

在 `.env.deploy` 将 `DP_MODEL_MODE` 设为 `live`、`DP_QUERY_PROCESSING_ENABLED` 设为 `1`，然后启动处理进程。

```sh
docker compose -p dp-deploy --env-file .env.deploy -f apps/datapilot/compose.yaml -f apps/datapilot/compose.deploy.yaml --profile application --profile processing up -d --build api web worker control
```

等任务排空后，使用同一套部署参数停止服务。

```sh
docker compose -p dp-deploy --env-file .env.deploy -f apps/datapilot/compose.yaml -f apps/datapilot/compose.deploy.yaml --profile application --profile processing stop
```

## 单独跑前端测试和构建

使用 Node 22.12 至 24.x。状态测试覆盖断线续接、过期响应和百分比精度等情况，构建命令先做 TypeScript 类型检查，再生成生产静态文件。[前端脚本](../apps/datapilot/web/package.json)

```sh
cd apps/datapilot/web
npm ci
npm run test:state
npm run build
```

## 截图是怎样得到的？

README 的图用原工作台回放了 2026 年 11 月 15 日同一条查询的两份 API 响应。一份在等待用户选择指标，一份在任务完成后返回了 SQL 和结果。题目是「2026 年 10 月华东净额是多少？」。该次验收的分析库装载了 `demo_v1`，回放时没有重新调用模型。

当时查询响应的 `data_version` 写的是 `tiny_v1`，数据库装载记录写的是 `demo_v1`。用后者的源数据重算，十月华东实付合计 21,469 分，成功退款 13,600 分，相减后正好是图中的 78.69 元。[演示数据](../fixtures/datapilot/demo_v1.source.json)
