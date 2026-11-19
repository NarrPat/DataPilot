import { Alert, Button, Card, Tag } from "antd";
import { useQuery } from "@tanstack/react-query";
import type { MetricsResponse, SchemaResponse } from "../../contracts/foundation";
import { accessDenied, ApiClient } from "./api";
import type { PublicUser } from "./api";
import { EmptyState, ErrorNotice, Loading, Meta } from "./ui";

interface CatalogProps { api: ApiClient; user: PublicUser; version: string | null; metricId: string | null }
export function CatalogPage({ api, user, version, metricId }: CatalogProps) {
  const mayRead = user.capabilities.includes("catalog:read");
  const suffix = version ? `?catalog_version=${encodeURIComponent(version)}` : "";
  const metrics = useQuery({ queryKey: ["datapilot", user.user_id, user.authz_version, "catalog-metrics", version],
    enabled: mayRead, retry: false, queryFn: ({ signal }) => api.get<MetricsResponse>(`/api/v1/catalog/metrics${suffix}`, signal) });
  const schema = useQuery({ queryKey: ["datapilot", user.user_id, user.authz_version, "catalog-schema", version],
    enabled: mayRead, retry: false, queryFn: ({ signal }) => api.get<SchemaResponse>(`/api/v1/catalog/schema${suffix}`, signal) });
  if (!mayRead) return <Card><EmptyState text="当前身份没有目录读取权限。" /></Card>;
  const denied = accessDenied(metrics.error) || accessDenied(schema.error);
  const data = denied ? undefined : metrics.data;
  const schemaData = denied ? undefined : schema.data;
  return <div className="page-stack">
    <section className="page-heading"><div><p className="eyebrow">版本化只读目录</p><h1>指标与表结构</h1><p>口径、单位、时间与汇总规则来自同一服务端目录。</p></div>
      <Button onClick={() => { void metrics.refetch(); void schema.refetch(); }}>刷新目录</Button></section>
    <ErrorNotice error={metrics.error ?? schema.error} retry={() => { void metrics.refetch(); void schema.refetch(); }} />
    {(metrics.isLoading || schema.isLoading) && <Loading text="读取当前权限内的目录" />}
    {data && <>
      <Card><div className="metadata"><Meta label="目录版本"><code data-testid="catalog-version">{data.catalog_version}</code></Meta>
        <Meta label="权限范围"><code data-testid="catalog-scope">{data.effective_scope}</code></Meta><Meta label="就绪状态">{data.readiness.state}</Meta></div>
        {data.readiness.state !== "ready" && <Alert type={data.readiness.state === "not_ready" ? "warning" : "info"}
          data-testid="catalog-readiness" title={data.readiness.state === "not_ready" ? "目录依赖尚未就绪" : "目录定义已读取，执行依赖就绪状态尚未验证"} description={data.readiness.code} />}
        <details><summary>定义与结构指纹</summary><p className="break-word">definition_hash：{data.definition_hash}</p><p className="break-word">schema_fingerprint：{data.schema_fingerprint}</p></details>
      </Card>
      {!data.metrics.length && <EmptyState text="当前范围没有可见指标。" />}
      <div className="metric-grid">{data.metrics.map((metric) => {
        const readiness = data.metric_readiness.find((r) => r.metric_id === metric.metric_id);
        return <Card key={metric.metric_id} id={`metric-${metric.metric_id}`} data-testid={`catalog-metric-${metric.metric_id}`}
          className={metric.metric_id === metricId ? "selected-metric" : ""} title={metric.name}
          extra={<Tag>{readiness?.ready === true ? "已验证就绪" : readiness?.ready === false ? "未就绪" : "就绪未知"}</Tag>}>
          <p><code>{metric.metric_id}</code> · {metric.metric_version}</p>
          <dl><dt>单位 / 事实粒度</dt><dd>{metric.unit} / {metric.fact_grain}</dd>
            <dt>业务时间字段</dt><dd>{metric.time_fields.join("、")}</dd>
            <dt>允许维度 / 时间粒度</dt><dd>{metric.allowed_dimensions.join("、")} / {metric.allowed_grains.join("、")}</dd>
            <dt>空值规则</dt><dd>{metric.zero_policy}</dd><dt>汇总规则</dt><dd>{metric.rollup_policy}</dd>
            <dt>筛选口径</dt><dd>{Object.entries(metric.filter_contract).map(([key, value]) => <p key={key}>{key}：{value}</p>)}</dd></dl>
          {metric.metric_id === "paid_aov" && <p className="muted">客单价按汇总支付金额 / 去重支付订单数重算；无支付订单时为 null，不能平均周值或品类值。</p>}
          {metric.metric_id.includes("refund") || metric.metric_id === "period_net_goods_amount" ?
            <p className="muted">成功退款按退款发生时间统计；退款不会移除历史支付订单或支付金额。</p> : null}
          <p className="muted">时区 Asia/Shanghai；周一开周；窗口为左闭右开。</p>
        </Card>;
      })}</div>
    </>}
    {schemaData && <Card title="当前范围允许的五个视图" data-testid="catalog-schema">
      {data && (schemaData.catalog_version !== data.catalog_version || schemaData.effective_scope !== data.effective_scope) ? <Alert type="warning" title="指标与结构版本或范围不同，暂不组合展示。请刷新。" /> : <>
        {!Object.keys(schemaData.views).length && <EmptyState text="当前范围没有可见视图。" />}
        <div className="schema-grid">{Object.entries(schemaData.views).map(([view, columns]) => <section key={view}>
          <h3><code>{view}</code></h3><table><thead><tr><th>列</th><th>类型</th></tr></thead><tbody>{Object.entries(columns).map(([column, type]) =>
            <tr key={column}><td>{column}</td><td>{type}</td></tr>)}</tbody></table></section>)}</div>
        <h3>允许关系</h3><ul>{schemaData.join_paths.map(([left, right]) => <li key={`${left}-${right}`}><code>{left} → {right}</code></li>)}</ul>
        <h3>固定能力</h3><p>{schemaData.allowed_features.join(" · ")}</p>
      </>}
    </Card>}
  </div>;
}
