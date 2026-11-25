import { Alert, Button, Card, Tag } from "antd";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CancelInput, ClarifyInput, QueryInput, QuerySnapshot, MetricId } from "../../contracts/foundation";
import { ApiClient, ApiError } from "./api";
import type { PublicUser } from "./api";
import { ResultChart } from "./charts";
import { subscribeEvents } from "./events";
import type { StreamState } from "./events";
import { acceptSnapshot, isTerminal, makeRequest, ratioAsPercent } from "./state";
import type { CommandRequest } from "./state";
import { displayDate, displayValue, EmptyState, ErrorNotice, JsonBlock, Loading, Meta, metricMeaning, metricNames, statusNames } from "./ui";

interface BaseReference { query_id: string; spec_version: number; thread_id: string }
interface QueryCommand<T> { queryId: string; body: T; request: CommandRequest }
const exampleQuestions: { metric: MetricId; question: string }[] = [
  { metric: "paid_goods_amount", question: "2026年10月华东各品类支付商品金额是多少？" },
  { metric: "paid_order_count", question: "2026年10月华东有多少笔支付订单？" },
  { metric: "refund_goods_amount", question: "2026年10月华东成功退款商品金额是多少？" },
  { metric: "period_net_goods_amount", question: "2026年10月华东每周各品类期间商品净流入额是多少？与9月同品类全期汇总相比如何？" },
  { metric: "paid_aov", question: "2026年10月华东各品类支付客单价是多少？" },
];
interface WorkbenchProps {
  api: ApiClient; user: PublicUser; queryId: string | null; navigationToken: number;
  draft: string; setDraft: (value: string) => void;
  accepted: (queryId: string, navigationToken: number) => void;
  catalog: (version: string, metricId: string) => void;
  revoked: () => void;
  rerun: { nonce: number; question: string } | null;
  consumeRerun: () => void;
}

export function Workbench({ api, user, queryId, navigationToken, draft, setDraft, accepted, catalog, revoked, rerun, consumeRerun }: WorkbenchProps) {
  const client = useQueryClient();
  const key = useMemo(() => ["datapilot", user.user_id, user.authz_version, "query", queryId] as const,
    [user.user_id, user.authz_version, queryId]);
  const [tab, setTab] = useState("table");
  const [base, setBase] = useState<BaseReference | null>(null);
  const [answer, setAnswer] = useState<MetricId | "">("");
  const [stream, setStream] = useState<StreamState>("closed");
  const createKey = useRef<CommandRequest | null>(null);
  const clarifyKey = useRef<CommandRequest | null>(null);
  const cancelKey = useRef<CommandRequest | null>(null);
  const lastDraftQuery = useRef<string | null>(null);
  const previousRerun = useRef<number | null>(null);
  const mayRead = user.capabilities.includes("queries:read_self");
  const mayCreate = user.capabilities.includes("queries:create");
  const query = useQuery({ queryKey: key, enabled: Boolean(queryId && mayRead),
    queryFn: async ({ signal }) => {
      const incoming = await api.get<QuerySnapshot>(`/api/v1/queries/${encodeURIComponent(queryId!)}`, signal);
      if (incoming.query_id !== queryId) throw new ApiError(503, {
        code: "QUERY_RESPONSE_MISMATCH", message: "响应不属于当前查询，已停止展示。", retryable: true,
      });
      const latest = acceptSnapshot(client.getQueryData<QuerySnapshot>(key), incoming);
      client.setQueryData(["datapilot", user.user_id, user.authz_version, "query-version", latest.query_id, latest.spec_version], latest);
      return latest;
    }, retry: false,
    refetchInterval: (q) => q.state.data && !isTerminal(q.state.data.status) ? 3000 : false,
  });
  const forbidden = query.error instanceof ApiError && [401, 403, 404].includes(query.error.status);
  const snapshot = forbidden ? undefined : query.data;
  const snapshotRef = useRef(snapshot);
  snapshotRef.current = snapshot;
  const refresh = useCallback(async () => {
    const result = await query.refetch();
    return result.error ? undefined : result.data;
  }, [query.refetch]);

  useEffect(() => {
    if (!snapshot || snapshot.query_id === lastDraftQuery.current) return;
    lastDraftQuery.current = snapshot.query_id;
    setDraft(snapshot.question);
    setBase(null);
  }, [snapshot, setDraft]);
  useEffect(() => { setAnswer(""); clarifyKey.current = null; }, [snapshot?.clarification?.clarification_id]);
  const hasSnapshot = Boolean(snapshot);
  useEffect(() => {
    const initial = snapshotRef.current;
    if (!queryId || !initial || isTerminal(initial.status)) { setStream("closed"); return; }
    const controller = new AbortController();
    let alive = true;
    void subscribeEvents(queryId, { seq: initial.last_event_seq, statusVersion: initial.status_version,
      specVersion: initial.spec_version }, {
      refresh, revoked: () => { if (alive) revoked(); },
      status: (value) => { if (alive) setStream(value); },
    }, controller.signal);
    return () => { alive = false; controller.abort(); };
  }, [queryId, hasSnapshot, user.user_id, user.authz_version, refresh, revoked]);

  const create = useMutation({ mutationFn: async (variables: { command: QueryInput; token: number }) => {
    createKey.current = makeRequest(createKey.current, variables.command);
    return api.post<QuerySnapshot>("/api/v1/queries", variables.command, createKey.current.key);
  }, onSuccess: (result, variables) => {
    createKey.current = null;
    setBase(null);
    client.setQueryData<QuerySnapshot>(["datapilot", user.user_id, user.authz_version, "query", result.query_id],
      (previous) => acceptSnapshot(previous, result));
    void client.invalidateQueries({ queryKey: ["datapilot", user.user_id, user.authz_version, "history"] });
    accepted(result.query_id, variables.token);
  } });
  const submit = (question: string, reference: BaseReference | null = base) => {
    const command: QueryInput = { question: question.trim(), ...(reference ? {
      thread_id: reference.thread_id, base_query_id: reference.query_id, base_spec_version: reference.spec_version,
    } : {}) };
    if (command.question && mayCreate && !create.isPending) create.mutate({ command, token: navigationToken });
  };
  useEffect(() => {
    if (!rerun || previousRerun.current === rerun.nonce) return;
    previousRerun.current = rerun.nonce;
    setDraft(rerun.question);
    setBase(null);
    submit(rerun.question, null);
    consumeRerun();
  }, [rerun]);

  const clarify = useMutation({ mutationFn: async (command: QueryCommand<ClarifyInput>) => {
    return api.post<QuerySnapshot>(`/api/v1/queries/${command.queryId}/clarify`, command.body, command.request.key);
  }, onSuccess: (incoming, command) => {
    if (clarifyKey.current?.key === command.request.key) clarifyKey.current = null;
    client.setQueryData<QuerySnapshot>(["datapilot", user.user_id, user.authz_version, "query", incoming.query_id],
      (previous) => acceptSnapshot(previous, incoming));
    if (command.queryId === queryId) void refresh();
  }, onError: (_error, command) => { if (command.queryId === queryId) void refresh(); } });
  const cancel = useMutation({ mutationFn: async (command: QueryCommand<CancelInput>) => {
    return api.post<QuerySnapshot>(`/api/v1/queries/${command.queryId}/cancel`, command.body, command.request.key);
  }, onSuccess: (incoming, command) => {
    if (cancelKey.current?.key === command.request.key) cancelKey.current = null;
    client.setQueryData<QuerySnapshot>(["datapilot", user.user_id, user.authz_version, "query", incoming.query_id],
      (previous) => acceptSnapshot(previous, incoming));
    if (command.queryId === queryId) void refresh();
  }, onError: (_error, command) => { if (command.queryId === queryId) void refresh(); } });
  const submitClarification = () => {
    if (!snapshot?.clarification || !answer) return;
    const body: ClarifyInput = { clarification_id: snapshot.clarification.clarification_id,
      expected_spec_version: snapshot.spec_version, answers: { metric_id: answer } };
    clarifyKey.current = makeRequest(clarifyKey.current, { query_id: snapshot.query_id, ...body });
    clarify.mutate({ queryId: snapshot.query_id, body, request: clarifyKey.current });
  };
  const submitCancel = () => {
    if (!snapshot) return;
    const body: CancelInput = { expected_status_version: snapshot.status_version };
    cancelKey.current = makeRequest(cancelKey.current, { query_id: snapshot.query_id, ...body });
    cancel.mutate({ queryId: snapshot.query_id, body, request: cancelKey.current });
  };
  // A command can finish after navigation. Its snapshot belongs in its own
  // cache entry; its transient error/loading state belongs to its query too.
  const currentCancel = cancel.variables?.queryId === queryId;
  const currentClarify = clarify.variables?.queryId === queryId;
  if (!mayRead && !mayCreate) return <Card><EmptyState text="当前身份没有问数权限，可在历史与评测页查看已授权报告。" /></Card>;
  const confirmed = snapshot?.spec;
  const result = snapshot?.status === "completed" ? snapshot.result : null;
  const nullValue = result?.periods.some((p) => p.value === null) || result?.series.some((p) => p.value === null);
  const stale = snapshot?.current_version_matches === false;
  return <div className="page-stack">
    <section className="page-heading"><div><p className="eyebrow">受控电商分析</p><h1>问数工作台</h1>
      <p>一次一个指标，展示可检查的口径、SQL 和结果。</p></div><Tag>范围：{user.effective_scope ?? "无"}</Tag></section>
    <Card>
      <form onSubmit={(event) => { event.preventDefault(); submit(draft); }}>
        <label htmlFor="question">运营问题</label>
        <textarea id="question" data-testid="question-input" value={draft} maxLength={2000} rows={3}
          onChange={(event) => setDraft(event.target.value)} placeholder="例如：上月华东各品类的支付商品金额是多少？" />
        <div className="form-actions">
          <label className="check-label"><input type="checkbox" data-testid="followup-toggle" checked={Boolean(base)}
            disabled={!confirmed || create.isPending} onChange={(event) => setBase(event.target.checked && snapshot ? {
              query_id: snapshot.query_id, spec_version: snapshot.spec_version, thread_id: snapshot.thread_id,
            } : null)} />沿用当前已确认规格追问</label>
          <Button type="primary" htmlType="submit" data-testid="query-submit" loading={create.isPending}
            disabled={!draft.trim() || !mayCreate}>提交查询</Button>
        </div>
        {base && <p className="muted" data-testid="followup-base">基于 {base.query_id} · spec {base.spec_version}。追问会创建新查询，原结果保留。</p>}
      </form>
      {mayCreate && <div className="question-examples" aria-label="五个指标的示例问题">
        <p className="muted">点击指标填入问题，查询月份已按样例数据选好，可在提交前修改。</p>
        <div className="form-actions">{exampleQuestions.map(({ metric, question }) =>
          <Button key={metric} size="small" disabled={create.isPending} data-testid={`example-${metric}`}
            onClick={() => { setDraft(question); setBase(null); create.reset(); }}>{metricNames[metric]}</Button>)}</div>
      </div>}
      <ErrorNotice error={create.error} retry={queryId ? () => { void refresh(); } : undefined} />
    </Card>
    <ErrorNotice error={query.error} retry={() => { void refresh(); }} />
    {query.isLoading && <Loading text="读取查询快照" />}
    {!queryId && <EmptyState text="输入问题开始查询，或从历史打开一份已保存的快照。" />}
    {snapshot && <section className="page-stack query-snapshot" data-query-id={snapshot.query_id} data-spec-version={snapshot.spec_version} data-result-ref={snapshot.result_ref ?? ""}
      data-last-event-seq={snapshot.last_event_seq} data-status-version={snapshot.status_version} data-cancel-requested={snapshot.cancel_requested}>
      {create.isPending && <Alert type="info" title="新查询正在提交，下方为上一轮查询，尚未替换为新结果。" />}
      {stale && <Alert type="warning" data-testid="stale-version" title="历史版本快照：保留原口径和数据版本；再次查询会创建当前版本的新记录。" />}
      <Card>
        <div className="snapshot-heading"><div><Tag color={snapshot.status === "completed" ? "success" : "processing"}>{statusNames[snapshot.status] ?? snapshot.status}</Tag>
          <code data-testid="query-status">{snapshot.status}</code>
          {snapshot.cancel_requested && !isTerminal(snapshot.status) && <Tag color="warning">cancel_requested · 正在请求取消</Tag>}</div>
          <Button data-testid="query-cancel" disabled={isTerminal(snapshot.status) || snapshot.cancel_requested || !user.capabilities.includes("queries:cancel_self")}
            loading={currentCancel && cancel.isPending} onClick={submitCancel}>取消查询</Button></div>
        <div className="metadata">
          <Meta label="query_id"><code data-testid="query-id">{snapshot.query_id}</code></Meta>
          <Meta label="spec_version"><code data-testid="spec-version">{snapshot.spec_version}</code></Meta>
          <Meta label="数据版本">{snapshot.data_version}</Meta><Meta label="范围">{snapshot.effective_scope}</Meta>
          <Meta label="参照时间 · Asia/Shanghai">{displayDate(snapshot.reference_time)}</Meta>
          <Meta label="SSE"><span data-testid="stream-status">{stream}</span></Meta>
        </div>
        <p>{snapshot.question}</p>
        {snapshot.reason_code && <p className="muted">原因：<code>{snapshot.reason_code}</code></p>}
        {snapshot.reason_code?.startsWith("SQL_METRIC_") && <Alert type="warning" showIcon
          title="生成的 SQL 不符合已确认的指标口径，已停止执行。" description="请查看规格中的指标、时间范围和筛选条件，明确问题后创建新查询。" />}
        {confirmed && <div className="definition-link"><Button type="link" onClick={() => catalog(confirmed.catalog_version, confirmed.spec.metric_id)}>
          {metricNames[confirmed.spec.metric_id]} · {confirmed.metric_version}</Button><span>{confirmed.catalog_version}</span>
          <p>{metricMeaning[confirmed.spec.metric_id]}</p>
          <p>当前期 [{confirmed.spec.time_range.start_at}, {confirmed.spec.time_range.end_at}) · {confirmed.spec.time_range.timezone} · 包含起点，不含终点</p>
          <p>时间粒度：{({ total: "全期", day: "日", week: "周", month: "月" })[confirmed.spec.grain]} · 分组：{confirmed.spec.group_by ?? "不分组"}</p>
          {confirmed.spec.comparison && <p>基期 [{confirmed.spec.comparison.baseline_start_at}, {confirmed.spec.comparison.baseline_end_at}) · 按同一业务分组汇总全期，不与本期时间桶逐行关联。</p>}</div>}
        {stream === "unsupported" && <Alert type="warning" title="事件版本不受支持，已停止应用事件。快照通过独立请求更新；读取失败时可重试。" />}
        {stream === "reconnecting" && <Alert type="info" title="事件连接正在恢复；查询继续执行，当前展示最后确认的服务器快照。" />}
      </Card>
      <ErrorNotice error={currentCancel ? cancel.error : undefined} retry={() => { void refresh(); }} />
      <ErrorNotice error={currentClarify ? clarify.error : undefined} retry={() => { void refresh(); }} />
      {snapshot.status === "needs_clarification" && snapshot.clarification && <Card title="确认一个口径后继续" data-testid="clarification-panel">
        <p>{snapshot.clarification.question}</p><p className="muted">有效至 {displayDate(snapshot.clarification.expires_at)}；继续使用同一 query_id。</p>
        <label htmlFor="clarification">选择指标</label>
        <select id="clarification" data-testid="clarification-options" value={answer} onChange={(event) => setAnswer(event.target.value as MetricId)}>
          <option value="">请选择</option>{snapshot.clarification.options.map((option) => <option key={option} value={option}>{metricNames[option]}</option>)}
        </select>
        <Button type="primary" data-testid="clarification-submit" disabled={!answer || !user.capabilities.includes("queries:clarify_self")}
          loading={currentClarify && clarify.isPending} onClick={submitClarification}>确认并继续</Button>
      </Card>}
      <Card>
        <div className="tabs" role="tablist" aria-label="查询详情">{[["spec", "规格"], ["sql", "SQL"], ["table", "表格"], ["chart", "图表"]].map(([id, label]) =>
          <button key={id} type="button" role="tab" aria-selected={tab === id} data-testid={`tab-${id}`} onClick={() => setTab(id)}>{label}</button>)}</div>
        <div role="tabpanel" className="tab-panel">
          {tab === "spec" && <JsonBlock value={snapshot.spec} testId="confirmed-spec" />}
          {tab === "sql" && <><h3>当前期 SQL</h3><pre data-testid="current-sql">{snapshot.query_plan?.current_sql ?? "尚未生成"}</pre>
            {snapshot.query_plan?.baseline_sql && <><h3>基期 SQL</h3><pre data-testid="baseline-sql">{snapshot.query_plan.baseline_sql}</pre></>}
            <details><summary>守卫摘要</summary><JsonBlock value={snapshot.guard_summary} /></details></>}
          {tab === "table" && (result ? <>
            <p className="muted">结果引用：<code data-testid="result-ref">{snapshot.result_ref}</code> · 单位 {result.unit}</p>
            {nullValue && <Alert type="info" data-testid="null-explanation" title="null 表示没有支付订单，客单价没有定义；不显示为 0。" />}
            {!result.periods.length ? <div data-testid="result-empty"><EmptyState text="合法空结果：当前筛选没有匹配分组。" /></div> :
              <div className="table-scroll"><table data-testid="result-table" data-query-id={snapshot.query_id} data-spec-version={snapshot.spec_version} data-result-ref={snapshot.result_ref ?? ""}>
                <thead><tr><th>分组</th><th>当前期间值（{result.unit}）</th>{confirmed?.spec.comparison && <><th>基期值</th><th>差额</th><th>变化率</th><th>说明</th></>}</tr></thead>
                <tbody>{result.periods.map((p) => <tr key={p.group_key ?? "total"}><td>{p.group_key ?? "总计"}</td><td>{displayValue(p.value)}</td>
                  {confirmed?.spec.comparison && <><td>{displayValue(p.baseline_value)}</td><td>{displayValue(p.delta)}</td><td>{ratioAsPercent(p.pct_change)}</td><td>{p.comparison_reason ?? "—"}</td></>}</tr>)}</tbody>
              </table></div>}
            {result.series.length > 0 && <details><summary>当前期时间序列（{result.series.length} 行）</summary>
              <div className="table-scroll"><table><thead><tr><th>时间桶</th><th>分组</th><th>{result.unit}</th></tr></thead><tbody>{result.series.map((p, index) =>
                <tr key={index}><td>{p.bucket_start ?? "总期间"}</td><td>{p.group_key ?? "总计"}</td><td>{displayValue(p.value)}</td></tr>)}</tbody></table></div></details>}
          </> : <EmptyState text="当前查询尚无完成结果；不会将上一轮表格冒充本次结果。" />)}
          {tab === "chart" && (result ? <ResultChart result={result} chart={snapshot.chart_spec} resultRef={snapshot.result_ref}
            queryId={snapshot.query_id} specVersion={snapshot.spec_version} grain={confirmed?.spec.grain ?? "total"} /> : <EmptyState text="结果完成后展示对应图表。" />)}
        </div>
      </Card>
    </section>}
  </div>;
}
