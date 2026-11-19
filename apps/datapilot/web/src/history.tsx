import { Alert, Button, Card, Tag } from "antd";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import type { HistoryResponse, ReportDocument, ReportResponse, ReportsResponse } from "../../contracts/foundation";
import { accessDenied, ApiClient } from "./api";
import type { PublicUser } from "./api";
import { displayDate, EmptyState, ErrorNotice, JsonBlock, Loading, statusNames } from "./ui";

interface HistoryProps {
  api: ApiClient; user: PublicUser;
  open: (queryId: string) => void;
  rerun: (question: string) => void;
}
function fraction(numerator: number, denominator: number): string {
  if (!denominator) return "未定义（分母为 0）";
  const hundredths = BigInt(numerator) * 10000n / BigInt(denominator);
  return `${hundredths / 100n}.${(hundredths % 100n).toString().padStart(2, "0")}%`;
}
function ReportView({ report }: { report: ReportDocument }) {
  const scored = report.status === "scored" && report.ready;
  const unknown = (value: string | number | null) => value === null ? "unknown" : String(value);
  return <Card title={report.title} data-testid="report-detail"><div className="form-actions">
    <Tag color={report.report_type === "synthetic_test_report" ? "orange" : "blue"} data-testid="report-type">
      {report.report_type === "synthetic_test_report" ? "测试报告" : "正式评测"}</Tag>
    <Tag>{report.status}</Tag><span>ready={String(report.ready)} · gold_reviewed={String(report.gold_reviewed)}</span></div>
    <p>报告时间：{displayDate(report.created_at)}</p>
    {!scored && <Alert type="info" title="此报告尚无可发布成绩；设计、未跑和失败状态不冒充已验收效果。" />}
    <div className="table-scroll"><table data-testid="report-metrics"><thead><tr><th>指标</th><th>分子</th><th>分母</th><th>结果</th></tr></thead>
      <tbody>{report.metrics.map((metric) => <tr key={metric.key}><td>{metric.key}</td><td>{metric.numerator}</td><td>{metric.denominator}</td>
        <td>{scored ? fraction(metric.numerator, metric.denominator) : "未验收"}</td></tr>)}</tbody></table></div>
    {!report.metrics.length && <p>尚无已评分指标。</p>}
    <h3>调用与费用</h3><dl data-testid="report-usage"><dt>模型调用</dt><dd>{unknown(report.usage.model_calls)}</dd>
      <dt>输入 / 输出 tokens</dt><dd>{unknown(report.usage.input_tokens)} / {unknown(report.usage.output_tokens)}</dd>
      <dt>费用 CNY</dt><dd>{unknown(report.usage.cost_cny)}</dd></dl>
    <p className="muted">unknown 表示没有观测值，不表示免费或零次。</p>
    <h3>配置与版本</h3><JsonBlock value={report.configuration} testId="report-configuration" />
    <h3>失败样例</h3>{report.failures.length ? <ul data-testid="report-failures">{report.failures.map((failure, index) =>
      <li key={`${failure.case_id}-${index}`}><strong>{failure.case_id}</strong> · {failure.stage} · {failure.code}<p>{failure.message}</p></li>)}</ul> : <p>报告未记录失败样例；不据此推断未执行的任务成功。</p>}
  </Card>;
}

export function HistoryPage({ api, user, open, rerun }: HistoryProps) {
  const mayHistory = user.capabilities.includes("history:read_self");
  const mayReports = user.capabilities.includes("reports:read");
  const [tab, setTab] = useState(mayHistory ? "queries" : "reports");
  const [status, setStatus] = useState("");
  const [reportId, setReportId] = useState<string | null>(null);
  const history = useInfiniteQuery({ queryKey: ["datapilot", user.user_id, user.authz_version, "history", status],
    enabled: mayHistory && tab === "queries", initialPageParam: null as string | null, retry: false,
    queryFn: ({ pageParam, signal }) => {
      const params = new URLSearchParams({ limit: "10" });
      if (status) params.set("status", status);
      if (pageParam) params.set("cursor", pageParam);
      return api.get<HistoryResponse>(`/api/v1/history?${params}`, signal);
    }, getNextPageParam: (last) => last.next_cursor ?? undefined });
  const reports = useInfiniteQuery({ queryKey: ["datapilot", user.user_id, user.authz_version, "reports"],
    enabled: mayReports && tab === "reports", initialPageParam: null as string | null, retry: false,
    queryFn: ({ pageParam, signal }) => api.get<ReportsResponse>(`/api/v1/eval/reports?limit=10${pageParam ? `&cursor=${encodeURIComponent(pageParam)}` : ""}`, signal),
    getNextPageParam: (last) => last.next_cursor ?? undefined });
  const detail = useQuery({ queryKey: ["datapilot", user.user_id, user.authz_version, "report", reportId],
    enabled: mayReports && tab === "reports" && Boolean(reportId), retry: false,
    queryFn: ({ signal }) => api.get<ReportResponse>(`/api/v1/eval/reports/${encodeURIComponent(reportId!)}`, signal) });
  const items = accessDenied(history.error) ? [] : history.data?.pages.flatMap((page) => page.items) ?? [];
  const reportDenied = accessDenied(reports.error) || accessDenied(detail.error);
  const reportItems = reportDenied ? [] : reports.data?.pages.flatMap((page) => page.items) ?? [];
  return <div className="page-stack">
    <section className="page-heading"><div><p className="eyebrow">持久化快照与评测来源</p><h1>历史与评测</h1><p>读取历史不会重新执行 SQL，再次查询会生成新的 query_id。</p></div></section>
    <div className="tabs" role="tablist" aria-label="历史与评测分类">
      {mayHistory && <button role="tab" data-testid="history-tab-queries" aria-selected={tab === "queries"} onClick={() => setTab("queries")}>查询历史</button>}
      {mayReports && <button role="tab" data-testid="history-tab-reports" aria-selected={tab === "reports"} onClick={() => setTab("reports")}>评测报告</button>}
    </div>
    {!mayHistory && !mayReports && <EmptyState text="当前身份没有历史或报告读取权限。" />}
    {tab === "queries" && mayHistory && <>
      <Card><div className="form-actions"><label>按状态筛选 <select data-testid="history-status-filter" value={status} onChange={(event) => setStatus(event.target.value)}>
        <option value="">全部状态</option>{Object.entries(statusNames).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
        <Button onClick={() => { void history.refetch(); }}>刷新历史</Button></div></Card>
      <ErrorNotice error={history.error} retry={() => { void history.refetch(); }} />
      {history.isLoading && <Loading text="读取当前用户历史" />}
      {history.isSuccess && !items.length && <EmptyState text="没有符合条件的历史查询。" />}
      {items.map((item) => <Card key={item.query_id} data-testid={`history-item-${item.query_id}`}>
        <div className="snapshot-heading"><strong>{item.question}</strong><div><Tag>{statusNames[item.status] ?? item.status}</Tag>
          {!item.current_version_matches && <Tag color="warning">历史版本</Tag>}</div></div>
        <p className="muted"><code>{item.query_id}</code> · spec {item.spec_version} · {item.effective_scope}</p>
        <p className="muted">{item.catalog_version} / {item.data_version} · {displayDate(item.api_received_at)}</p>
        <div className="form-actions"><Button onClick={() => open(item.query_id)}>打开原快照</Button>
          {user.capabilities.includes("queries:create") && <Button onClick={() => rerun(item.question)}>再次查询</Button>}</div>
      </Card>)}
      {history.hasNextPage && <Button data-testid="history-next" loading={history.isFetchingNextPage} onClick={() => { void history.fetchNextPage(); }}>加载更多历史</Button>}
    </>}
    {tab === "reports" && mayReports && <>
      <ErrorNotice error={reports.error ?? detail.error} retry={() => { void reports.refetch(); if (reportId) void detail.refetch(); }} />
      {reports.isLoading && <Loading text="读取已导入报告" />}
      {reports.isSuccess && !reportItems.length && <EmptyState text="尚未导入已授权报告；没有可展示的模型成绩。" />}
      <div className="report-list">{reportItems.map((report) => <Card key={report.report_id} data-testid={`report-item-${report.report_id}`}>
        <Tag>{report.report_type === "synthetic_test_report" ? "测试报告" : "正式评测"}</Tag><Tag>{report.status}</Tag>
        <h3>{report.title}</h3><p>ready={String(report.ready)} · {displayDate(report.created_at)}</p>
        <Button onClick={() => setReportId(report.report_id)}>查看报告</Button>
      </Card>)}</div>
      {reports.hasNextPage && <Button loading={reports.isFetchingNextPage} onClick={() => { void reports.fetchNextPage(); }}>加载更多报告</Button>}
      {detail.isLoading && <Loading text="读取报告详情" />}
      {detail.data && !reportDenied && <ReportView report={detail.data.report} />}
    </>}
    {!mayReports && <p className="muted">评测报告仅向具备 reports:read 能力的演示管理员开放。</p>}
  </div>;
}
