import { Alert } from "antd";
import type { ChartSpec, QueryResult } from "../../contracts/foundation";
import { displayValue, EmptyState } from "./ui";

const colors = ["#186b5b", "#d47a24", "#486ba8", "#965c9b", "#9b532f", "#557941"];
interface ChartProps {
  result: QueryResult; chart: ChartSpec | null; resultRef: string | null;
  queryId: string; specVersion: number; grain: string;
}

/** Fixed SVG component. It reads only the closed ChartSpec and actual result columns. */
export function ResultChart({ result, chart, resultRef, queryId, specVersion, grain }: ChartProps) {
  if (!chart || chart.result_ref !== resultRef || chart.unit !== result.unit)
    return <Alert type="warning" title="图表引用与当前结果不一致，请刷新快照。" />;
  const props = { "data-testid": "result-chart", "data-query-id": queryId,
    "data-spec-version": specVersion, "data-result-ref": resultRef ?? "" };
  if (chart.kind === "table") return <div {...props} className="chart-note">该规格使用表格展示；无时间维度时不绘制折线。</div>;
  const points = chart.kind === "line" ? result.series : result.periods;
  if (!points.length) return <div {...props}><EmptyState text="合法空结果：没有可绘制的数据" /></div>;
  const numbers = points.flatMap((p) => p.value === null ? [] : [Number(p.value)]);
  if (numbers.some((n) => !Number.isFinite(n) || Math.abs(n) > Number.MAX_SAFE_INTEGER))
    return <div {...props}><Alert type="info" title="数值超出图表安全范围，请查看原始格式化表格。" /></div>;
  const min = Math.min(0, ...numbers), max = Math.max(0, ...numbers);
  const span = max - min || 1;
  const width = 800, height = 300, left = 68, right = 25, top = 28, bottom = 70;
  const y = (value: number) => top + (max - value) / span * (height - top - bottom);
  const innerWidth = width - left - right;
  const common = <>
    <line x1={left} x2={width - right} y1={y(0)} y2={y(0)} stroke="#a9b5b0" />
    <text x={left - 8} y={top + 4} textAnchor="end" className="axis-label">{max}</text>
    <text x={left - 8} y={height - bottom + 4} textAnchor="end" className="axis-label">{min}</text>
    <text x={left} y={16} className="axis-label">{result.unit}</text>
  </>;
  if (chart.kind === "bar") {
    const slot = innerWidth / result.periods.length;
    return <div {...props}><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="当前期间指标柱状图">
      {common}
      {result.periods.map((p, index) => {
        const center = left + slot * (index + .5);
        const value = p.value === null ? null : Number(p.value);
        return <g key={p.group_key ?? "total"}>
          {value === null ? <text x={center} y={y(0) - 10} textAnchor="middle">null</text> :
            <rect x={center - Math.min(34, slot * .34)} y={Math.min(y(value), y(0))}
              width={Math.min(68, slot * .68)} height={Math.max(1, Math.abs(y(value) - y(0)))}
              fill={colors[index % colors.length]}><title>{p.group_key ?? "总计"}：{displayValue(p.value)}</title></rect>}
          <text x={center} y={height - bottom + 22} textAnchor="middle" className="axis-label">{p.group_key ?? "总计"}</text>
        </g>;
      })}
    </svg><p className="muted">显示当前期间完整聚合值；表格保留金额原始格式，图表不生成额外总计。</p></div>;
  }
  const buckets = [...new Set(result.series.map((p) => p.bucket_start).filter((v): v is string => v !== null))].sort();
  const dates = buckets.map((b) => Date.parse(b));
  if (!dates.length || dates.some((date) => !Number.isFinite(date)))
    return <div {...props}><Alert type="info" title="没有合法时间桶，使用表格查看结果。" /></div>;
  const first = dates[0], last = dates[dates.length - 1];
  const x = (bucket: string) => left + (last === first ? innerWidth / 2 : (Date.parse(bucket) - first) / (last - first) * innerWidth);
  const groups = [...new Set(result.series.map((p) => p.group_key))];
  const gap = (before: string, after: string) => {
    if (grain === "month") {
      const a = new Date(before), b = new Date(after);
      return (b.getUTCFullYear() - a.getUTCFullYear()) * 12 + b.getUTCMonth() - a.getUTCMonth() > 1;
    }
    return Date.parse(after) - Date.parse(before) > (grain === "week" ? 7 : 1) * 86400000 * 1.1;
  };
  return <div {...props}><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="当前期时间序列折线图，缺失桶和null不补零">
    {common}
    {groups.map((group, index) => {
      const series = result.series.filter((p) => p.group_key === group && p.bucket_start !== null)
        .sort((a, b) => a.bucket_start!.localeCompare(b.bucket_start!));
      const segments: string[][] = [[]];
      let previous: string | null = null;
      for (const p of series) {
        const bucket = p.bucket_start!;
        if (p.value === null) { segments.push([]); previous = null; continue; }
        if (previous && gap(previous, bucket)) segments.push([]);
        segments[segments.length - 1].push(`${x(bucket)},${y(Number(p.value))}`);
        previous = bucket;
      }
      return <g key={group ?? "total"}>
        {segments.filter((s) => s.length).map((s, n) => <polyline key={n} points={s.join(" ")} fill="none" stroke={colors[index % colors.length]} strokeWidth="2.5" />)}
        {series.filter((p) => p.value !== null).map((p) => <circle key={p.bucket_start} cx={x(p.bucket_start!)} cy={y(Number(p.value))}
          r="4" fill={colors[index % colors.length]}><title>{p.bucket_start} · {group ?? "总计"}：{displayValue(p.value)}</title></circle>)}
      </g>;
    })}
    {buckets.map((bucket, index) => (index === 0 || index === buckets.length - 1 || buckets.length <= 8) &&
      <text key={bucket} x={x(bucket)} y={height - bottom + 22} textAnchor="middle" className="axis-label">{bucket.slice(0, 10)}</text>)}
  </svg><div className="legend">{groups.map((g, i) => <span key={g ?? "total"}><i style={{ background: colors[i % colors.length] }} />{g ?? "总计"}</span>)}</div>
    <p className="muted">仅当前期序列；缺失时间桶不自动补齐，null 留空。比较值按期间总额展示，月份不逐周硬配对。</p></div>;
}
