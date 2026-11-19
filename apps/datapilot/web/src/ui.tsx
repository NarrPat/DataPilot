import { Alert, Button, Empty, Spin } from "antd";
import type { ReactNode } from "react";
import type { MetricId } from "../../contracts/current";
import { ApiError } from "./api";

export const metricNames: Record<MetricId, string> = {
  paid_goods_amount: "支付商品金额", paid_order_count: "支付订单数",
  refund_goods_amount: "成功退款商品金额", period_net_goods_amount: "期间商品净流入", paid_aov: "支付客单价",
};
export const metricMeaning: Record<MetricId, string> = {
  paid_goods_amount: "按支付成功时间统计商品行实付金额，后续退款不冲减此指标。",
  paid_order_count: "按支付成功时间去重统计订单；同一订单跨品类出现时，各品类订单数不能直接相加。",
  refund_goods_amount: "按退款成功时间统计金额，包含之前支付的订单；待处理和失败退款不计入。",
  period_net_goods_amount: "本期支付商品金额减本期成功退款金额，两条时间轴分别统计，结果可以为负。",
  paid_aov: "支付商品金额 ÷ 去重支付订单数；全期重新计算，不平均各周客单价。无订单为 null，零元订单为 0。",
};
export const statusNames: Record<string, string> = {
  queued: "排队中", parsing: "解析问题", generating: "生成计划", guarding: "权限与口径校验",
  planning: "执行预检", executing: "只读查询", validating: "检查结果", repairing: "受限修正",
  needs_clarification: "等待澄清", completed: "已完成", rejected: "已拒绝", needs_refine: "需要缩小或调整问题",
  failed: "执行失败", timed_out: "已超时", canceled: "已取消",
};

export function ErrorNotice({ error, retry }: { error: unknown; retry?: () => void }) {
  if (!error) return null;
  const safe = error instanceof ApiError;
  return <Alert type="error" showIcon role="alert" data-testid="request-error"
    title={safe ? `${error.status} · ${error.code}` : "连接失败"}
    description={<><p>{safe ? error.message : "无法连接服务，输入已保留。请检查连接后重试。"}</p>
      {safe && error.status === 429 && <p>容量暂满，输入与本次请求键已保留，可以原样重试。</p>}
      {safe && error.status === 409 && <p>服务器版本或请求键冲突。请先刷新当前快照，再确认输入。</p>}
      {safe && error.requestId && <small>请求编号：{error.requestId}</small>}
      {retry && <div><Button onClick={retry}>刷新当前快照</Button></div>}</>} />;
}

export function Loading({ text = "加载中" }: { text?: string }) {
  return <div className="loading" role="status"><Spin /> <span>{text}</span></div>;
}
export function EmptyState({ text }: { text: string }) { return <Empty description={text} />; }
export function JsonBlock({ value, testId }: { value: unknown; testId?: string }) {
  return <pre className="json-block" data-testid={testId}>{value === null || value === undefined ? "尚未产生" : JSON.stringify(value, null, 2)}</pre>;
}
export function Meta({ label, children }: { label: string; children: ReactNode }) {
  return <span className="meta"><small>{label}</small><span>{children}</span></span>;
}
export function displayDate(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? "时间未知" : new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai", dateStyle: "medium", timeStyle: "short",
  }).format(date);
}
export function displayValue(value: string | number | null | undefined): string {
  return value === null || value === undefined ? "—" : String(value);
}
