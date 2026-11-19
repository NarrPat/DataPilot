import { Alert, Button, Card } from "antd";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import { ApiClient, ApiError } from "./api";
import type { PublicUser, SessionResponse } from "./api";
import { CatalogPage } from "./catalog";
import { HistoryPage } from "./history";
import { Workbench } from "./workbench";
import { ErrorNotice, Loading } from "./ui";

type Page = "workbench" | "catalog" | "history";
interface Route { page: Page; queryId: string | null; catalogVersion: string | null; metricId: string | null }
function currentRoute(): Route {
  const params = new URLSearchParams(window.location.search);
  const page = params.get("page");
  return { page: page === "catalog" || page === "history" ? page : "workbench",
    queryId: params.get("query"), catalogVersion: params.get("catalog_version"), metricId: params.get("metric") };
}
function writeRoute(route: Route): void {
  const params = new URLSearchParams({ page: route.page });
  if (route.queryId) params.set("query", route.queryId);
  if (route.catalogVersion) params.set("catalog_version", route.catalogVersion);
  if (route.metricId) params.set("metric", route.metricId);
  window.history.pushState(null, "", `${window.location.pathname}?${params}`);
}

function Login({ api, signedIn, note }: { api: ApiClient; signedIn: (value: SessionResponse) => void; note: string | null }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<unknown>(null);
  return <main className="login-shell"><div className="login-brand"><span className="brand-symbol">D</span><span>DataPilot</span></div>
    <p className="login-intro">把运营问题变成可检查的分析。</p>
    <Card className="login-card" title="登录 DataPilot">
      {note && <Alert type="info" title={note} />}
      <form onSubmit={(event) => {
        event.preventDefault();
        if (pending) return;
        setPending(true); setError(null);
        void api.login(username, password).then((session) => { setPassword(""); signedIn(session); })
          .catch(setError).finally(() => setPending(false));
      }}>
        <label htmlFor="username">账号</label><input id="username" data-testid="login-username" autoComplete="username" maxLength={128}
          value={username} onChange={(event) => setUsername(event.target.value)} required />
        <label htmlFor="password">密码</label><input id="password" type="password" data-testid="login-password" autoComplete="current-password"
          value={password} onChange={(event) => setPassword(event.target.value)} required />
        <Button block type="primary" htmlType="submit" loading={pending} data-testid="login-submit">登录</Button>
      </form>
      <ErrorNotice error={error} />
      <p className="muted">使用本机配置的演示账号。权限和数据范围由服务端决定。</p>
    </Card>
  </main>;
}

function Workspace({ api, user, logout, revoked, sessionError }: {
  api: ApiClient; user: PublicUser; logout: () => Promise<void>; revoked: () => void; sessionError: unknown;
}) {
  const [route, setRoute] = useState<Route>(currentRoute);
  const [draft, setDraft] = useState("");
  const [rerun, setRerun] = useState<{ nonce: number; question: string } | null>(null);
  const [logoutError, setLogoutError] = useState<unknown>(null);
  const [loggingOut, setLoggingOut] = useState(false);
  const token = useRef(0);
  const nonce = useRef(0);
  const navigate = useCallback((next: Route) => {
    token.current += 1;
    writeRoute(next); setRoute(next);
  }, []);
  useEffect(() => {
    const pop = () => { token.current += 1; setRoute(currentRoute()); setRerun(null); };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, []);
  const accepted = useCallback((queryId: string, expected: number) => {
    if (token.current !== expected) return;
    setRerun(null);
    navigate({ page: "workbench", queryId, catalogVersion: null, metricId: null });
  }, [navigate]);
  const consumeRerun = useCallback(() => setRerun(null), []);
  return <div className="app-shell">
    <header className="app-header"><a href="?page=workbench" className="brand" onClick={(event) => { event.preventDefault(); navigate({ ...route, page: "workbench" }); }}>
      <span className="brand-symbol">D</span><strong>DataPilot</strong></a>
      <nav aria-label="主导航">{([["workbench", "问数工作台"], ["catalog", "指标与表结构"], ["history", "历史与评测"]] as const).map(([page, title]) =>
        <button key={page} data-testid={`nav-${page}`} aria-current={route.page === page ? "page" : undefined}
          onClick={() => { setRerun(null); navigate({ ...route, page }); }}>{title}</button>)}</nav>
      <div className="identity"><span><strong data-testid="user-name">{user.username}</strong><small data-testid="user-scope">{user.effective_scope ?? "报告只读"}</small></span>
        <Button data-testid="logout" loading={loggingOut} onClick={() => {
          setLoggingOut(true); setLogoutError(null);
          void logout().catch(setLogoutError).finally(() => setLoggingOut(false));
        }}>退出</Button></div>
    </header>
    <main className="app-main">
      <ErrorNotice error={logoutError} />
      {Boolean(sessionError) && <Alert type="warning" title="当前会话校验暂不可用；展示上次确认的身份，操作仍需服务端重新授权。" />}
      {route.page === "workbench" && <Workbench api={api} user={user} queryId={route.queryId} navigationToken={token.current}
        draft={draft} setDraft={setDraft} accepted={accepted} revoked={revoked} rerun={rerun} consumeRerun={consumeRerun}
        catalog={(version, metricId) => navigate({ ...route, page: "catalog", catalogVersion: version, metricId })} />}
      {route.page === "catalog" && <CatalogPage api={api} user={user} version={route.catalogVersion} metricId={route.metricId} />}
      {route.page === "history" && <HistoryPage api={api} user={user}
        open={(queryId) => navigate({ ...route, page: "workbench", queryId })}
        rerun={(question) => { setDraft(question); setRerun({ question, nonce: ++nonce.current }); navigate({ ...route, page: "workbench", queryId: null }); }} />}
    </main>
    <footer>合成电商数据 · 五表五指标 · Asia/Shanghai</footer>
  </div>;
}

export function App() {
  const client = useQueryClient();
  const [signedOut, setSignedOut] = useState(false);
  const [note, setNote] = useState<string | null>(null);
  const api = useRef<ApiClient | null>(null);
  const channel = useRef<BroadcastChannel | null>(null);
  const hadSession = useRef(false);
  const revoke = useCallback(() => {
    api.current?.reset();
    void client.cancelQueries(); client.clear();
    setSignedOut(true); setNote("会话或访问权限已变化，请重新登录。");
  }, [client]);
  const refreshAuthorization = useCallback(() => {
    // Let the current forbidden response reach its view first, which hides the
    // cached payload immediately. Then discard resource caches and reread /me.
    window.setTimeout(() => {
      const resource = (query: { queryKey: readonly unknown[] }) =>
        query.queryKey[0] === "datapilot" && query.queryKey[1] !== "session";
      void client.cancelQueries({ predicate: resource });
      client.removeQueries({ predicate: resource });
      void client.invalidateQueries({ queryKey: ["datapilot", "session"] });
    }, 0);
  }, [client]);
  if (!api.current) api.current = new ApiClient(revoke, refreshAuthorization);
  const transport = api.current;
  const session = useQuery({ queryKey: ["datapilot", "session"], enabled: !signedOut,
    queryFn: ({ signal }) => transport.me(signal), retry: false, refetchInterval: 30000,
  });
  const identity = session.data ? `${session.data.user.user_id}:${session.data.user.authz_version}` : null;
  useEffect(() => {
    if (session.error instanceof ApiError && session.error.status === 401) {
      if (hadSession.current) revoke(); else setSignedOut(true);
    }
  }, [session.error, revoke]);
  useEffect(() => {
    if (!session.data) return;
    hadSession.current = true;
    const user = session.data.user;
    void client.cancelQueries({ predicate: (query) => query.queryKey[0] === "datapilot" &&
      query.queryKey[1] !== "session" && (query.queryKey[1] !== user.user_id || query.queryKey[2] !== user.authz_version) });
    client.removeQueries({ predicate: (query) => query.queryKey[0] === "datapilot" &&
      query.queryKey[1] !== "session" && (query.queryKey[1] !== user.user_id || query.queryKey[2] !== user.authz_version) });
  }, [identity, client]);
  useEffect(() => {
    if (!("BroadcastChannel" in window)) return;
    const current = new BroadcastChannel("datapilot.session");
    channel.current = current;
    current.onmessage = (message: MessageEvent<unknown>) => {
      if (message.data && typeof message.data === "object" && "type" in message.data && message.data.type === "signed-out") revoke();
    };
    return () => { current.close(); channel.current = null; };
  }, [revoke]);
  if (!signedOut && session.isPending) return <Loading text="检查本地会话" />;
  if (signedOut || !session.data) return <Login api={transport} note={note}
    signedIn={(value) => { setSignedOut(false); setNote(null); client.setQueryData(["datapilot", "session"], value); }} />;
  return <Workspace key={identity} api={transport} user={session.data.user} revoked={revoke} sessionError={session.error}
    logout={async () => {
      await transport.logout();
      channel.current?.postMessage({ type: "signed-out" });
      void client.cancelQueries(); client.clear();
      setSignedOut(true); setNote("已退出登录。已接受的查询继续由后台处理。");
    }} />;
}
