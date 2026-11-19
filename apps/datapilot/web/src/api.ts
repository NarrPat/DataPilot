/** Same-origin transport. Session cookies stay HttpOnly; CSRF lives in memory. */
import type { CsrfResponse, LoginResponse, MeResponse, PrincipalPublic } from "../../contracts/foundation";
export type PublicUser = PrincipalPublic;
export type SessionResponse = MeResponse;

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export class ApiError extends Error {
  status: number;
  code: string;
  requestId: string | null;
  retryable: boolean;
  constructor(status: number, body: unknown) {
    const valid = record(body);
    const code = valid && typeof body.code === "string" ? body.code : `HTTP_${status}`;
    super(valid && typeof body.message === "string" ? body.message : "服务暂不可用，请稍后重试。");
    this.status = status;
    this.code = code;
    this.requestId = valid && typeof body.request_id === "string" ? body.request_id : null;
    this.retryable = valid && body.retryable === true;
  }
}

export function accessDenied(error: unknown): boolean {
  return error instanceof ApiError && [401, 403, 404].includes(error.status);
}

export class ApiClient {
  private csrf: string | null = null;
  private epoch = 0;
  private identity: string | null = null;
  private unauthorized: () => void;
  private forbidden: () => void;
  constructor(unauthorized: () => void, forbidden: () => void = () => undefined) {
    this.unauthorized = unauthorized; this.forbidden = forbidden;
  }
  reset(): void { this.csrf = null; this.identity = null; this.epoch += 1; }

  private async request<T>(path: string, options: RequestInit = {}, notify = true): Promise<T> {
    const epoch = this.epoch;
    const response = await fetch(path, { ...options, credentials: "same-origin", cache: "no-store" });
    if (epoch !== this.epoch) throw new ApiError(409, {
      code: "SESSION_CHANGED", message: "身份已变化，已丢弃先前会话的迟到响应。", retryable: false,
    });
    if (response.status === 204) return undefined as T;
    let data: unknown;
    try { data = await response.json(); } catch { data = null; }
    if (epoch !== this.epoch) throw new ApiError(409, {
      code: "SESSION_CHANGED", message: "身份已变化，已丢弃先前会话的迟到响应。", retryable: false,
    });
    if (!response.ok) {
      if (response.status === 401 && notify && epoch === this.epoch) this.unauthorized();
      if (response.status === 403 && record(data) && data.code === "FORBIDDEN") this.forbidden();
      throw new ApiError(response.status, data);
    }
    if (!record(data) || typeof data.request_id !== "string") throw new ApiError(503, {
      code: "INVALID_API_RESPONSE", message: "响应契约不完整，请刷新后重试。", retryable: true,
    });
    return data as T;
  }

  get<T>(path: string, signal?: AbortSignal): Promise<T> {
    return this.request<T>(path, { signal });
  }
  async post<T>(path: string, body: object | undefined, key?: string): Promise<T> {
    if (!this.csrf) await this.bootstrap();
    return this.request<T>(path, { method: "POST", headers: {
      "Content-Type": "application/json", "X-CSRF-Token": this.csrf ?? "",
      ...(key ? { "Idempotency-Key": key } : {}),
    }, body: body === undefined ? undefined : JSON.stringify(body) });
  }
  async bootstrap(): Promise<void> {
    const result = await this.request<CsrfResponse>("/api/v1/auth/csrf", {}, false);
    this.csrf = result.csrf_token;
  }
  async login(username: string, password: string): Promise<SessionResponse> {
    await this.bootstrap();
    const result = await this.request<LoginResponse>("/api/v1/auth/login", {
      method: "POST", headers: { "Content-Type": "application/json", "X-CSRF-Token": this.csrf ?? "" },
      body: JSON.stringify({ username, password }),
    }, false);
    this.epoch += 1;
    this.csrf = result.csrf_token;
    this.identity = `${result.user.user_id}:${result.user.authz_version}`;
    return { request_id: result.request_id, user: result.user };
  }
  async me(signal?: AbortSignal): Promise<SessionResponse> {
    const result = await this.request<SessionResponse>("/api/v1/me", { signal }, false);
    const identity = `${result.user.user_id}:${result.user.authz_version}`;
    if (this.identity && identity !== this.identity) { this.epoch += 1; this.csrf = null; }
    this.identity = identity;
    return result;
  }
  async logout(): Promise<void> {
    await this.post<void>("/api/v1/auth/logout", undefined);
    this.reset();
  }
}
