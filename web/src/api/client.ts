// 统一请求封装：/api/v1 前缀、CSRF 双提交、401 处理、错误归一化。
//
// 类型停在 `unknown`：信封层不知道也不该知道每个端点的 details 长什么样，
// 由各域自己做收窄（示例见 web/src/api/nodes.ts 的 parseNodeDeregisterBlockers）。
// 声明成 any 会让收窄这一步在类型上可以被跳过，那正是 strict 模式要防的。

const API_PREFIX = '/api/v1'
const CSRF_COOKIE_NAME = 'XDTS-CSRF'
const CSRF_HEADER_NAME = 'XDTS-CSRF'

/** 幂等方法不需要携带 CSRF 头。 */
const IDEMPOTENT_METHODS = new Set(['GET', 'HEAD', 'OPTIONS'])

export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'

interface EnvelopeErrorBody {
  code?: string
  message?: string
  details?: unknown
}

/** 对应后端 Envelope[T]；data/error 互斥，具体由 success 区分。 */
interface ResponseEnvelope {
  success?: boolean
  data?: unknown
  error?: EnvelopeErrorBody
  request_id?: string
}

function isResponseEnvelope(value: unknown): value is ResponseEnvelope {
  return typeof value === 'object' && value !== null && 'success' in value
}

/** 统一的 API 错误类型；status === 0 表示网络层失败（未收到响应）。 */
export class ApiError extends Error {
  readonly status: number
  readonly code: string | undefined
  readonly requestId: string | undefined
  /** 后端 `error.details` 原样透传（见文件头注释）。绝大多数错误没有这一段，
   * 取值为 undefined；调用方必须自己收窄类型后再使用，不要断言。 */
  readonly details: unknown

  constructor(
    message: string,
    status: number,
    code?: string,
    requestId?: string,
    details?: unknown,
  ) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.requestId = requestId
    this.details = details
  }
}

export interface ApiRequestOptions {
  method?: HttpMethod
  body?: unknown
  signal?: AbortSignal
  /** 登录页等场景下，401 不应触发全局跳转登录，由调用方自行处理错误展示。 */
  suppressAuthRedirect?: boolean
}

let unauthorizedHandler: (() => void) | null = null

export function setUnauthorizedHandler(handler: (() => void) | null): void {
  unauthorizedHandler = handler
}

function readCookie(name: string): string | undefined {
  const prefix = `${name}=`
  for (const part of document.cookie.split('; ')) {
    if (part.startsWith(prefix)) {
      return decodeURIComponent(part.slice(prefix.length))
    }
  }
  return undefined
}

async function safeParseJson(response: Response): Promise<unknown> {
  const text = await response.text()
  if (!text) {
    return undefined
  }
  try {
    return JSON.parse(text) as unknown
  } catch {
    return undefined
  }
}

/**
 * 发起一次 `/api/v1` 请求。
 *
 * - 会话通过 HttpOnly Cookie 传递（`credentials: 'include'`），前端不持有令牌。
 * - 非幂等方法自动附带 CSRF 头（双提交 Cookie 方案）。
 * - 401 时（非 suppressAuthRedirect）调用全局 unauthorizedHandler 跳转登录页。
 */
export async function apiRequest<T>(path: string, options: ApiRequestOptions = {}): Promise<T> {
  const method = options.method ?? 'GET'
  const headers: Record<string, string> = {
    Accept: 'application/json',
  }
  if (options.body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }
  if (!IDEMPOTENT_METHODS.has(method)) {
    const csrf = readCookie(CSRF_COOKIE_NAME)
    if (csrf) {
      headers[CSRF_HEADER_NAME] = csrf
    }
  }

  let response: Response
  try {
    response = await fetch(`${API_PREFIX}${path}`, {
      method,
      headers,
      credentials: 'include',
      body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
      signal: options.signal,
    })
  } catch {
    throw new ApiError('网络请求失败，请检查网络连接', 0)
  }

  if (response.status === 401 && !options.suppressAuthRedirect) {
    unauthorizedHandler?.()
  }

  if (response.status === 204) {
    return undefined as T
  }

  const payload = await safeParseJson(response)
  const envelope = isResponseEnvelope(payload) ? payload : undefined

  if (!response.ok || envelope?.success === false) {
    const message =
      typeof envelope?.error?.message === 'string' && envelope.error.message.length > 0
        ? envelope.error.message
        : `请求失败（HTTP ${response.status}）`
    throw new ApiError(
      message,
      response.status,
      envelope?.error?.code,
      envelope?.request_id,
      envelope?.error?.details,
    )
  }

  // 正常情况下后端总是按 Envelope 返回（data 字段承载实际数据）；万一遇到
  // 不是这个信封形状的 200 响应（例如未来的非 JSON 端点），原样透传。
  return (envelope ? envelope.data : payload) as T
}

export const apiClient = {
  get: <T>(path: string, options?: Omit<ApiRequestOptions, 'method' | 'body'>) =>
    apiRequest<T>(path, { ...options, method: 'GET' }),
  post: <T>(path: string, body?: unknown, options?: Omit<ApiRequestOptions, 'method' | 'body'>) =>
    apiRequest<T>(path, { ...options, method: 'POST', body }),
  put: <T>(path: string, body?: unknown, options?: Omit<ApiRequestOptions, 'method' | 'body'>) =>
    apiRequest<T>(path, { ...options, method: 'PUT', body }),
  patch: <T>(path: string, body?: unknown, options?: Omit<ApiRequestOptions, 'method' | 'body'>) =>
    apiRequest<T>(path, { ...options, method: 'PATCH', body }),
  delete: <T>(path: string, options?: Omit<ApiRequestOptions, 'method' | 'body'>) =>
    apiRequest<T>(path, { ...options, method: 'DELETE' }),
}
