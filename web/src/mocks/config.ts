// Mock 层开关：仅开发环境生效（`import.meta.env.DEV` 在生产构建中会被
// 静态替换为 false），避免影响生产构建的真实鉴权与业务请求。
// 默认开启，方便 `npm run dev` 在后端未就绪时直接走通页面；
// 后端本地联调时可在 `web/.env.development.local` 设置 `VITE_API_MOCK=false` 关闭。
export function isApiMockEnabled(): boolean {
  return import.meta.env.DEV && import.meta.env.VITE_API_MOCK !== 'false'
}
