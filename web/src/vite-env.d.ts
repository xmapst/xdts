/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** 开发期 mock 层开关，默认开启；设为 'false' 关闭并联调真实后端。 */
  readonly VITE_API_MOCK?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
