import type { TFunction } from 'i18next'

export function enumLabel(t: TFunction, prefix: string, value: string): string {
  // key 由后端原值拼出，永远进不了 TranslationSchema 的字面量联合，所以这里
  // 必须放宽 t 的签名。这不是用 cast 掩盖契约问题：兜底的 defaultValue 正是
  // 对"这个 key 可能不存在"的处理，而不是假装它一定存在。
  const loose = t as unknown as (key: string, options: { defaultValue: string }) => string
  return loose(`${prefix}.${value}`, { defaultValue: value })
}
