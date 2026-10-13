// Augment i18next so literal translation keys are checked statically. Dynamic strings use enumLabel fallback.
import 'i18next'
import type { TranslationSchema } from './locales/zh-CN'

declare module 'i18next' {
  interface CustomTypeOptions {
    defaultNS: 'translation'
    resources: {
      translation: TranslationSchema
    }
  }
}
