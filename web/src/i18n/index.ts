import i18next from 'i18next'
import LanguageDetector from 'i18next-browser-languagedetector'
import { initReactI18next } from 'react-i18next'
import zhCN from './locales/zh-CN'
import enUS from './locales/en-US'

export const defaultLanguage = 'zh-CN'
export const supportedLanguages = ['zh-CN', 'en-US'] as const
export type SupportedLanguage = (typeof supportedLanguages)[number]

void i18next
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources: {
      'zh-CN': { translation: zhCN },
      'en-US': { translation: enUS },
    },
    fallbackLng: defaultLanguage,
    supportedLngs: supportedLanguages,
    interpolation: { escapeValue: false },
    // 中文为默认语言：只信任用户在 TopBar 里显式切换过的语言
    // （缓存进 localStorage），不按浏览器 / 系统语言自动探测，避免访问者在
    // 未做任何选择的情况下看到非中文界面。
    detection: {
      order: ['localStorage'],
      caches: ['localStorage'],
      lookupLocalStorage: 'xdts.lang',
    },
  })

export default i18next
