import { App, message as staticMessage } from 'antd'
import { isValidElement } from 'react'
import type {
  ArgsProps,
  JointContent,
  MessageInstance,
  NoticeType,
  TypeOpen,
} from 'antd/es/message/interface'
import { HintIcon } from './HintIcon'

function wrapMessage(instance: MessageInstance): MessageInstance {
  const open = (args: ArgsProps) =>
    instance.open({
      ...args,
      pauseOnHover: true,
      icon: args.type === 'error' ? args.icon : <></>,
      duration: args.duration ?? 8,
      content:
        args.type === 'error' ? (
          args.content
        ) : (
          <HintIcon type={args.type ?? 'info'} content={args.content} />
        ),
    })
  const typed =
    (type: NoticeType): TypeOpen =>
    (content: JointContent, duration, onClose) => {
      const args: ArgsProps =
        content &&
        typeof content === 'object' &&
        !isValidElement(content) &&
        'content' in content
          ? content
          : { content }
      return open({
        ...args,
        type,
        duration: typeof duration === 'number' ? duration : args.duration,
        onClose: typeof duration === 'function' ? duration : (onClose ?? args.onClose),
      })
    }
  return {
    open,
    destroy: instance.destroy,
    info: typed('info'),
    success: typed('success'),
    warning: typed('warning'),
    error: typed('error'),
    loading: typed('loading'),
  }
}

export function useHintFeedback() {
  const app = App.useApp()
  return { message: wrapMessage(app.message), notification: app.notification }
}

export const hintMessage = wrapMessage(staticMessage)
