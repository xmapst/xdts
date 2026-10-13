import { screen, waitFor } from '@testing-library/react'
import type { Matcher, SelectorMatcherOptions } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

export async function findHintText(
  text: Matcher,
  options?: SelectorMatcherOptions,
): Promise<HTMLElement> {
  const user = userEvent.setup()
  let result: HTMLElement | undefined
  await waitFor(
    async () => {
      const visible = screen
        .queryAllByText(text, options)
        .find((element) => !element.closest('[aria-hidden="true"]'))
      if (visible) {
        result = visible
        return
      }
      for (const icon of screen.queryAllByRole('img', { name: /提示|information/i })) {
        await user.hover(icon)
        await new Promise((resolve) => setTimeout(resolve, 25))
        const matches = screen.queryAllByText(text, options)
        if (matches.length) {
          result = matches[0]
          return
        }
        await user.unhover(icon)
      }
      throw new Error('No visible text or hovered hint matches the requested content')
    },
    { timeout: 5000 },
  )
  return result!
}
