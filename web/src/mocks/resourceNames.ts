import { ApiError } from '@/api/client'

export function validateResourceName(
  name: string,
  entries: readonly { id: string; name: string }[],
  code: string,
  id?: string,
) {
  const normalized = name.trim()
  if (!normalized || Array.from(normalized).length > 128) {
    throw new ApiError('Invalid resource name', 400, 'invalid_name')
  }
  const key = normalized.normalize('NFD').replace(/\p{M}/gu, '').toLocaleLowerCase()
  if (
    entries.some(
      (entry) =>
        entry.id !== id &&
        entry.name.trim().normalize('NFD').replace(/\p{M}/gu, '').toLocaleLowerCase() === key,
    )
  ) {
    throw new ApiError('Resource name already exists', 409, code)
  }
  return normalized
}
