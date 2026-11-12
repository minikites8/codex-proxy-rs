import type { BpsAccountOptions } from '@/api'

export function parseBpsAccountOptions(value: Record<string, unknown> | undefined): BpsAccountOptions | undefined {
  if (!value || typeof value.image_input_enabled !== 'boolean'
    || typeof value.omit_images_when_disabled !== 'boolean'
    || typeof value.omit_unsupported_hosted_tools !== 'boolean'
    || typeof value.ignore_encrypted_history !== 'boolean') {
    return undefined
  }
  return {
    image_input_enabled: value.image_input_enabled,
    omit_images_when_disabled: value.omit_images_when_disabled,
    omit_unsupported_hosted_tools: value.omit_unsupported_hosted_tools,
    ignore_encrypted_history: value.ignore_encrypted_history,
  }
}

export function bpsConnectionChanges(value: BpsAccountOptions | undefined, saved: BpsAccountOptions | undefined) {
  if (!value || !saved)
    return undefined
  const changes = {
    imageInputEnabled: value.image_input_enabled !== saved.image_input_enabled
      ? value.image_input_enabled
      : undefined,
    omitImagesWhenDisabled: value.omit_images_when_disabled !== saved.omit_images_when_disabled
      ? value.omit_images_when_disabled
      : undefined,
    omitUnsupportedHostedTools: value.omit_unsupported_hosted_tools !== saved.omit_unsupported_hosted_tools
      ? value.omit_unsupported_hosted_tools
      : undefined,
    ignoreEncryptedHistory: value.ignore_encrypted_history !== saved.ignore_encrypted_history
      ? value.ignore_encrypted_history
      : undefined,
  }
  return Object.values(changes).some(value => value !== undefined)
    ? changes
    : undefined
}
