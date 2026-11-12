<script setup lang="ts">
import type { BpsAccountOptions } from '@/api'
import { ZFormItem, ZSwitch } from '@codex-proxy/ui'
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'

defineProps<{ disabled: boolean }>()
const options = defineModel<BpsAccountOptions>({ required: true })
const { t } = useI18n({ useScope: 'global' })
const fields = computed(() => [
  {
    key: 'image_input_enabled',
    label: t('accounts.editor.bps.images'),
    description: t('accounts.editor.bps.imagesHelp'),
  },
  {
    key: 'omit_images_when_disabled',
    label: t('accounts.editor.bps.omitImages'),
    description: t('accounts.editor.bps.omitImagesHelp'),
  },
  {
    key: 'omit_unsupported_hosted_tools',
    label: t('accounts.editor.bps.omitHostedTools'),
    description: t('accounts.editor.bps.omitHostedToolsHelp'),
  },
  {
    key: 'ignore_encrypted_history',
    label: t('accounts.editor.bps.ignoreEncryptedHistory'),
    description: t('accounts.editor.bps.ignoreEncryptedHistoryHelp'),
  },
] as const)
</script>

<template>
  <div class="grid gap-4.5 sm:grid-cols-2">
    <ZFormItem v-for="field in fields" :key="field.key" :label="field.label" :description="field.description">
      <ZSwitch
        v-model="options[field.key]"
        :aria-label="field.label"
        :disabled="disabled || (field.key === 'omit_images_when_disabled' && options.image_input_enabled)"
      />
    </ZFormItem>
  </div>
</template>
