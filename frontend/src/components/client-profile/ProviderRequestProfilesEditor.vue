<script setup lang="ts">
import type { ClientProfileSelection, ProviderRequestProfile, ProviderRequestProfiles, XaiClientProfileSelection } from '@/api/modules/settings/profiles'

import { ZSegmented } from '@codex-proxy/ui'
import { computed, shallowRef } from 'vue'
import { useI18n } from 'vue-i18n'
import { formatProviderLabel, providerIcon } from '@/utils/providers'
import ClientProfileEditor from './ClientProfileEditor.vue'
import XaiClientProfileEditor from './XaiClientProfileEditor.vue'

withDefaults(defineProps<{
  active?: boolean
  disabled?: boolean
  allowInherit?: boolean
}>(), {
  active: true,
  disabled: false,
  allowInherit: false,
})

const { t } = useI18n({ useScope: 'global' })

const model = defineModel<ProviderRequestProfiles>({ required: true })
// 这里只展示管理 API 支持的可选画像；BPS 使用固定的 Excel 请求身份
const profileProviderIds = ['openai', 'xai'] as const
const provider = shallowRef<(typeof profileProviderIds)[number]>('openai')
const providerOptions = profileProviderIds.map(value => ({
  value,
  label: formatProviderLabel(value),
  icon: providerIcon(value),
}))
const providerSelectorStyle = { width: `${providerOptions.length * 40}px` }
const openai = profileModel<ClientProfileSelection>('openai')
const xai = profileModel<XaiClientProfileSelection>('xai')

function profileModel<T extends object>(providerId: string) {
  return computed<T | null>({
    get: () => (model.value[providerId] as T | undefined) ?? null,
    set: value => updateProfile(providerId, value as ProviderRequestProfile | null),
  })
}

function updateProfile(providerId: string, value: ProviderRequestProfile | null) {
  const profiles = { ...model.value }
  if (value === null)
    delete profiles[providerId]
  else
    profiles[providerId] = value
  model.value = profiles
}
</script>

<template>
  <div class="grid min-w-0 gap-4">
    <div v-if="!allowInherit" class="flex flex-wrap items-start justify-between gap-3">
      <slot name="heading" />
      <ZSegmented
        v-model="provider"
        class="ml-auto max-w-full shrink-0"
        :aria-label="t('settings.clientProfile.clientIdentityProvider')"
        :options="providerOptions"
        :disabled="disabled"
        :style="providerSelectorStyle"
        display="icon"
      />
    </div>
    <ClientProfileEditor
      v-if="provider === 'openai'"
      v-model="openai"
      :allow-inherit="allowInherit"
      :active="active"
      :disabled="disabled"
    >
      <template #source-extra>
        <ZSegmented
          v-model="provider"
          class="max-w-full shrink-0"
          :aria-label="t('settings.clientProfile.upstreamIdentityPlatform')"
          :options="providerOptions"
          :disabled="disabled"
          :style="providerSelectorStyle"
          display="icon"
        />
      </template>
    </ClientProfileEditor>
    <XaiClientProfileEditor
      v-else
      v-model="xai"
      :allow-inherit="allowInherit"
      :active="active"
      :disabled="disabled"
    >
      <template #source-extra>
        <ZSegmented
          v-model="provider"
          class="max-w-full shrink-0"
          :aria-label="t('settings.clientProfile.upstreamIdentityPlatform')"
          :options="providerOptions"
          :disabled="disabled"
          :style="providerSelectorStyle"
          display="icon"
        />
      </template>
    </XaiClientProfileEditor>
  </div>
</template>
