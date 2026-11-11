//! Basispoints 独立 Provider，持有 Excel OAuth、账号目录和上游传输

mod admin;
mod catalog;
mod config;
mod credential;
mod error;
mod feedback;
mod images;
mod network;
mod oauth;
mod pricing;
mod provider;
mod quota;
mod stream;
mod transport;
mod workers;

pub use config::{BpsConfig, BpsUpstreamTransport};
pub use error::BpsError;

use gateway_admin::ports::provider::ProviderAdmin;
use gateway_core::{
    engine::provider::Provider, provider_ports::ProviderStorePorts, task::WorkerContribution,
};
use std::sync::Arc;

/// 与组合根交换的执行、管理和后台任务能力
pub struct ProviderBundle {
    provider: Arc<provider::BpsProvider>,
    workers: Vec<WorkerContribution>,
}

impl ProviderBundle {
    /// 返回执行注册入口
    #[must_use]
    pub fn core_provider(&self) -> Arc<dyn Provider> {
        self.provider.clone()
    }
    /// 返回管理注册入口
    #[must_use]
    pub fn admin_provider(&self) -> Arc<dyn ProviderAdmin> {
        self.provider.clone()
    }
    /// 转移后台任务给 Host 监督
    pub fn take_worker_contributions(&mut self) -> Vec<WorkerContribution> {
        std::mem::take(&mut self.workers)
    }
}

/// 组装 Provider；账号事实仍由调用方提供的 Store 端口持有
///
/// # Errors
/// 配置或后台任务注册失败时返回安全分类
pub fn initialize(
    config: BpsConfig,
    ports: ProviderStorePorts,
) -> Result<ProviderBundle, BpsError> {
    config.validate()?;
    let provider = Arc::new(provider::BpsProvider::new(config, ports));
    let workers = workers::contributions(provider.clone())?;
    Ok(ProviderBundle { provider, workers })
}
