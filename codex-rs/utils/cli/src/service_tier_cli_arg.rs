use clap::ValueEnum;
use codex_protocol::config_types::ServiceTier;

/// Service tier to request for model responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ServiceTierCliArg {
    Flex,
    Batch,
    Priority,
    Default,
}

impl From<ServiceTierCliArg> for ServiceTier {
    fn from(value: ServiceTierCliArg) -> Self {
        match value {
            ServiceTierCliArg::Flex => Self::Flex,
            ServiceTierCliArg::Batch => Self::Batch,
            ServiceTierCliArg::Priority => Self::Priority,
            ServiceTierCliArg::Default => Self::Default,
        }
    }
}
