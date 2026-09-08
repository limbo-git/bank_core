//! bank-domain —— 银行核心领域层。
//!
//! 铁律:本 crate **零 IO、零时钟**(时间一律由外部注入),保证内核确定性,
//! 是未来分布式"同命令流重放 → 同状态"的前提。

pub mod commands;
pub mod error;
pub mod money;
pub mod types;

pub use commands::{Command, Deposit, OpenAccount, Transfer, Withdraw};
pub use error::DomainError;
pub use money::Money;
pub use types::{CommandStatus, CommandType, LedgerSide};
