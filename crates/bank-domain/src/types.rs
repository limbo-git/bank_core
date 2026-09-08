//! 领域基础类型:借贷方向、命令类型、命令状态。
//!
//! as_str() 的值即 DB 落盘值(见 migrations),两者必须一致——由集成测试把关。

use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// 分录借贷方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LedgerSide {
    Debit,
    Credit,
}

impl LedgerSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            LedgerSide::Debit => "debit",
            LedgerSide::Credit => "credit",
        }
    }
}

/// 命令类型(命令面,见 PROJECT_PLAN §5)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommandType {
    OpenAccount,
    Deposit,
    Withdraw,
    Transfer,
    Reverse,
    SettleInterest,
    RunEod,
}

impl CommandType {
    pub const fn as_str(self) -> &'static str {
        match self {
            CommandType::OpenAccount => "open_account",
            CommandType::Deposit => "deposit",
            CommandType::Withdraw => "withdraw",
            CommandType::Transfer => "transfer",
            CommandType::Reverse => "reverse",
            CommandType::SettleInterest => "settle_interest",
            CommandType::RunEod => "run_eod",
        }
    }
}

/// 反查落盘字符串 → 枚举(api 层解析命令类型用)
impl TryFrom<&str> for CommandType {
    type Error = DomainError;

    fn try_from(v: &str) -> Result<Self, Self::Error> {
        Ok(match v {
            "open_account" => CommandType::OpenAccount,
            "deposit" => CommandType::Deposit,
            "withdraw" => CommandType::Withdraw,
            "transfer" => CommandType::Transfer,
            "reverse" => CommandType::Reverse,
            "settle_interest" => CommandType::SettleInterest,
            "run_eod" => CommandType::RunEod,
            _ => return Err(DomainError::InvalidArgument(format!("未知命令类型: {v}"))),
        })
    }
}

/// 命令状态机:received → committed / failed;进程/网络故障时落 outcome_unknown
/// ("已提交但结果未知"),客户端带同幂等键重查即可收敛。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommandStatus {
    Received,
    Committed,
    OutcomeUnknown,
    Failed,
}

impl CommandStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            CommandStatus::Received => "received",
            CommandStatus::Committed => "committed",
            CommandStatus::OutcomeUnknown => "outcome_unknown",
            CommandStatus::Failed => "failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 落盘值与枚举一致() {
        assert_eq!(LedgerSide::Debit.as_str(), "debit");
        assert_eq!(LedgerSide::Credit.as_str(), "credit");
        assert_eq!(CommandType::OpenAccount.as_str(), "open_account");
        assert_eq!(CommandType::RunEod.as_str(), "run_eod");
        assert_eq!(CommandStatus::OutcomeUnknown.as_str(), "outcome_unknown");
        assert_eq!(CommandStatus::Committed.as_str(), "committed");
    }
}
