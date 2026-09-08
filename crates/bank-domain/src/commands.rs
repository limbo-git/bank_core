//! 命令定义:带校验的构造器,命令面数据从这进领域。
//!
//! 命令结构体字段公开供 store 层读取;合法性在构造时用 `new()` 钉死,
//! 不允许绕过构造器直接赋值(字段虽 pub,约定一律走 new)。

use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::types::CommandType;
use crate::Money;

fn non_empty(s: &str, what: &str) -> Result<(), DomainError> {
    if s.trim().is_empty() {
        Err(DomainError::InvalidArgument(format!("{what}不能为空")))
    } else {
        Ok(())
    }
}

fn positive_amount(cents: i64) -> Result<Money, DomainError> {
    Money::from_cents(cents).ensure_positive()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenAccount {
    pub account_no: String,
    pub holder: Option<String>,
}

impl OpenAccount {
    pub fn new(account_no: impl Into<String>, holder: Option<String>) -> Result<Self, DomainError> {
        let account_no = account_no.into();
        let holder = holder.filter(|h| !h.trim().is_empty());
        non_empty(&account_no, "账号")?;
        Ok(OpenAccount { account_no, holder })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deposit {
    pub account_no: String,
    pub amount: Money,
}

impl Deposit {
    pub fn new(account_no: impl Into<String>, amount_cents: i64) -> Result<Self, DomainError> {
        let account_no = account_no.into();
        non_empty(&account_no, "账号")?;
        Ok(Deposit {
            account_no,
            amount: positive_amount(amount_cents)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Withdraw {
    pub account_no: String,
    pub amount: Money,
}

impl Withdraw {
    pub fn new(account_no: impl Into<String>, amount_cents: i64) -> Result<Self, DomainError> {
        let account_no = account_no.into();
        non_empty(&account_no, "账号")?;
        Ok(Withdraw {
            account_no,
            amount: positive_amount(amount_cents)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transfer {
    pub from_account_no: String,
    pub to_account_no: String,
    pub amount: Money,
}

impl Transfer {
    pub fn new(
        from_account_no: impl Into<String>,
        to_account_no: impl Into<String>,
        amount_cents: i64,
    ) -> Result<Self, DomainError> {
        let from_account_no = from_account_no.into();
        let to_account_no = to_account_no.into();
        non_empty(&from_account_no, "转出账号")?;
        non_empty(&to_account_no, "转入账号")?;
        if from_account_no == to_account_no {
            return Err(DomainError::InvalidArgument(
                "转出与转入账户不能相同".into(),
            ));
        }
        Ok(Transfer {
            from_account_no,
            to_account_no,
            amount: positive_amount(amount_cents)?,
        })
    }
}

/// 统一命令枚举(命令账的落盘类型由 [`Command::command_type`] 给出)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    OpenAccount(OpenAccount),
    Deposit(Deposit),
    Withdraw(Withdraw),
    Transfer(Transfer),
}

impl Command {
    pub fn command_type(&self) -> CommandType {
        match self {
            Command::OpenAccount(_) => CommandType::OpenAccount,
            Command::Deposit(_) => CommandType::Deposit,
            Command::Withdraw(_) => CommandType::Withdraw,
            Command::Transfer(_) => CommandType::Transfer,
        }
    }

    /// 命令涉及的账户号(行锁与存在性检查用;OpenAccount 无既有账户)
    pub fn account_nos(&self) -> Vec<&str> {
        match self {
            Command::OpenAccount(_) => vec![],
            Command::Deposit(d) => vec![d.account_no.as_str()],
            Command::Withdraw(w) => vec![w.account_no.as_str()],
            Command::Transfer(t) => vec![t.from_account_no.as_str(), t.to_account_no.as_str()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 金额与账号校验() {
        assert!(Deposit::new("A", 100).is_ok());
        assert!(Deposit::new("A", 0).is_err());
        assert!(Deposit::new("A", -5).is_err());
        assert!(Deposit::new("  ", 100).is_err());
        assert!(Withdraw::new("A", -1).is_err());
    }

    #[test]
    fn 自转账被拒() {
        let e = Transfer::new("A", "A", 100).unwrap_err();
        assert!(matches!(e, DomainError::InvalidArgument(_)));
    }

    #[test]
    fn 命令类型映射() {
        let cmd = Command::Deposit(Deposit::new("A", 100).unwrap());
        assert_eq!(cmd.command_type(), CommandType::Deposit);
        assert_eq!(cmd.account_nos(), vec!["A"]);
        let t = Command::Transfer(Transfer::new("A", "B", 100).unwrap());
        assert_eq!(t.account_nos(), vec!["A", "B"]);
    }
}
