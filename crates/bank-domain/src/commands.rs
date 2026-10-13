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
    Reverse(Reverse),
    SettleInterest(SettleInterest),
    RunEod(RunEod),
}

/// 冲正:仅全额、引用原命令、每笔原交易限一次(唯一索引兜底)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reverse {
    pub target_idempotency_key: String,
}

impl Reverse {
    pub fn new(target_idempotency_key: impl Into<String>) -> Result<Self, DomainError> {
        let target_idempotency_key = target_idempotency_key.into();
        non_empty(&target_idempotency_key, "被冲正命令的幂等键")?;
        Ok(Reverse {
            target_idempotency_key,
        })
    }
}

/// 区间计息:按日终余额 × 年利率 ÷ 360 单利,结算日一次入账。
/// 日期为 ISO `YYYY-MM-DD` 字符串(区间左闭右开),口径全部来自命令参数(可重放)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettleInterest {
    pub period_start: String,
    pub period_end: String,
    pub annual_rate_bp: i64,
}

impl SettleInterest {
    pub fn new(
        period_start: impl Into<String>,
        period_end: impl Into<String>,
        annual_rate_bp: i64,
    ) -> Result<Self, DomainError> {
        let period_start = period_start.into();
        let period_end = period_end.into();
        if !is_iso_date(&period_start) || !is_iso_date(&period_end) {
            return Err(DomainError::InvalidArgument(
                "日期必须为 ISO 格式 YYYY-MM-DD".into(),
            ));
        }
        if period_end <= period_start {
            return Err(DomainError::InvalidArgument(
                "计息区间必须左闭右开且 end > start".into(),
            ));
        }
        if !(0..=100_000).contains(&annual_rate_bp) {
            return Err(DomainError::InvalidArgument(format!(
                "年利率(bp,万分之几)须在 0..=100000,收到 {annual_rate_bp}"
            )));
        }
        Ok(SettleInterest {
            period_start,
            period_end,
            annual_rate_bp,
        })
    }
}

/// 日切:对指定日期做日终快照 + 试算平衡(轻量日切)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEod {
    pub day: String,
}

impl RunEod {
    pub fn new(day: impl Into<String>) -> Result<Self, DomainError> {
        let day = day.into();
        if !is_iso_date(&day) {
            return Err(DomainError::InvalidArgument(
                "日期必须为 ISO 格式 YYYY-MM-DD".into(),
            ));
        }
        Ok(RunEod { day })
    }
}

/// 校验 ISO 日期(仅格式,范围交给数据库的 date 类型)
fn is_iso_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..].iter().all(u8::is_ascii_digit)
}

impl Command {
    pub fn command_type(&self) -> CommandType {
        match self {
            Command::OpenAccount(_) => CommandType::OpenAccount,
            Command::Deposit(_) => CommandType::Deposit,
            Command::Withdraw(_) => CommandType::Withdraw,
            Command::Transfer(_) => CommandType::Transfer,
            Command::Reverse(_) => CommandType::Reverse,
            Command::SettleInterest(_) => CommandType::SettleInterest,
            Command::RunEod(_) => CommandType::RunEod,
        }
    }

    /// 命令涉及的账户号(行锁与存在性检查用;仅显式指定账户的命令有值)
    pub fn account_nos(&self) -> Vec<&str> {
        match self {
            Command::OpenAccount(_) => vec![],
            Command::Deposit(d) => vec![d.account_no.as_str()],
            Command::Withdraw(w) => vec![w.account_no.as_str()],
            Command::Transfer(t) => vec![t.from_account_no.as_str(), t.to_account_no.as_str()],
            Command::Reverse(_) | Command::SettleInterest(_) | Command::RunEod(_) => vec![],
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

    #[test]
    fn m3_冲正命令校验() {
        assert!(Reverse::new("key-1").is_ok());
        assert!(Reverse::new("   ").is_err());
        let cmd = Command::Reverse(Reverse::new("k").unwrap());
        assert_eq!(cmd.command_type(), CommandType::Reverse);
        assert!(cmd.account_nos().is_empty());
    }

    #[test]
    fn m3_计息命令校验() {
        assert!(SettleInterest::new("2026-01-01", "2026-01-02", 360).is_ok());
        // 日期格式
        assert!(SettleInterest::new("2026-1-1", "2026-01-02", 360).is_err());
        assert!(SettleInterest::new("2026-01-01", "2026-13-02", 360).is_ok()); // 仅校验格式,范围交给 DB
                                                                               // 区间必须 end > start
        assert!(SettleInterest::new("2026-01-02", "2026-01-01", 360).is_err());
        assert!(SettleInterest::new("2026-01-01", "2026-01-01", 360).is_err());
        // 利率范围
        assert!(SettleInterest::new("2026-01-01", "2026-01-02", -1).is_err());
        assert!(SettleInterest::new("2026-01-01", "2026-01-02", 100_001).is_err());
        assert!(SettleInterest::new("2026-01-01", "2026-01-02", 0).is_ok());
        let cmd =
            Command::SettleInterest(SettleInterest::new("2026-01-01", "2026-01-02", 175).unwrap());
        assert_eq!(cmd.command_type(), CommandType::SettleInterest);
        assert!(cmd.account_nos().is_empty());
    }

    #[test]
    fn m3_日切命令校验() {
        assert!(RunEod::new("2026-01-01").is_ok());
        assert!(RunEod::new("2026/01/01").is_err());
        assert!(RunEod::new("2026-01-1").is_err());
        let cmd = Command::RunEod(RunEod::new("2026-01-01").unwrap());
        assert_eq!(cmd.command_type(), CommandType::RunEod);
        assert!(cmd.account_nos().is_empty());
    }
}
