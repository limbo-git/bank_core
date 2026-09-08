//! 金额类型:内部一律 i64 最小货币单位(分),零浮点。
//!
//! 全链路禁止裸 i64/浮点与金额混用;金额方向由借贷表达,故余额可为负的
//! 场景由投影层各自约束(客户存款余额 >= 0 是 DB CHECK 的事)。

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::DomainError;

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Money(i64);

impl Money {
    pub const ZERO: Money = Money(0);

    /// 从最小货币单位(分)构造
    pub const fn from_cents(cents: i64) -> Self {
        Money(cents)
    }

    /// 取最小货币单位(分)
    pub const fn cents(self) -> i64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn is_positive(self) -> bool {
        self.0 > 0
    }

    pub fn is_negative(self) -> bool {
        self.0 < 0
    }

    /// 命令金额校验:必须 > 0(负数与零一律拒绝)
    pub fn ensure_positive(self) -> Result<Self, DomainError> {
        if self.0 > 0 {
            Ok(self)
        } else {
            Err(DomainError::NotPositive(format!("{self}")))
        }
    }

    pub fn checked_add(self, other: Self) -> Result<Self, DomainError> {
        self.0
            .checked_add(other.0)
            .map(Money)
            .ok_or_else(|| DomainError::Overflow(format!("{self} + {other}")))
    }

    pub fn checked_sub(self, other: Self) -> Result<Self, DomainError> {
        self.0
            .checked_sub(other.0)
            .map(Money)
            .ok_or_else(|| DomainError::Overflow(format!("{self} - {other}")))
    }
}

impl fmt::Display for Money {
    /// 例:12345 分 → "¥123.45";-5 分 → "-¥0.05"
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v = self.0;
        let (sign, abs) = if v < 0 {
            ("-", v.unsigned_abs())
        } else {
            ("", v as u64)
        };
        write!(f, "{sign}¥{}.{:02}", abs / 100, abs % 100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn display_正常与负数() {
        assert_eq!(Money::from_cents(12345).to_string(), "¥123.45");
        assert_eq!(Money::from_cents(100000).to_string(), "¥1000.00");
        assert_eq!(Money::from_cents(0).to_string(), "¥0.00");
        assert_eq!(Money::from_cents(-5).to_string(), "-¥0.05");
    }

    #[test]
    fn ensure_positive_拒绝零与负() {
        assert!(Money::ZERO.ensure_positive().is_err());
        assert!(Money::from_cents(-1).ensure_positive().is_err());
        assert_eq!(
            Money::from_cents(1).ensure_positive(),
            Ok(Money::from_cents(1))
        );
    }

    #[test]
    fn 溢出边界() {
        let max = Money::from_cents(i64::MAX);
        let min = Money::from_cents(i64::MIN);
        assert!(max.checked_add(Money::from_cents(1)).is_err());
        assert!(min.checked_sub(Money::from_cents(1)).is_err());
        assert!(max.checked_add(Money::ZERO).is_ok());
    }

    proptest! {
        #[test]
        fn 加减与原生_i64_语义一致(a: i64, b: i64) {
            let ma = Money::from_cents(a);
            let mb = Money::from_cents(b);
            match a.checked_add(b) {
                Some(sum) => assert_eq!(ma.checked_add(mb).unwrap().cents(), sum),
                None => assert!(ma.checked_add(mb).is_err()),
            }
            match a.checked_sub(b) {
                Some(diff) => assert_eq!(ma.checked_sub(mb).unwrap().cents(), diff),
                None => assert!(ma.checked_sub(mb).is_err()),
            }
        }

        #[test]
        fn 正金额校验(amount: i64) {
            let ok = Money::from_cents(amount).ensure_positive().is_ok();
            assert_eq!(ok, amount > 0);
        }
    }
}
