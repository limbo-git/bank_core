//! 领域错误模型。后续 store/api 层错误在此之上逐层包装。

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// 金额运算溢出(如 i64 分溢出)
    #[error("金额运算溢出: {0}")]
    Overflow(String),

    /// 金额必须为正数(方向由借贷表达,不允许负金额)
    #[error("金额必须大于零, 收到 {0}")]
    NotPositive(String),

    /// 参数非法(空账号、自转账、未知命令类型等)
    #[error("非法参数: {0}")]
    InvalidArgument(String),
}
