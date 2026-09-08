//! 存储层错误模型

use thiserror::Error;

use bank_domain::DomainError;

#[derive(Debug, Error)]
pub enum StoreError {
    /// 领域校验错误(金额非法/自转账等,应在进 store 前拦截,此处兜底)
    #[error("领域错误: {0}")]
    Domain(#[from] DomainError),

    /// 账户不存在
    #[error("账户不存在: {0}")]
    NotFound(String),

    /// 余额不足(含银行现金池不足)
    #[error("余额不足: {0}")]
    InsufficientFunds(String),

    /// 账户状态不允许该操作
    #[error("账户状态不允许操作: {0}")]
    AccountNotActive(String),

    /// 账户已存在(不同命令键重复开户)
    #[error("账户已存在: {0}")]
    AccountAlreadyExists(String),

    /// 内部不一致(科目缺失等,属程序缺陷)
    #[error("内部错误: {0}")]
    Internal(String),

    /// 数据库错误
    #[error("数据库错误: {0}")]
    Db(#[from] sqlx::Error),
}
