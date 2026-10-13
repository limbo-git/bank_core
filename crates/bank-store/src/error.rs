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

    /// 被冲正的原命令不存在
    #[error("被冲正命令不存在: {0}")]
    OriginalNotFound(String),

    /// 该命令不可冲正(未提交/开户命令/冲正命令)
    #[error("命令不可冲正: {0}")]
    NotReversible(String),

    /// 原交易已被冲正过一次
    #[error("原交易已被冲正: {0}")]
    AlreadyReversed(String),

    /// 冲正会导致余额/现金池为负(钱已被花掉)
    #[error("冲正会导致透支,拒绝执行: {0}")]
    WouldOverdraw(String),

    /// 该计息区间已结算过
    #[error("计息区间已结算: {0}")]
    PeriodAlreadySettled(String),

    /// 该日已完成日切
    #[error("该日已日切: {0}")]
    DayAlreadyClosed(String),

    /// 内部不一致(科目缺失等,属程序缺陷)
    #[error("内部错误: {0}")]
    Internal(String),

    /// 数据库错误
    #[error("数据库错误: {0}")]
    Db(#[from] sqlx::Error),
}
