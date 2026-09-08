//! bank-store —— 存储层:连接、迁移、命令事务脚本、查询。

pub mod commands;
pub mod error;
pub mod queries;

pub use error::StoreError;

use std::env;

use sqlx::PgPool;

/// 从环境变量拼出连接 URL(BANK_DB_* 凭据,见 ~/.env/database.env)
pub fn bank_db_url() -> Result<String, String> {
    let get =
        |k: &str| env::var(k).map_err(|_| format!("缺少环境变量 {k}(见 ~/.env/database.env)"));
    let host = get("BANK_DB_HOST")?;
    let port = get("BANK_DB_PORT")?;
    let name = get("BANK_DB_NAME")?;
    let user = get("BANK_DB_USER")?;
    let password = get("BANK_DB_PASSWORD")?;
    Ok(format!("postgres://{user}:{password}@{host}:{port}/{name}"))
}

/// 执行迁移(幂等:sqlx 以版本表跟踪,可重复运行)
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}

/// 连接数据库并执行迁移
pub async fn connect_and_migrate() -> Result<PgPool, Box<dyn std::error::Error>> {
    let url = bank_db_url()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await?;
    migrate(&pool).await?;
    Ok(pool)
}
