//! bank-store —— 存储层:连接、迁移、命令事务脚本、查询。
//!
//! 双角色设计:bank_owner 拥有对象(只用于迁移),bank_app 是应用角色、
//! 只拿最小权限(裸 UPDATE/DELETE 分录的权限从未授予)。角色分离使
//! "分录不可变"不只依赖触发器。凭据见 ~/.env/database.env,PG 仅监听 127.0.0.1。

pub mod commands;
pub mod error;
pub mod queries;

pub use error::StoreError;

use std::env;

use sqlx::PgPool;

/// 应用角色连接 URL(BANK_DB_* 凭据)
pub fn bank_db_url() -> Result<String, String> {
    build_url("BANK_DB_")
}

/// 属主角色连接 URL(BANK_DB_OWNER_*;仅用于执行迁移)
pub fn bank_db_owner_url() -> Result<String, String> {
    build_url("BANK_DB_OWNER_")
}

fn build_url(prefix: &str) -> Result<String, String> {
    let get =
        |k: &str| env::var(k).map_err(|_| format!("缺少环境变量 {k}(见 ~/.env/database.env)"));
    let host = get("BANK_DB_HOST")?;
    let port = get("BANK_DB_PORT")?;
    let name = get("BANK_DB_NAME")?;
    let user = get(&format!("{prefix}USER"))?;
    let password = get(&format!("{prefix}PASSWORD"))?;
    Ok(format!("postgres://{user}:{password}@{host}:{port}/{name}"))
}

/// 执行迁移(幂等:sqlx 以版本表跟踪,可重复运行;须由属主执行)
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}

/// 以属主身份迁移,再以应用角色(最小权限)建立运行池返回。
pub async fn connect_and_migrate() -> Result<PgPool, Box<dyn std::error::Error>> {
    let owner_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&bank_db_owner_url()?)
        .await?;
    migrate(&owner_pool).await?;
    owner_pool.close().await;

    let app_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&bank_db_url()?)
        .await?;
    Ok(app_pool)
}
