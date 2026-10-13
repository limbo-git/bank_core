-- 0003: 权限硬化 —— 分录"只追加"由 角色分离 + 最小权限 + 触发器 三层保证。
-- 本迁移由对象属主 bank_owner 执行;应用角色 bank_app 只获得最小权限集。

-- 幂等:先清空应用角色在 public 下的一切权限,再按最小集重授
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM bank_app;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM bank_app;
REVOKE ALL ON SCHEMA public FROM bank_app;

GRANT USAGE ON SCHEMA public TO bank_app;

-- 参考数据 + 分录流水:只读 + 只增;UPDATE / DELETE 一律不授予
GRANT SELECT, INSERT ON coa, ledger_entry TO bank_app;

-- 账户 / 科目投影 / 命令账:业务需要更新投影与命令状态,但仍无 DELETE 权限
GRANT SELECT, INSERT, UPDATE ON account, coa_balance, command_log TO bank_app;

-- identity 自增列需要序列权限
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO bank_app;

-- 迁移版本表对应用角色完全不可见(迁移只由属主执行)
REVOKE ALL ON _sqlx_migrations FROM bank_app;

-- 属主将来新建的对象,默认也授予应用角色最小权限
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT ON TABLES TO bank_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO bank_app;
