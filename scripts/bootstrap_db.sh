#!/usr/bin/env bash
# 幂等的数据库初始化:角色 / 库 / 属主移交 / 权限前置。
# 需要 superuser 凭据(~/.env/database.env 的 PG_SUPERUSER*)。
#
# 设计:bank_owner 拥有全部对象(只用于迁移);bank_app 是应用角色,
# 只拿最小权限(见 migrations/0003_hardening.sql)。属主天然绕过权限,
# 因此"分录不可变"必须靠 角色分离 + REVOKE + 触发器 三层,而非单靠触发器。
set -euo pipefail

ENVF="$HOME/.env/database.env"
# shellcheck disable=SC1090
source "$ENVF"
OWNER_USER="${BANK_DB_OWNER_USER:-bank_owner}"
APP_USER="${BANK_DB_USER:-bank_app}"
DB_LIST="bank_core bank_core_test bank_core_replay bank_core_bench bank_core_bench_raw"

psql_m() { PGPASSWORD="$PG_SUPERUSER_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$PG_SUPERUSER" -v ON_ERROR_STOP=1 -q "$@"; }

rand_pw() { python3 -c "import secrets;print(secrets.token_hex(24))"; }

ensure_login_role() { # $1=角色名 $2=env 用户变量名 $3=env 密码变量名
  local role=$1 uvar=$2 pvar=$3 pw
  if [ -z "$(psql_m -tAc "SELECT 1 FROM pg_roles WHERE rolname='$role'")" ]; then
    pw=$(rand_pw)
    psql_m -c "CREATE ROLE $role LOGIN PASSWORD '$pw'" >/dev/null
    echo "已创建角色: $role"
    if ! grep -q "^${uvar}=" "$ENVF"; then
      printf '%s=%s\n%s=%s\n' "$uvar" "$role" "$pvar" "$pw" >> "$ENVF"
      chmod 600 "$ENVF"
      echo "凭据已写入 $ENVF ($uvar/$pvar)"
    fi
  else
    echo "角色已存在: $role"
  fi
}

ensure_login_role "$OWNER_USER" BANK_DB_OWNER_USER BANK_DB_OWNER_PASSWORD
ensure_login_role "$APP_USER" BANK_DB_USER BANK_DB_PASSWORD

for db in $DB_LIST; do
  if [ -z "$(psql_m -tAc "SELECT 1 FROM pg_database WHERE datname='$db'")" ]; then
    psql_m -c "CREATE DATABASE $db OWNER $OWNER_USER" >/dev/null
    echo "已创建库: $db(属主 $OWNER_USER)"
  else
    # PG15+ 下 public schema 归"数据库属主"所有,库属主必须一并移交
    psql_m -c "ALTER DATABASE $db OWNER TO $OWNER_USER" >/dev/null
    echo "已移交库属主: $db → $OWNER_USER"
  fi
  echo "--- $db:移交对象属主给 $OWNER_USER ---"
  PGPASSWORD="$PG_SUPERUSER_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$PG_SUPERUSER" -v ON_ERROR_STOP=1 -q -d "$db" <<SQL
DO \$\$
DECLARE r record;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='public' LOOP
    EXECUTE format('ALTER TABLE public.%I OWNER TO $OWNER_USER', r.tablename);
  END LOOP;
  FOR r IN SELECT sequencename FROM pg_sequences WHERE schemaname='public' LOOP
    EXECUTE format('ALTER SEQUENCE public.%I OWNER TO $OWNER_USER', r.sequencename);
  END LOOP;
END \$\$;
SQL
done

echo "bootstrap 完成 ✅"
