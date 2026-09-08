-- 0002: 核心账务表 —— 命令账 / 账户 / 科目余额 / 分录流水(不可变)

-- 客户账户(负债类科目 2001"活期存款"的明细账;balance_cents 为客户可用余额,>= 0)
CREATE TABLE account (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_no    text NOT NULL UNIQUE,
    holder        text,
    type          text NOT NULL DEFAULT 'current',      -- 预留:多账户类型
    currency      char(3) NOT NULL DEFAULT 'CNY',       -- 预留:多币种
    coa_id        bigint NOT NULL REFERENCES coa(id),
    status        text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'frozen', 'closed')),
    balance_cents bigint NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    opened_at     timestamptz NOT NULL DEFAULT now()
);

-- 科目余额投影(口径:balance_cents >= 0,符号约定为"正数 = 科目正常方向余额";
-- credit-normal 科目如 2001/6001 记 credit 增加,debit-normal 如 1001/6601 记 debit 增加)
CREATE TABLE coa_balance (
    coa_id        bigint PRIMARY KEY REFERENCES coa(id),
    balance_cents bigint NOT NULL DEFAULT 0 CHECK (balance_cents >= 0)
);

INSERT INTO coa_balance (coa_id, balance_cents)
SELECT id, 0 FROM coa;

-- 命令账:幂等锚点 + 审计线索 + 未来事件源(三合一)
CREATE TABLE command_log (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    command_type    text NOT NULL,
    payload         jsonb NOT NULL,
    status          text NOT NULL DEFAULT 'received'
                    CHECK (status IN ('received', 'committed', 'outcome_unknown', 'failed')),
    result          jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    committed_at    timestamptz
);

-- 分录流水:真相源,只追加(触发器禁止 UPDATE/DELETE)
-- 记账位约定:coa_id 必填;account_id 为空 = 银行内部科目记账位(现金/费用等);
--            account_id 非空 = 客户账户明细(其 coa_id 即账户所属科目)。
-- 跨列约束(客户明细必须挂 2001 之类)由应用层保证 + M2 对账工具兜底复核。
CREATE TABLE ledger_entry (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    command_id          bigint NOT NULL REFERENCES command_log(id),
    seq                 smallint NOT NULL CHECK (seq >= 0),
    coa_id              bigint NOT NULL REFERENCES coa(id),
    account_id          bigint REFERENCES account(id),
    side                text NOT NULL CHECK (side IN ('debit', 'credit')),
    amount_cents        bigint NOT NULL CHECK (amount_cents > 0),
    reverses_command_id bigint REFERENCES command_log(id),   -- 冲正引用(M3 使用)
    booked_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (command_id, seq)
);

CREATE INDEX idx_ledger_entry_account ON ledger_entry (account_id, id);
CREATE INDEX idx_ledger_entry_coa     ON ledger_entry (coa_id, id);
CREATE INDEX idx_ledger_entry_rev     ON ledger_entry (reverses_command_id) WHERE reverses_command_id IS NOT NULL;

-- 分录不可变铁律
CREATE FUNCTION ledger_entry_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entry 不可变: 禁止 % 操作 (id=%)', TG_OP, OLD.id;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_entry_immutable
    BEFORE UPDATE OR DELETE ON ledger_entry
    FOR EACH ROW EXECUTE FUNCTION ledger_entry_immutable();

COMMENT ON TABLE command_log  IS '命令账:幂等锚点+审计线索+未来事件源;received/committed/outcome_unknown/failed';
COMMENT ON TABLE ledger_entry IS '分录流水:真相源,只追加;借贷平衡由应用保证并对账复核';
COMMENT ON TABLE coa_balance  IS '科目余额投影(正数=正常方向);试算平衡校验 Σ(debit类)=Σ(credit类)';
