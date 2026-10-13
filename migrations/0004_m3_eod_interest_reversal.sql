-- 0004: M3 表结构 —— 日终快照 / 日终余额(计息基础)/ 计息结算台账 / 冲正唯一性

-- 日终快照(轻量日切):每日一行,记录试算平衡结果;closed_at 由 day 推导,保证可重放
CREATE TABLE eod_snapshot (
    day       date PRIMARY KEY,
    closed_at timestamptz NOT NULL,
    balanced  boolean NOT NULL
);
COMMENT ON TABLE eod_snapshot IS '轻量日切:每日一条;closed_at 由 day 推导(确定性,可重放)';

-- 日终余额(区间计息的"日积数"来源)
CREATE TABLE account_daily_balance (
    account_id    bigint NOT NULL REFERENCES account(id),
    day           date NOT NULL,
    balance_cents bigint NOT NULL CHECK (balance_cents >= 0),
    PRIMARY KEY (account_id, day)
);
COMMENT ON TABLE account_daily_balance IS '日终余额:区间计息 = Σ(日终余额 × 年利率) ÷ (10000 × 360)';

-- 计息结算台账:同一区间只能结算一次(防重复计提,业务级幂等)
CREATE TABLE interest_settlement (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    period_start date NOT NULL,
    period_end   date NOT NULL,
    annual_rate_bp integer NOT NULL CHECK (annual_rate_bp >= 0),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT interest_period_unique UNIQUE (period_start, period_end),
    CONSTRAINT interest_period_order CHECK (period_end > period_start)
);
COMMENT ON TABLE interest_settlement IS '计息结算台账:UNIQUE(period_start, period_end) 防止同区间重复计提';

-- 冲正唯一性:每笔原交易只能被冲正一次(部分唯一索引,null 不参与)
CREATE UNIQUE INDEX idx_ledger_entry_one_reversal
    ON ledger_entry (reverses_command_id)
    WHERE reverses_command_id IS NOT NULL;
