-- 0001: 会计科目表(精简版)+ 期初种子
-- M0 先行落 coa(静态参考数据);账户/命令账/分录等业务表在 M1 引入。

CREATE TABLE coa (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code        text NOT NULL UNIQUE,
    name        text NOT NULL,
    category    text NOT NULL CHECK (category IN ('asset', 'liability', 'equity', 'income', 'expense')),
    normal_side text NOT NULL CHECK (normal_side IN ('debit', 'credit'))
);

COMMENT ON TABLE coa IS '会计科目表(精简版):余额方向由 normal_side 定义';

INSERT INTO coa (code, name, category, normal_side) VALUES
    ('1001', '库存现金',   'asset',     'debit'),
    ('2001', '活期存款',   'liability', 'credit'),
    ('6001', '手续费收入', 'income',    'credit'),
    ('6601', '利息费用',   'expense',   'debit');
