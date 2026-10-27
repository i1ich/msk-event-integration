CREATE TABLE public.orders (
    id         bigserial PRIMARY KEY,
    account_id varchar(18)   NOT NULL,
    status     varchar(32)   NOT NULL,
    amount     numeric(12,2) NOT NULL,
    currency   char(3)       NOT NULL,
    updated_at timestamptz   NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL: UPDATE/DELETE WAL records carry all old column values,
-- so Debezium "before" images and delete events contain the full old row (not just the PK).
ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- Publication is created here; Debezium runs with publication.autocreate.mode=disabled.
CREATE PUBLICATION dbz_orders_pub FOR TABLE public.orders;

INSERT INTO public.orders (account_id, status, amount, currency) VALUES
    ('001000000000001AAA', 'NEW',      120.50, 'EUR'),
    ('001000000000002AAA', 'PAID',      99.99, 'USD'),
    ('001000000000001AAA', 'SHIPPED', 1500.00, 'EUR');
