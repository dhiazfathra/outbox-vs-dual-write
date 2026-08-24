DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS outbox;

CREATE TABLE orders (
  seq          BIGINT PRIMARY KEY,
  customer     TEXT   NOT NULL,
  amount_cents BIGINT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE outbox (
  seq          BIGINT PRIMARY KEY,
  payload      TEXT   NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  sent_at      TIMESTAMPTZ
);
CREATE INDEX outbox_unsent ON outbox (seq) WHERE sent_at IS NULL;
