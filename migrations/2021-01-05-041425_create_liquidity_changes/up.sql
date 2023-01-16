CREATE TABLE liquidity_changes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_hash VARCHAR NOT NULL,
  event_sequence INTEGER NOT NULL,
  block_height INTEGER NOT NULL,
  block_timestamp TIMESTAMP NOT NULL,
  initiator_address VARCHAR NOT NULL,
  pool_address VARCHAR NOT NULL,
  router_address VARCHAR NOT NULL,
  amount_0 NUMERIC(38, 0) NOT NULL,
  amount_1 NUMERIC(38, 0) NOT NULL,
  liquidity NUMERIC(38, 0) NOT NULL
);

CREATE INDEX index_pool_address_on_lc ON liquidity_changes (pool_address);
CREATE INDEX index_initiator_address_on_lc ON liquidity_changes (initiator_address);
CREATE INDEX index_block_timestamp_on_lc ON liquidity_changes (block_timestamp DESC);
CREATE UNIQUE INDEX index_event_on_lc ON liquidity_changes (transaction_hash, event_sequence)
