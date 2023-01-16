CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION pgcrypto;

CREATE TABLE swaps (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_hash VARCHAR NOT NULL,
  event_sequence INTEGER NOT NULL,
  block_height INTEGER NOT NULL,
  block_timestamp TIMESTAMP NOT NULL,
  initiator_address VARCHAR NOT NULL,
  pool_address VARCHAR NOT NULL,
  router_address VARCHAR NOT NULL,
  to_address VARCHAR NOT NULL,
  amount_0_in NUMERIC(38, 0) NOT NULL,
  amount_1_in NUMERIC(38, 0) NOT NULL,
  amount_0_out NUMERIC(38, 0) NOT NULL,
  amount_1_out NUMERIC(38, 0) NOT NULL
);

CREATE INDEX index_pool_address_on_swaps ON swaps (pool_address);
CREATE INDEX index_initiator_address_on_swaps ON swaps (initiator_address);
CREATE INDEX index_block_timestamp_on_swaps ON swaps (block_timestamp DESC);
CREATE UNIQUE INDEX index_event_on_swaps ON swaps (transaction_hash, event_sequence)
