CREATE TABLE liquidity_changes (
  id BIGSERIAL PRIMARY KEY,
  transaction_hash VARCHAR NOT NULL,
  event_sequence INTEGER NOT NULL,
  block_height INTEGER NOT NULL,
  block_timestamp TIMESTAMP NOT NULL,
  initiator_address VARCHAR NOT NULL,
  token_address VARCHAR NOT NULL,
  change_amount NUMERIC(38, 0) NOT NULL
);

CREATE INDEX index_token_address_on_lc ON liquidity_changes (token_address);
CREATE INDEX index_initiator_address_on_lc ON liquidity_changes (initiator_address);
CREATE INDEX index_block_timestamp_on_lc ON liquidity_changes (block_timestamp DESC);
CREATE UNIQUE INDEX index_event_on_lc ON liquidity_changes (transaction_hash, event_sequence)
