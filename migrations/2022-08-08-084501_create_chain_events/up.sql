-- Your SQL goes here
CREATE TABLE chain_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  block_height INTEGER NOT NULL,
  block_timestamp TIMESTAMP NOT NULL,
  tx_hash VARCHAR NOT NULL,
  event_index INTEGER NOT NULL,
  contract_address VARCHAR NOT NULL,
  initiator_address VARCHAR NOT NULL,
  event_name VARCHAR NOT NULL,
  event_params JSON NOT NULL,
  processed VARCHAR NOT NULL
);

CREATE UNIQUE INDEX index_block_height_tx_hash_event_index_chain_events ON chain_events (block_height, tx_hash, event_index);
