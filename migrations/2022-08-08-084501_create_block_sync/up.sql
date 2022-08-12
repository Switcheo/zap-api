-- Your SQL goes here
CREATE TABLE block_syncs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  block_height INTEGER NOT NULL,
  block_timestamp TIMESTAMP NOT NULL,
  num_txs INTEGER NOT NULL
);

CREATE UNIQUE INDEX index_block_height_chain_events ON block_syncs (block_height);
