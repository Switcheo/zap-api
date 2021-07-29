-- Your SQL goes here
CREATE TABLE claims (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_hash VARCHAR NOT NULL,
  event_sequence INTEGER NOT NULL,
  block_height INTEGER NOT NULL,
  block_timestamp TIMESTAMP NOT NULL,
  distributor_address VARCHAR NOT NULL,
  epoch_number INTEGER NOT NULL,
  initiator_address VARCHAR NOT NULL
);

CREATE INDEX index_initiator_address_on_claim ON claims (initiator_address);
CREATE UNIQUE INDEX index_unique_claim ON claims (distributor_address, epoch_number, initiator_address);
