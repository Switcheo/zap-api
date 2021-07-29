-- Your SQL goes here-- Your SQL goes here
CREATE TABLE backfill_completions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contract_address VARCHAR NOT NULL,
  event_name VARCHAR NOT NULL
);

CREATE UNIQUE INDEX index_address_event_on_backfill_completions ON backfill_completions (contract_address, event_name);
