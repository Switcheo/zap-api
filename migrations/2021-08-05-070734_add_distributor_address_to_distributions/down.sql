-- This file should undo anything in `up.sql`
-- Your SQL goes here
ALTER TABLE distributions
REMOVE COLUMN distributor_address;

CREATE INDEX index_epoch_number_on_distr ON distributions (epoch_number);
CREATE UNIQUE INDEX index_epoch_address_on_distr ON distributions (epoch_number, address_hex);

DROP INDEX index_unique_on_distr;
