-- Your SQL goes here
ALTER TABLE distributions
ADD COLUMN distributor_address VARCHAR NOT NULL DEFAULT '0xca6d3f56218aaa89cd20406cf22aee26ba8f6089';

ALTER TABLE distributions
ALTER COLUMN distributor_address DROP DEFAULT;

CREATE UNIQUE INDEX index_unique_on_distr ON distributions (distributor_address, epoch_number, address_hex);

DROP INDEX index_epoch_number_on_distr;
DROP INDEX index_epoch_address_on_distr;
