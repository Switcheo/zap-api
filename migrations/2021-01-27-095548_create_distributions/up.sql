CREATE TABLE distributions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  epoch_number INTEGER NOT NULL,
  address_bech32 VARCHAR NOT NULL,
  address_hex VARCHAR NOT NULL,
  amount NUMERIC(38, 0) NOT NULL,
  proof VARCHAR NOT NULL
);

CREATE INDEX index_epoch_number_on_distr ON distributions (epoch_number);
CREATE INDEX index_address_bech32_on_distr ON distributions (address_bech32);
CREATE UNIQUE INDEX index_epoch_address_on_distr ON distributions (epoch_number, address_hex)
