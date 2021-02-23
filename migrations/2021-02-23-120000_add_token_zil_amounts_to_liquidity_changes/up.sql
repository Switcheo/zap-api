-- Your SQL goes here

ALTER TABLE liquidity_changes 
  ADD COLUMN token_amount NUMERIC(38, 0) NOT NULL DEFAULT 0,
  ADD COLUMN zil_amount NUMERIC(38, 0) NOT NULL DEFAULT 0;
