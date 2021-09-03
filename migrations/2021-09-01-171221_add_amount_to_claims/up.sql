-- Your SQL goes here
ALTER TABLE claims
ADD COLUMN amount  NUMERIC(38, 0) NOT NULL;
