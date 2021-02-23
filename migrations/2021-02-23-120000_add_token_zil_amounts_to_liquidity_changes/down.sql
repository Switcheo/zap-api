-- This file should undo anything in `up.sql`
ALTER TABLE table_name 
  DROP COLUMN token_amount,
  DROP COLUMN zil_amount;
