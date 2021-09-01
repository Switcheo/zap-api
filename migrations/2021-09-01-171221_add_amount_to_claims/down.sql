-- This file should undo anything in `up.sql`
ALTER TABLE claims
REMOVE COLUMN amount;
