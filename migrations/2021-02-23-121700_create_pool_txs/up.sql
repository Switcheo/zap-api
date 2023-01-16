CREATE VIEW pool_txs AS
SELECT 
  id,
  transaction_hash,
  block_height,
  block_timestamp,
  initiator_address,
  pool_address,
  router_address,
  to_address,

  amount_0_in,
  amount_1_in,
  amount_0_out,
  amount_1_out,

  NULL amount_0,
  NULL amount_1,
  NULL liquidity,

  'swap' tx_type
  
FROM swaps

UNION

SELECT
  id,
  transaction_hash,
  block_height,
  block_timestamp,
  initiator_address,
  pool_address,
  router_address,
  NULL to_address,

  amount_0,
  amount_1,
  liquidity,

  NULL amount_0_in,
  NULL amount_1_in,
  NULL amount_0_out,
  NULL amount_1_out,

  'liquidity' tx_type
  
FROM liquidity_changes;
