CREATE VIEW pool_txs AS
SELECT 
  swap_0.id "id",
  swap_0.transaction_hash transaction_hash,
  swap_0.block_height block_height,
  swap_0.block_timestamp block_timestamp,
  swap_0.initiator_address initiator_address,
  swap_0.token_address token_address,

  swap_0.token_amount token_amount,
  swap_0.zil_amount zil_amount,
  
  'swap' tx_type,
  
  swap_0.is_sending_zil swap0_is_sending_zil,

  swap_1.token_address swap1_token_address,
  swap_1.id swap1_id,
  swap_1.token_amount swap1_token_amount,
  swap_1.zil_amount swap1_zil_amount,
  swap_1.is_sending_zil swap1_is_sending_zil,
  
  NULL change_amount
  
FROM 
  swaps swap_0
LEFT JOIN
  swaps swap_1
ON
  swap_0.event_sequence = 0
  AND swap_1.event_sequence = 1
  AND swap_0.transaction_hash = swap_1.transaction_hash

UNION

SELECT
  id,
  transaction_hash,
  block_height,
  block_timestamp,
  initiator_address,
  token_address,

  token_amount,
  zil_amount,
  
  'liquidity' tx_type,
  
  NULL swap0_is_sending_zil,

  NULL swap1_token_address,
  NULL swap1_id,
  NULL swap1_token_amount,
  NULL swap1_zil_amount,
  NULL swap1_is_sending_zil,

  change_amount
FROM liquidity_changes;
