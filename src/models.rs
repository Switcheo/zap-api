use serde::{Serialize};
use std::time::{SystemTime};
use bigdecimal::{BigDecimal};

#[derive(Queryable, Serialize)]
pub struct Swap {
  id: i64,
  pub transaction_hash: String,
  pub event_sequence: i32,
  pub block_height: i32,
  pub block_timestamp: SystemTime,
  pub initiator_address: String,
  pub token_address: String,
  pub token_amount: BigDecimal,
  pub zil_amount: BigDecimal,
  pub is_sending_zil: bool,
}
