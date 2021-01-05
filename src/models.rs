use serde::{Serialize};
use chrono::{NaiveDateTime};
use bigdecimal::{BigDecimal};

use crate::schema::{swaps};

#[derive(Debug, Identifiable, Queryable, Serialize)]
pub struct Swap {
  pub id: i64,
  pub transaction_hash: String,
  pub event_sequence: i32,
  pub block_height: i32,
  pub block_timestamp: NaiveDateTime,
  pub initiator_address: String,
  pub token_address: String,
  pub token_amount: BigDecimal,
  pub zil_amount: BigDecimal,
  pub is_sending_zil: bool,
}

#[derive(Debug, Insertable)]
#[table_name="swaps"]
pub struct NewSwap<'a> {
  pub transaction_hash: &'a str,
  pub event_sequence: &'a i32,
  pub block_height: &'a i32,
  pub block_timestamp: &'a NaiveDateTime,
  pub initiator_address: &'a str,
  pub token_address: &'a str,
  pub token_amount: &'a BigDecimal,
  pub zil_amount: &'a BigDecimal,
  pub is_sending_zil: &'a bool,
}
