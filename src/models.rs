use bigdecimal::{BigDecimal};
use chrono::{NaiveDateTime};
use diesel::sql_types::{Text, Numeric};
use serde::{Serialize};
use uuid::Uuid;

use crate::schema::{swaps, liquidity_changes};

#[derive(Debug, Identifiable, Queryable, Serialize)]
pub struct Swap {
  pub id: Uuid,
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

#[derive(Debug, Identifiable, Queryable, Serialize)]
pub struct LiquidityChange {
  pub id: Uuid,
  pub transaction_hash: String,
  pub event_sequence: i32,
  pub block_height: i32,
  pub block_timestamp: NaiveDateTime,
  pub initiator_address: String,
  pub token_address: String,
  pub change_amount: BigDecimal,
}

#[derive(Debug, Insertable)]
#[table_name="liquidity_changes"]
pub struct NewLiquidityChange<'a> {
  pub transaction_hash: &'a str,
  pub event_sequence: &'a i32,
  pub block_height: &'a i32,
  pub block_timestamp: &'a NaiveDateTime,
  pub initiator_address: &'a str,
  pub token_address: &'a str,
  pub change_amount: &'a BigDecimal,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, PartialEq)]
pub struct Liquidity {
  #[sql_type="Text"]
  pub pool: String,
  #[sql_type="Numeric"]
  pub amount: BigDecimal,
}
