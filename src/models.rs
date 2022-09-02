use bigdecimal::{BigDecimal};
use chrono::{NaiveDateTime};
use diesel::sql_types::{Text, Numeric};
use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::schema::{swaps, liquidity_changes, distributions, claims, pool_txs, block_syncs};

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
  pub token_amount: BigDecimal,
  pub zil_amount: BigDecimal,
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
  pub token_amount: &'a BigDecimal,
  pub zil_amount: &'a BigDecimal,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, Deserialize, PartialEq)]
pub struct Liquidity {
  #[sql_type="Text"]
  pub pool: String,
  #[sql_type="Numeric"]
  pub amount: BigDecimal,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, PartialEq)]
pub struct LiquidityFromProvider {
  #[sql_type="Text"]
  pub pool: String,
  #[sql_type="Text"]
  pub address: String,
  #[sql_type="Numeric"]
  pub amount: BigDecimal,
}

pub type VolumeForUser = LiquidityFromProvider;

#[derive(Debug, Queryable, QueryableByName, Serialize, PartialEq)]
pub struct Volume {
  #[sql_type="Text"]
  pub pool: String,

  // in/out wrt the pool

  // user swap zil for token
  #[sql_type="Numeric"]
  pub in_zil_amount: BigDecimal,
  #[sql_type="Numeric"]
  pub out_token_amount: BigDecimal,

  // user swap token for zil
  #[sql_type="Numeric"]
  pub out_zil_amount: BigDecimal,
  #[sql_type="Numeric"]
  pub in_token_amount: BigDecimal,
}

#[derive(Debug, Identifiable, Queryable, Serialize)]
pub struct PoolTx {
  pub id: Uuid,
  pub transaction_hash: String,
  pub block_height: i32,
  pub block_timestamp: NaiveDateTime,
  pub initiator_address: String,
  pub token_address: String,

  pub token_amount: Option<BigDecimal>,
  pub zil_amount: Option<BigDecimal>,

  pub tx_type: String,

  pub swap0_is_sending_zil: Option<bool>,

  pub swap1_token_address: Option<String>,
  pub swap1_token_amount: Option<BigDecimal>,
  pub swap1_zil_amount: Option<BigDecimal>,
  pub swap1_is_sending_zil: Option<bool>,

  pub change_amount: Option<BigDecimal>,
}

#[derive(Debug, Identifiable, Queryable, QueryableByName, Serialize)]
#[table_name="distributions"]
pub struct Distribution {
  pub id: Uuid,
  pub distributor_address: String,
  pub epoch_number: i32,
  pub address_bech32: String,
  pub address_hex: String,
  pub amount: BigDecimal,
  pub proof: String,
}

#[derive(Debug, Clone, Insertable)]
#[table_name="distributions"]
pub struct NewDistribution<'a> {
  pub distributor_address: &'a str,
  pub epoch_number: &'a i32,
  pub address_bech32: &'a str,
  pub address_hex: &'a str,
  pub amount: &'a BigDecimal,
  pub proof: &'a str,
}

#[derive(Debug, Identifiable, Queryable, Serialize)]
pub struct Claim {
  pub id: Uuid,
  pub transaction_hash: String,
  pub event_sequence: i32,
  pub block_height: i32,
  pub block_timestamp: NaiveDateTime,
  pub initiator_address: String,
  pub distributor_address: String,
  pub epoch_number: i32,
  pub amount: BigDecimal,
}

#[derive(Debug, Clone, Insertable)]
#[table_name="claims"]
pub struct NewClaim<'a> {
  pub transaction_hash: &'a str,
  pub event_sequence: &'a i32,
  pub block_height: &'a i32,
  pub block_timestamp: &'a NaiveDateTime,
  pub initiator_address: &'a str,
  pub distributor_address: &'a str,
  pub epoch_number: &'a i32,
  pub amount: &'a BigDecimal,
}

#[derive(Debug, Clone, Identifiable, Queryable, Serialize)]
pub struct BlockSync {
  pub id: Uuid,
  pub block_height: i32,
  pub block_timestamp: NaiveDateTime,
  pub num_txs: i32,
}

#[derive(Debug, Clone, Insertable)]
#[table_name="block_syncs"]
pub struct NewBlockSync<'a> {
  pub block_height: &'a i32,
  pub block_timestamp: &'a NaiveDateTime,
  pub num_txs: &'a i32,
}
