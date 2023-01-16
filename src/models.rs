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
  pub pool_address: String,
  pub router_address: String,
  pub to_address: String,
  pub amount_0_in: BigDecimal,
  pub amount_1_in: BigDecimal,
  pub amount_0_out: BigDecimal,
  pub amount_1_out: BigDecimal,
}

#[derive(Debug, Insertable)]
#[table_name="swaps"]
pub struct NewSwap<'a> {
  pub transaction_hash: &'a str,
  pub event_sequence: &'a i32,
  pub block_height: &'a i32,
  pub block_timestamp: &'a NaiveDateTime,
  pub initiator_address: &'a str,
  pub pool_address: &'a str,
  pub router_address: &'a str,
  pub to_address: &'a str,
  pub amount_0_in: &'a BigDecimal,
  pub amount_1_in: &'a BigDecimal,
  pub amount_0_out: &'a BigDecimal,
  pub amount_1_out: &'a BigDecimal,
}

#[derive(Debug, Identifiable, Queryable, Serialize)]
pub struct LiquidityChange {
  pub id: Uuid,
  pub transaction_hash: String,
  pub event_sequence: i32,
  pub block_height: i32,
  pub block_timestamp: NaiveDateTime,
  pub initiator_address: String,
  pub pool_address: String,
  pub router_address: String,
  pub amount_0: BigDecimal,
  pub amount_1: BigDecimal,
  pub liquidity: BigDecimal,
}

#[derive(Debug, Insertable)]
#[table_name="liquidity_changes"]
pub struct NewLiquidityChange<'a> {
  pub transaction_hash: &'a str,
  pub event_sequence: &'a i32,
  pub block_height: &'a i32,
  pub block_timestamp: &'a NaiveDateTime,
  pub initiator_address: &'a str,
  pub pool_address: &'a str,
  pub router_address: &'a str,
  pub amount_0: &'a BigDecimal,
  pub amount_1: &'a BigDecimal,
  pub liquidity: &'a BigDecimal,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, Deserialize, PartialEq)]
pub struct Liquidity {
  #[sql_type="Text"]
  pub pool: String,
  #[sql_type="Numeric"]
  pub amount_0: BigDecimal,
  #[sql_type="Numeric"]
  pub amount_1: BigDecimal,
  #[sql_type="Numeric"]
  pub liquidity: BigDecimal,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, PartialEq)]
pub struct LiquidityFromProvider {
  #[sql_type="Text"]
  pub pool: String,
  #[sql_type="Text"]
  pub address: String,
  #[sql_type="Numeric"]
  pub liquidity: BigDecimal,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, PartialEq)]
pub struct VolumeForUser {
  #[sql_type="Text"]
  pub pool: String,
  #[sql_type="Text"]
  pub address: String,
  #[sql_type="Numeric"]
  pub amount_0: BigDecimal,
  #[sql_type="Numeric"]
  pub amount_1: BigDecimal,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, PartialEq)]
pub struct Volume {
  #[sql_type="Text"]
  pub pool: String,

  // in/out wrt the pool

  #[sql_type="Numeric"]
  pub total_amount_0_in: BigDecimal,
  #[sql_type="Numeric"]
  pub total_amount_1_in: BigDecimal,

  #[sql_type="Numeric"]
  pub total_amount_0_out: BigDecimal,
  #[sql_type="Numeric"]
  pub total_amount_1_out: BigDecimal,

  // #[sql_type="Numeric"]
  // pub liquidity: BigDecimal,
}

#[derive(Debug, Identifiable, Queryable, Serialize)]
pub struct PoolTx {
  pub id: Uuid,
  pub transaction_hash: String,
  pub block_height: i32,
  pub block_timestamp: NaiveDateTime,
  pub initiator_address: String,
  pub pool_address: String,
  pub router_address: String,
  pub to_address: Option<String>,

  pub amount_0: Option<BigDecimal>,
  pub amount_1: Option<BigDecimal>,
  pub liquidity: Option<BigDecimal>,

  pub tx_type: String,

  pub amount_0_in: Option<BigDecimal>,
  pub amount_1_in: Option<BigDecimal>,
  pub amount_0_out: Option<BigDecimal>,
  pub amount_1_out: Option<BigDecimal>,
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
