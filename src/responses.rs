use bigdecimal::{BigDecimal};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewBlockResponse {
  pub hash: String,
  pub event: String,
  pub txs: Vec<ViewBlockTx>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewBlockTx {
  pub hash: String,
  pub block_height: i32,
  pub from: String,
  pub to: String,
  pub value: String,
  pub fee: String,
  pub timestamp: i64,
  pub signature: String,
  pub direction: String,
  pub nonce: u32,
  pub receipt_success: bool,
  pub data: String,
  pub internal_transfers: Vec<Value>,
  pub events: Vec<ViewBlockEvent>,
  pub transitions: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewBlockEvent {
  pub address: String,
  pub name: String,
  pub details: String,
  pub params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ZilStreamToken {
  pub name: String,
  pub symbol: String,
  pub address_bech32: String,
  pub icon: String,
  pub website: String,
  pub decimals: u32,
  pub init_supply: BigDecimal,
  pub max_supply: BigDecimal,
  pub total_supply: BigDecimal,
  pub current_supply: BigDecimal,
}
