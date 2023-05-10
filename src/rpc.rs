use reqwest::blocking::Client;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strum_macros::Display;

use crate::utils;

#[derive(Display, Clone)]
pub enum RPCMethod {
  GetTransaction,
  GetTransactionsForTxBlock,
  GetNumTxBlocks,
  GetTxBlock,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RPCRequest {
  id: i32,
  jsonrpc: String,
  method: String,
  params: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MaybeTxEvent {
  pub _eventname: Option<String>,
  pub address: String,
  pub params: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxEvent {
  pub _eventname: String,
  pub address: String,
  pub params: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxMsg {
  pub _amount: String,
  pub _recipient: String,
  pub _tag: String,
  pub params: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxTransition {
  pub accepted: Option<bool>,
  pub addr: String,
  pub depth: i32,
  pub msg: TxMsg,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxReceipt {
  pub success: bool,
  pub accepted: Option<bool>,
  pub event_logs: Option<Vec<MaybeTxEvent>>,
  pub transitions: Option<Vec<TxTransition>>,
}

impl TxReceipt {
  pub fn events(&self) -> Vec<TxEvent> {
    let init: Vec<TxEvent> = vec![];
    self.event_logs.clone().unwrap_or(vec![])
      .into_iter()
      .fold(init, |events, result| match result._eventname {
        Some(eventname) => {
          let event = TxEvent {
            _eventname: eventname,
            address: result.address.clone(),
            params: result.params.unwrap().clone(),
          };
          return [&events[..], &[event]].concat();
        },
        None => events,
      })
  }
  pub fn transitions(&self) -> Vec<TxTransition> {
    self.transitions.clone().unwrap_or(vec![])
  }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxResult {
  #[serde(rename = "ID")]
  pub id: String,
  pub amount: String,
  pub data: Option<String>,
  pub nonce: String,
  pub receipt: TxReceipt,
  #[serde(rename = "senderPubKey")]
  pub sender_pub_key: String,

  #[serde(rename = "gasLimit")]
  pub gas_limit: String,

  #[serde(rename = "gasPrice")]
  pub gas_price: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct BlockHeader {
  pub block_num: String,
  pub num_txns: i32,
  pub timestamp: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct BlockBody {
  pub block_hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockResult {
  pub header: BlockHeader,
  pub body: BlockBody,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockTxsResult(Vec<Option<Vec<String>>>);
impl BlockTxsResult {
  pub fn list(&self) -> Vec<String> {
    let BlockTxsResult(nested_txs) = self;
    nested_txs.clone()
      .into_iter()
      .filter(|o| o.is_some())
      .map(|v| v.unwrap())
      .flatten()
      .collect()
  }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RPCResponse {
  pub id: i32,
  pub jsonrpc: String,
  pub result: Value,
}

#[derive(Clone)]
pub struct ZilliqaClient {
  rpc_url: String,
  http_client: Client,
}

impl ZilliqaClient {
  pub fn new(rpc_url: &str) -> ZilliqaClient {
    Self {
      rpc_url: rpc_url.to_string(),
      http_client: Client::new(),
    }
  }
  pub fn rpc_call(&self, rpc_method: RPCMethod, params: Vec<String>) -> Result<Value, utils::FetchError>  {
    let method = rpc_method.to_string();
    trace!("call {} {}", method, self.rpc_url);
    let url = Url::parse(self.rpc_url.as_str()).expect("URL parsing failed!");

    let request = RPCRequest { 
      id: 1, 
      jsonrpc: "2.0".to_string(),
      method,
      params,
    };
    let payload = serde_json::to_string(&request).unwrap();
    trace!("payload {}", payload);

    let resp = self.http_client.post(url).body(payload).send()?;
    let body = resp.text()?;
    trace!("response {}", body);

    let rpc_response: RPCResponse = serde_json::from_str(body.as_str())?;
    return Ok(rpc_response.result);
  }

  pub fn get_transaction(&self, tx_hash: &String) -> Result<TxResult, utils::FetchError> {
    let result = self.rpc_call(RPCMethod::GetTransaction, vec![tx_hash.clone()])?;
    let tx_result = serde_json::from_value(result).unwrap();
    return Ok(tx_result);
  }

  pub fn get_block(&self, block_height: &u32) -> Result<BlockResult, utils::FetchError> {
    let result = self.rpc_call(RPCMethod::GetTxBlock, vec![block_height.to_string()])?;
    let blk_result = serde_json::from_value(result).unwrap();
    return Ok(blk_result);
  }

  pub fn get_latest_block(&self) -> Result<u32, utils::FetchError> {
    let result = self.rpc_call(RPCMethod::GetNumTxBlocks, vec![])?;
    let blk_result_string: String = serde_json::from_value(result).unwrap();
    let blk_result = blk_result_string.parse::<u32>().unwrap();

    return Ok(blk_result);
  }

  pub fn get_block_txs(&self, block_height: &u32) -> Result<BlockTxsResult, utils::FetchError> {
    let result = self.rpc_call(RPCMethod::GetTransactionsForTxBlock, vec![block_height.to_string()])?;
    let txs_result = serde_json::from_value(result).unwrap();
    return Ok(txs_result);
  }
}
