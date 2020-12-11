use actix::prelude::*;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Value};
use std::{fmt, thread, time};

use crate::models;

pub struct Worker;

impl Actor for Worker {
  type Context = Context<Self>;

  fn started(&mut self, _: &mut Self::Context) {
    println!("Worker is alive!");
    let addr = SyncArbiter::start(3, move || EventFetchActor::default());
    addr.do_send(FetchSwaps { page_number: 1 });
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    println!("Worker died!");
  }
}

enum Network {
  MainNet,
  TestNet,
}

impl fmt::Display for Network {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Network::MainNet => write!(f, "mainnet"),
      Network::TestNet => write!(f, "testnet"),
    }
  }
}

/// Define messages
#[derive(Message)]
#[rtype(result = "Result<bool, std::io::Error>")]
struct FetchSwaps {
  page_number: u16,
}

/// Define messages
#[derive(Message)]
#[rtype(result = "Result<bool, std::io::Error>")]
struct FetchMints {
  page_number: u16,
}

/// Define messages
#[derive(Message)]
#[rtype(result = "Result<bool, std::io::Error>")]
struct FetchBurns {
  page_number: u16,
}

/// Define actor
struct EventFetchActor {
  client: Client,
  network: Network,
  contract_hash: String,
}

impl Default for EventFetchActor {
  fn default() -> Self {
    let api_key = std::env::var("VIEWBLOCK_API_KEY").expect("VIEWBLOCK_API_KEY env var missing.");
    let mut headers = HeaderMap::new();
    headers.insert(
      "X-APIKEY",
      HeaderValue::from_str(api_key.as_str()).expect("Invalid API key."),
    );

    let client = Client::builder()
      .default_headers(headers)
      .build()
      .expect("Failed to build client.");

    Self {
      client: client,
      network: Network::MainNet,
      contract_hash: "0xBa11eB7bCc0a02e947ACF03Cc651Bfaf19C9EC00".to_string(),
    }
  }
}

impl Actor for EventFetchActor {
  type Context = SyncContext<Self>;

  fn started(&mut self, _: &mut SyncContext<Self>) {
    println!("Event fetch actor started up")
  }
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
  hash: String,
  event: String,
  txs: Vec<Tx>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Tx {
  hash: String,
  blockHeight: String,
  from: String,
  to: String,
  value: String,
  fee: String,
  timestamp: u64,
  signature: String,
  direction: String,
  nonce: u32,
  receiptSuccess: bool,
  data: String,
  internalTransfers: Vec<Value>,
  events: Vec<Event>,
  transitions: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Event {
  address: String,
  name: String,
  details: String,
  params: Value,
}

/// Define handler for `Ping` message
impl Handler<FetchSwaps> for EventFetchActor {
  type Result = Result<bool, std::io::Error>;

  fn handle(&mut self, msg: FetchSwaps, _: &mut SyncContext<Self>) -> Self::Result {
    println!("SENDING");
    let url = Url::parse_with_params(
      format!(
        "https://api.viewblock.io/v1/zilliqa/contracts/{}/events/Swapped",
        self.contract_hash
      )
      .as_str(),
      &[
        ("page", msg.page_number.to_string()),
        ("network", self.network.to_string()),
      ],
    )
    .expect("Failed to parse url.");

    let resp = self.client.get(url).send().expect("Failed to send.");
    let body = resp.text().expect("Failed to parse response");
    println!("BODY\n{:?}", body);
    let result = serde_json::from_str(body.as_str());

    match result {
      Ok => {
        println!("RESULT\n{:?}", result.hash);

        thread::sleep(time::Duration::new(1, 0));

        println!("Done.");

      }
    }
    Ok(true)
  }
}
