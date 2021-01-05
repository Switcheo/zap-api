use actix::prelude::*;
use bigdecimal::{BigDecimal};
use diesel::PgConnection;
use diesel::r2d2::{Pool, ConnectionManager};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use std::{fmt, thread, time};
use std::str::FromStr;
use std::convert::TryInto;

use crate::db;
use crate::models;
use crate::responses;

pub struct Worker{
  db_pool: Pool<ConnectionManager<PgConnection>>,
}

impl Worker {
  pub fn new(db_pool: Pool<ConnectionManager<PgConnection>>) -> Self {
    Worker { db_pool: db_pool }
  }
}

impl Actor for Worker {
  type Context = Context<Self>;

  fn started(&mut self, _: &mut Self::Context) {
    println!("Worker is alive!");
    let pool = self.db_pool.clone();
    let addr = SyncArbiter::start(3, move || EventFetchActor::new(pool.clone()));
    addr.do_send(FetchSwaps { page_number: 1, next: addr.clone() });
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


#[derive(Debug)]
enum FetchError {
    // We will defer to the parse error implementation for their error.
    // Supplying extra info requires adding more data to the type.
    Fetch(reqwest::Error),
    Parse(serde_json::Error),
}

impl From<reqwest::Error> for FetchError {
  fn from(err: reqwest::Error) -> FetchError {
    FetchError::Fetch(err)
  }
}

impl From<serde_json::Error> for FetchError {
  fn from(err: serde_json::Error) -> FetchError {
    FetchError::Parse(err)
  }
}

/// Define messages
#[derive(Message)]
#[rtype(result = "Result<bool, FetchError>")]
struct FetchSwaps {
  page_number: u16,
  next: actix::Addr<EventFetchActor>
}

/// Define messages
#[derive(Message)]
#[rtype(result = "Result<bool, FetchError>")]
struct FetchMints {
  page_number: u16,
}

/// Define messages
#[derive(Message)]
#[rtype(result = "Result<bool, FetchError>")]
struct FetchBurns {
  page_number: u16,
}

/// Define actor
struct EventFetchActor {
  client: Client,
  network: Network,
  contract_hash: String,
  db_pool: Pool<ConnectionManager<PgConnection>>
}

impl EventFetchActor {
  fn new(db_pool: Pool<ConnectionManager<PgConnection>>) -> Self {
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
      db_pool: db_pool
    }
  }
}

impl Actor for EventFetchActor {
  type Context = SyncContext<Self>;

  fn started(&mut self, _: &mut SyncContext<Self>) {
    println!("Event fetch actor started up")
  }
}

/// Define handler for `Ping` message
impl Handler<FetchSwaps> for EventFetchActor {
  type Result = Result<bool, FetchError>;

  fn handle(&mut self, msg: FetchSwaps, _ctx: &mut SyncContext<Self>) -> Self::Result {
    println!("Fetching swaps page {}", msg.page_number);
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
    ).expect("URL parsing failed!");

    let resp = self.client.get(url).send()?;
    let body = resp.text()?;
    println!("Parsing page {}", msg.page_number);
    let result: responses::ViewBlockResponse = serde_json::from_str(body.as_str())?;

    if result.txs.len() == 0 {
      println!("Done.");
      thread::sleep(time::Duration::new(60, 0));
      msg.next.do_send(FetchSwaps { page_number: 0, next: msg.next.clone() });
      return Ok(false);
    }

    for tx in result.txs {
      for (i, event) in tx.events.iter().enumerate() {
        let name = event.name.as_str();
        if name == "Swapped" {
          let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
          let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
          let input_amount = event.params.pointer("/input/0/params/0").unwrap().as_str().expect("Malformed response!");
          let output_amount = event.params.pointer("/output/0/params/0").unwrap().as_str().expect("Malformed response!");
          let input_denom = event.params.pointer("/input/1/name").unwrap().as_str().expect("Malformed response!");

          let token_amount;
          let zil_amount;
          let is_sending_zil;
          match input_denom {
            "Token" => {
              token_amount = BigDecimal::from_str(input_amount).unwrap();
              zil_amount = BigDecimal::from_str(output_amount).unwrap();
              is_sending_zil = false;
            },
            "Zil" => {
              zil_amount = BigDecimal::from_str(input_amount).unwrap();
              token_amount = BigDecimal::from_str(output_amount).unwrap();
              is_sending_zil = true;
            }
            _ => {
              panic!("Malformed input denom!");
            }
          }

          let new_swap = models::NewSwap {
            transaction_hash: &tx.hash,
            event_sequence: &(i as i32),
            block_height: &tx.block_height,
            block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
            initiator_address: address,
            token_address: pool,
            token_amount: &token_amount,
            zil_amount: &zil_amount,
            is_sending_zil: &is_sending_zil,
          };

          println!("Inserting: {:?}", new_swap);

          let conn = self.db_pool.get().expect("couldn't get db connection from pool");
          let res = db::insert_swap(new_swap, &conn);
          match res {
            Err(err) => println!("Error inserting: {}", err.to_string()),
            Ok(n) => n,
          }
        }
      }
    }

    println!("Next page..");
    thread::sleep(time::Duration::new(1, 0));
    msg.next.do_send(FetchSwaps { page_number: msg.page_number + 1, next: msg.next.clone() });
    return Ok(true)
  }
}
