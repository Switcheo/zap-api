use actix::prelude::*;
use bigdecimal::{BigDecimal};
use diesel::PgConnection;
use diesel::r2d2::{Pool, ConnectionManager};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use std::{fmt, thread, time};
use std::ops::Neg;
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
    addr.do_send(FetchMints { page_number: 1, next: addr.clone() });
    addr.do_send(FetchBurns { page_number: 1, next: addr.clone() });
    addr.do_send(FetchSwaps { page_number: 1, next: addr.clone() });
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    println!("Worker died!");
  }
}

enum Event {
  Minted,
  Burnt,
  Swapped,
}

impl fmt::Display for Event {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Event::Minted => write!(f, "Mint"),
      Event::Burnt => write!(f, "Burnt"),
      Event::Swapped => write!(f, "Swapped"),
    }
  }
}

enum Network {
  MainNet,
  TestNet,
}

impl Network {
  fn first_txn_hash_for(&self, event: &Event) -> &str {
    match *self {
      Network::MainNet => {
        match event {
          Event::Minted => "0x00a0e4800c709f38d97cd7e769c756a8c059e6aca5eaeace81509c8ab7eccdf4",
          Event::Burnt => "0x9f53e01877d7b40db95d332c3172e1f0d628fd68eabc1e7fcf3316be5619fd50",
          Event::Swapped => "0xee5c4cc44822ee48d2ea8466a4a03c9adb41635caac600e27700fc5e81d8d2dc",
        }
      },
      Network::TestNet => {
        match event {
          Event::Minted => "0x9b8b5695c406d71137f5f420a67cf6b352ae865068530d293116dde072dbfdf6",
          Event::Burnt => "",
          Event::Swapped => "",
        }
      },
    }
  }

  fn contract_hash(&self) -> String {
    String::from(match *self {
      Network::TestNet => "0x1a62dd9c84b0c8948cb51fc664ba143e7a34985c",
      Network::MainNet => "0xBa11eB7bCc0a02e947ACF03Cc651Bfaf19C9EC00",
    })
  }
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

type FetchResult = Result<(), FetchError>;

/// Define messages
#[derive(Message)]
#[rtype(result = "FetchResult")]
struct FetchSwaps {
  page_number: u16,
  next: actix::Addr<EventFetchActor>
}

/// Define messages
#[derive(Message)]
#[rtype(result = "FetchResult")]
struct FetchMints {
  page_number: u16,
  next: actix::Addr<EventFetchActor>
}

/// Define messages
#[derive(Message)]
#[rtype(result = "FetchResult")]
struct FetchBurns {
  page_number: u16,
  next: actix::Addr<EventFetchActor>
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
    let network_str = std::env::var("NETWORK").unwrap_or(String::from("testnet"));
    let network = match network_str.as_str() {
      "testnet" => Network::TestNet,
      "mainnet" => Network::MainNet,
      _ => panic!("Invalid network string")
    };
    let contract_hash = network.contract_hash();
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
      client,
      network,
      contract_hash,
      db_pool,
    }
  }

  fn get_and_parse(&mut self, page_number: u16, event: Event) -> Result<responses::ViewBlockResponse, FetchError> {
    println!("Fetching {} page {}", event, page_number);

    let url = Url::parse_with_params(
      format!(
        "https://api.viewblock.io/v1/zilliqa/contracts/{}/events/{}",
        self.contract_hash,
        event
      )
      .as_str(),
      &[
        ("page", page_number.to_string()),
        ("network", self.network.to_string()),
      ],
    ).expect("URL parsing failed!");

    let resp = self.client.get(url).send()?;
    let body = resp.text()?;

    println!("Parsing {} page {}", event, page_number);
    // println!("{}", body);
    let result: responses::ViewBlockResponse = serde_json::from_str(body.as_str())?;
    return Ok(result)
  }

  fn backfill_complete_for(&self, event: Event) -> bool {
    let hash = String::from(self.network.first_txn_hash_for(&event));
    let conn = self.db_pool.get().expect("couldn't get db connection from pool");
    match event {
      Event::Minted | Event::Burnt => db::liquidity_change_exists(&conn, hash).unwrap(),
      Event::Swapped => db::swap_exists(&conn, hash).unwrap(),
    }
  }
}

impl Actor for EventFetchActor {
  type Context = SyncContext<Self>;

  fn started(&mut self, _: &mut SyncContext<Self>) {
    println!("Event fetch actor started up")
  }
}

/// Define handler for `FetchMints` message
impl Handler<FetchMints> for EventFetchActor {
  type Result = FetchResult;

  fn handle(&mut self, msg: FetchMints, _ctx: &mut SyncContext<Self>) -> Self::Result {
    let result = self.get_and_parse(msg.page_number, Event::Minted)?;

    if result.txs.len() == 0 {
      println!("Done with mints.");
      thread::sleep(time::Duration::new(60, 0));
      msg.next.do_send(FetchMints { page_number: 1, next: msg.next.clone() });
      return Ok(());
    }

    for tx in result.txs {
      for (i, event) in tx.events.iter().enumerate() {
        let name = event.name.as_str();
        if name == "Mint" {
          let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
          let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
          let amount = event.params.get("amount").unwrap().as_str().expect("Malformed response!");

          let add_liquidity = models::NewLiquidityChange {
            transaction_hash: &tx.hash,
            event_sequence: &(i as i32),
            block_height: &tx.block_height,
            block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
            initiator_address: address,
            token_address: pool,
            change_amount: &BigDecimal::from_str(amount).unwrap(),
          };

          println!("Inserting: {:?}", add_liquidity);

          let conn = self.db_pool.get().expect("couldn't get db connection from pool");
          let res = db::insert_liquidity_change(add_liquidity, &conn);
          match res {
            Err(err) => {
              if self.backfill_complete_for(Event::Minted) {
                println!("Fetched till last inserted mint.");
                thread::sleep(time::Duration::new(60, 0));
                msg.next.do_send(FetchMints { page_number: 1, next: msg.next.clone() });
                return Ok(())
              }
              println!("Error inserting: {}", err.to_string())
            },
            Ok(n) => n,
          }
        }
      }
    }

    println!("Next page..");
    thread::sleep(time::Duration::new(1, 0));
    msg.next.do_send(FetchMints { page_number: msg.page_number + 1, next: msg.next.clone() });
    return Ok(())
  }
}

/// Define handler for `FetchBurns` message
impl Handler<FetchBurns> for EventFetchActor {
  type Result = FetchResult;

  fn handle(&mut self, msg: FetchBurns, _ctx: &mut SyncContext<Self>) -> Self::Result {
    let result = self.get_and_parse(msg.page_number, Event::Burnt)?;

    if result.txs.len() == 0 {
      println!("Done with burns.");
      thread::sleep(time::Duration::new(60, 0));
      msg.next.do_send(FetchBurns { page_number: 1, next: msg.next.clone() });
      return Ok(());
    }

    for tx in result.txs {
      for (i, event) in tx.events.iter().enumerate() {
        let name = event.name.as_str();
        if name == "Burnt" {
          let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
          let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
          let amount = event.params.get("amount").unwrap().as_str().expect("Malformed response!");

          let remove_liquidity = models::NewLiquidityChange {
            transaction_hash: &tx.hash,
            event_sequence: &(i as i32),
            block_height: &tx.block_height,
            block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
            initiator_address: address,
            token_address: pool,
            change_amount: &BigDecimal::from_str(amount).unwrap().neg(),
          };

          println!("Inserting: {:?}", remove_liquidity);

          let conn = self.db_pool.get().expect("couldn't get db connection from pool");
          let res = db::insert_liquidity_change(remove_liquidity, &conn);
          match res {
            Err(err) => {
              if self.backfill_complete_for(Event::Burnt) {
                println!("Fetched till last inserted burn.");
                thread::sleep(time::Duration::new(60, 0));
                msg.next.do_send(FetchBurns { page_number: 1, next: msg.next.clone() });
                return Ok(())
              }
              println!("Error inserting: {}", err.to_string())
            },
            Ok(n) => n,
          }
        }
      }
    }

    println!("Next page..");
    thread::sleep(time::Duration::new(1, 0));
    msg.next.do_send(FetchBurns { page_number: msg.page_number + 1, next: msg.next.clone() });
    return Ok(())
  }
}

/// Define handler for `FetchSwaps` message
impl Handler<FetchSwaps> for EventFetchActor {
  type Result = FetchResult;

  fn handle(&mut self, msg: FetchSwaps, _ctx: &mut SyncContext<Self>) -> Self::Result {
    let result = self.get_and_parse(msg.page_number, Event::Swapped)?;

    if result.txs.len() == 0 {
      println!("Done with swaps.");
      thread::sleep(time::Duration::new(60, 0));
      msg.next.do_send(FetchSwaps { page_number: 1, next: msg.next.clone() });
      return Ok(());
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
            Err(err) => {
              if self.backfill_complete_for(Event::Swapped) {
                println!("Fetched till last inserted swap.");
                thread::sleep(time::Duration::new(60, 0));
                msg.next.do_send(FetchSwaps { page_number: 1, next: msg.next.clone() });
                return Ok(())
              }
              println!("Error inserting: {}", err.to_string())
            },
            Ok(n) => n,
          }
        }
      }
    }

    println!("Next page..");
    thread::sleep(time::Duration::new(1, 0));
    msg.next.do_send(FetchSwaps { page_number: msg.page_number + 1, next: msg.next.clone() });
    return Ok(())
  }
}
