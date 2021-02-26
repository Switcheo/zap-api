use actix::prelude::*;
use bigdecimal::{BigDecimal};
use diesel::PgConnection;
use diesel::r2d2::{Pool, ConnectionManager};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;
use std::time::{Duration};
use std::convert::TryInto;
use std::ops::Neg;
use std::str::FromStr;

use crate::db;
use crate::models;
use crate::responses;
use crate::utils;
use crate::constants::{Event, Network};

pub struct Coordinator{
  db_pool: Pool<ConnectionManager<PgConnection>>,
  arbiter: Option<Addr<EventFetchActor>>,
}

impl Coordinator {
  pub fn new(db_pool: Pool<ConnectionManager<PgConnection>>) -> Self {
    Coordinator { db_pool: db_pool, arbiter: None }
  }
}

impl Actor for Coordinator {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    println!("Coordinator is alive!");
    let pool = self.db_pool.clone();
    let coordinator = ctx.address();
    let arbiter = SyncArbiter::start(3, move || EventFetchActor::new(pool.clone(), coordinator.clone()));
    arbiter.do_send(FetchMints { page_number: 1 });
    arbiter.do_send(FetchBurns { page_number: 1 });
    arbiter.do_send(FetchSwaps { page_number: 1 });
    self.arbiter = Some(arbiter);
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    println!("Coordinator died!");
  }
}

/// Define handler for `NextFetch` message which
/// is sent from FetchActors to continue fetching
/// next pages.
impl Handler<NextFetch> for Coordinator {
  type Result = ();

  fn handle(&mut self, msg: NextFetch, ctx: &mut Context<Self>) -> Self::Result {
    ctx.run_later(Duration::from_secs(msg.delay), move |worker, _| {
      let arbiter = worker.arbiter.as_ref().unwrap();
      let page_number = msg.page_number;
      match msg.event{
        Event::Minted => arbiter.do_send(FetchMints { page_number }),
        Event::Burnt => arbiter.do_send(FetchBurns { page_number }),
        Event::Swapped => arbiter.do_send(FetchSwaps { page_number }),
      };
    });
  }
}

/// Define messages
/// All messages return unit result, as error handling
/// is done within the handler itself.

// Messages for coordinator
#[derive(Message)]
#[rtype(result = "()")]
struct NextFetch {
  event: Event,
  page_number: u16,
  delay: u64,
}

// Messages for fetch actors
#[derive(Message)]
#[rtype(result = "()")]
struct FetchSwaps {
  page_number: u16,
}

#[derive(Message)]
#[rtype(result = "()")]
struct FetchMints {
  page_number: u16,
}

#[derive(Message)]
#[rtype(result = "()")]
struct FetchBurns {
  page_number: u16,
}

/// The actual fetch result
type FetchResult = Result<NextFetch, FetchError>;

#[derive(Debug)]
enum FetchError {
    // We will defer to the parse error implementation for their error.
    // Supplying extra info requires adding more data to the type.
    Fetch(reqwest::Error),
    Parse(serde_json::Error),
    Database(diesel::result::Error),
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

impl From<diesel::result::Error> for FetchError {
  fn from(err: diesel::result::Error) -> FetchError {
    FetchError::Database(err)
  }
}

/// Define fetch actor
struct EventFetchActor {
  coordinator: Addr<Coordinator>,
  client: Client,
  network: Network,
  contract_hash: String,
  db_pool: Pool<ConnectionManager<PgConnection>>
}

impl EventFetchActor {
  fn new(db_pool: Pool<ConnectionManager<PgConnection>>, coordinator: Addr<Coordinator>) -> Self {
    let api_key = std::env::var("VIEWBLOCK_API_KEY").expect("VIEWBLOCK_API_KEY env var missing.");
    let network = utils::get_network();
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
      coordinator,
      client,
      network,
      contract_hash,
      db_pool,
    }
  }

  fn get_and_parse(&mut self, page_number: u16, event: Event) -> Result<responses::ViewBlockResponse, FetchError> {
    let network = utils::get_network();
    let event_name = network.event_name(&event);

    println!("Fetching {} page {}", event_name, page_number);

    let url = Url::parse_with_params(
      format!(
        "https://api.viewblock.io/v1/zilliqa/contracts/{}/events/{}",
        self.contract_hash,
        event_name
      )
      .as_str(),
      &[
        ("page", page_number.to_string()),
        ("network", self.network.to_string()),
      ],
    ).expect("URL parsing failed!");

    let resp = self.client.get(url).send()?;
    let body = resp.text()?;

    println!("Parsing {} page {}", event_name, page_number);
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
  type Result = ();

  fn handle(&mut self, msg: FetchMints, _ctx: &mut SyncContext<Self>) -> Self::Result {
    let mut execute = || -> FetchResult {
      return Ok(NextFetch{event: Event::Minted, page_number: 1, delay: 60000 });
      let result = self.get_and_parse(msg.page_number, Event::Minted)?;

      if result.txs.len() == 0 {
        println!("Done with mints.");
        return Ok(NextFetch{event: Event::Minted, page_number: 1, delay: 60 });
      }

      let network = utils::get_network();
      let event_name = network.event_name(&Event::Minted);

      for tx in result.txs {
        for (i, event) in tx.events.iter().enumerate() {
          let name = event.name.as_str();
          if name == event_name {
            let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
            let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
            let amount = event.params.get("amount").unwrap().as_str().expect("Malformed response!");

            let transfer_event = tx.events.iter().find(|&event| event.name.as_str() == "TransferFromSuccess").unwrap();
            let token_amount = transfer_event.params.get("amount").unwrap().as_str().expect("Malformed response!");
            let zil_amount = tx.value.as_str();

            let add_liquidity = models::NewLiquidityChange {
              transaction_hash: &tx.hash,
              event_sequence: &(i as i32),
              block_height: &tx.block_height,
              block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
              initiator_address: address,
              token_address: pool,
              change_amount: &BigDecimal::from_str(amount).unwrap(),
              token_amount: &BigDecimal::from_str(token_amount).unwrap(),
              zil_amount: &BigDecimal::from_str(zil_amount).unwrap(),
            };

            println!("Inserting: {:?}", add_liquidity);

            let conn = self.db_pool.get().expect("couldn't get db connection from pool");
            let res = db::insert_liquidity_change(add_liquidity, &conn);
            if let Err(e) = res {
              if self.backfill_complete_for(Event::Minted) {
                println!("Fetched till last inserted mint.");
                return Ok(NextFetch{event: Event::Minted, page_number: 1, delay: 60 });
              }
              match e {
                diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) => println!("Ignoring duplicate entry!"),
                _ => return Err(FetchError::from(e))
              }
            }
          }
        }
      }

      println!("Next page..");
      return Ok(NextFetch{event: Event::Minted, page_number: msg.page_number + 1, delay: 1 });
    };

    // handle retrying
    let result = execute();
    match result {
      Ok(msg) => self.coordinator.do_send(msg),
      Err(e) => {
        println!("{:#?}", e);
        println!("Unhandled error while fetching, retrying in 10 seconds..");
        self.coordinator.do_send(NextFetch{event: Event::Minted, page_number: msg.page_number, delay: 10 });
      }
    }
  }
}

/// Define handler for `FetchBurns` message
impl Handler<FetchBurns> for EventFetchActor {
  type Result = ();

  fn handle(&mut self, msg: FetchBurns, _ctx: &mut SyncContext<Self>) -> Self::Result {
    let mut execute = || -> FetchResult {
      let result = self.get_and_parse(msg.page_number, Event::Burnt)?;

      if result.txs.len() == 0 {
        println!("Done with burns.");
        return Ok(NextFetch{event: Event::Burnt, page_number: 1, delay: 60 });
      }

      let network = utils::get_network();
      let event_name = network.event_name(&Event::Burnt);

      for tx in result.txs {
        for (i, event) in tx.events.iter().enumerate() {
          let name = event.name.as_str();
          if name == event_name {

            let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
            let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
            let amount = event.params.get("amount").unwrap().as_str().expect("Malformed response!");

            let transfer_event = tx.events.iter().find(|&event| event.name.as_str() == "TransferSuccess").unwrap();
            let token_amount = transfer_event.params.get("amount").unwrap().as_str().expect("Malformed response!");
            let zil_amount = tx.internal_transfers[0].get("value").unwrap().as_str().expect("Malformed response!");

            let remove_liquidity = models::NewLiquidityChange {
              transaction_hash: &tx.hash,
              event_sequence: &(i as i32),
              block_height: &tx.block_height,
              block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
              initiator_address: address,
              token_address: pool,
              change_amount: &BigDecimal::from_str(amount).unwrap().neg(),
              token_amount: &BigDecimal::from_str(token_amount).unwrap(),
              zil_amount: &BigDecimal::from_str(zil_amount).unwrap(),
            };

            println!("Inserting: {:?}", remove_liquidity);

            let conn = self.db_pool.get().expect("couldn't get db connection from pool");
            let res = db::insert_liquidity_change(remove_liquidity, &conn);
            if let Err(e) = res {
              if self.backfill_complete_for(Event::Burnt) {
                println!("Fetched till last inserted burn.");
                return Ok(NextFetch{event: Event::Burnt, page_number: 1, delay: 60 });
              }
              match e {
                diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) => println!("Ignoring duplicate entry!"),
                _ => return Err(FetchError::from(e))
              }
            }
          }
        }
      }

      println!("Next page..");
      return Ok(NextFetch{event: Event::Burnt, page_number: msg.page_number + 1, delay: 1 });
    };

    // handle retrying
    let result = execute();
    match result {
      Ok(m) => self.coordinator.do_send(m),
      Err(e) => {
        println!("{:#?}", e);
        println!("Unhandled error while fetching, retrying in 10 seconds..");
        self.coordinator.do_send(NextFetch{event: Event::Burnt, page_number: msg.page_number, delay: 10 });
      }
    }
  }
}

/// Define handler for `FetchSwaps` message
impl Handler<FetchSwaps> for EventFetchActor {
  type Result = ();

  fn handle(&mut self, msg: FetchSwaps, _ctx: &mut SyncContext<Self>) -> Self::Result {
    let mut execute = || -> FetchResult {
      let result = self.get_and_parse(msg.page_number, Event::Swapped)?;

      return Ok(NextFetch{event: Event::Swapped, page_number: 1, delay: 60000 });

      if result.txs.len() == 0 {
        println!("Done with swaps.");
        return Ok(NextFetch{event: Event::Swapped, page_number: 1, delay: 60 });
      }

      let network = utils::get_network();
      let event_name = network.event_name(&Event::Swapped);

      for tx in result.txs {
        for (i, event) in tx.events.iter().enumerate() {
          let name = event.name.as_str();
          if name == event_name {
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
            if let Err(e) = res {
              if self.backfill_complete_for(Event::Swapped) {
                println!("Fetched till last inserted swap.");
                return Ok(NextFetch{event: Event::Swapped, page_number: 1, delay: 60 });
              }
              match e {
                diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) => println!("Ignoring duplicate entry!"),
                _ => return Err(FetchError::from(e))
              }
            }
          }
        }
      }

      println!("Next page..");
      return Ok(NextFetch{event: Event::Swapped, page_number: msg.page_number + 1, delay: 1 });
    };

    // handle retrying
    let result = execute();
    match result {
      Ok(msg) => self.coordinator.do_send(msg),
      Err(e) => {
        println!("{:#?}", e);
        println!("Unhandled error while fetching, retrying in 10 seconds..");
        self.coordinator.do_send(NextFetch{event: Event::Swapped, page_number: msg.page_number, delay: 10 });
      }
    }
  }
}
