use actix::prelude::*;
use bech32::{decode, FromBase32};
use bigdecimal::{BigDecimal};
use diesel::PgConnection;
use diesel::r2d2::{Pool, ConnectionManager};
use hex::{encode};
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
use crate::constants::{Event, Network};

#[derive(Clone)]
pub struct WorkerConfig {
  network: Network,
  contract_hash: String,
  distributor_contract_hashes: Vec<String>,
}

impl WorkerConfig {
  pub fn new(network: Network, contract_hash: &str, distributor_contract_hashes: Vec<&str>) -> Self {
    Self {
      network: network.clone(),
      contract_hash: contract_hash.to_owned(),
      distributor_contract_hashes: distributor_contract_hashes.into_iter().map(|h| h.to_owned()).collect(),
    }
  }
}

pub struct Coordinator{
  config: WorkerConfig,
  db_pool: Pool<ConnectionManager<PgConnection>>,
  arbiter: Option<Addr<EventFetchActor>>,
}

impl Coordinator {
  pub fn new(config: WorkerConfig, db_pool: Pool<ConnectionManager<PgConnection>>) -> Self {
    Coordinator { config, db_pool, arbiter: None }
  }
}

impl Actor for Coordinator {
  type Context = Context<Self>;

  fn started(&mut self, ctx: &mut Self::Context) {
    info!("Coordinator started up.");
    let config = self.config.clone();
    let db_pool = self.db_pool.clone();
    let address = ctx.address();
    let arbiter = SyncArbiter::start(3, move || EventFetchActor::new(config.clone(), db_pool.clone(), address.clone()));
    let contract_hash = self.config.contract_hash.as_str();
    arbiter.do_send(Fetch::new(contract_hash, Event::Minted));
    arbiter.do_send(Fetch::new(contract_hash, Event::Burnt));
    arbiter.do_send(Fetch::new(contract_hash, Event::Swapped));
    for h in &self.config.distributor_contract_hashes {
      arbiter.do_send(Fetch::new(h.as_str(), Event::Claimed));
    }
    self.arbiter = Some(arbiter);
  }

  fn stopped(&mut self, _: &mut Self::Context) {
    warn!("Coordinator died!");
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
      arbiter.do_send(msg.get_next());
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
  msg: Fetch,
  delay: u64,
}

impl NextFetch {
  fn poll(msg: &Fetch) -> Self {
    Self {
      msg: Fetch {
        contract_hash: msg.contract_hash.clone(),
        event: msg.event.clone(),
        page_number: 1,
      },
      delay: 60,
    }
  }

  fn paginate(msg: &Fetch) -> Self {
    Self {
      msg: Fetch {
        contract_hash: msg.contract_hash.clone(),
        event: msg.event.clone(),
        page_number:  msg.page_number + 1,
      },
      delay: 1,
    }
  }

  fn retry(msg: &Fetch) -> Self {
    Self { msg: msg.clone(), delay: 10 }
  }

  fn get_next(&self) -> Fetch {
    self.msg.clone()
  }
}

// Messages for fetch actors
#[derive(Message, Clone)]
#[rtype(result = "()")]
struct Fetch {
  contract_hash: String,
  event: Event,
  page_number: u16,
}

impl Fetch {
  fn new(contract_hash: &str, event: Event) -> Self {
    Self {
      contract_hash: contract_hash.to_owned(),
      event,
      page_number: 1,
    }
  }
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

type PersistResult = Result<(), diesel::result::Error>;

/// Define fetch actor
struct EventFetchActor {
  config: WorkerConfig,
  coordinator: Addr<Coordinator>,
  client: Client,
  db_pool: Pool<ConnectionManager<PgConnection>>
}

impl EventFetchActor {
  fn new(config: WorkerConfig, db_pool: Pool<ConnectionManager<PgConnection>>, coordinator: Addr<Coordinator>) -> Self {
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
      config,
      coordinator,
      client,
      db_pool,
    }
  }

  fn get_and_parse(&mut self, contract_hash: &str, event: Event, page_number: u16) -> Result<responses::ViewBlockResponse, FetchError> {
    info!("Fetching {} for {} page {}", event, contract_hash, page_number);

    let url = Url::parse_with_params(
      format!(
        "https://api.viewblock.io/v1/zilliqa/contracts/{}/events/{}",
        contract_hash,
        event
      )
      .as_str(),
      &[
        ("page", page_number.to_string()),
        ("network", self.config.network.to_string()),
      ],
    ).expect("URL parsing failed!");

    let resp = self.client.get(url).send()?;
    let body = resp.text()?;

    debug!("Parsing {} for {} page {}", event, contract_hash, page_number);
    trace!("{}", body);
    let result: responses::ViewBlockResponse = serde_json::from_str(body.as_str())?;

    return Ok(result)
  }
}

impl Actor for EventFetchActor {
  type Context = SyncContext<Self>;

  fn started(&mut self, _: &mut SyncContext<Self>) {
    info!("Event fetch actor started up.")
  }
}

/// Define handler for `FetchMints` message
impl Handler<Fetch> for EventFetchActor {
  type Result = ();

  fn handle(&mut self, msg: Fetch, _ctx: &mut SyncContext<Self>) -> Self::Result {
    let (contract_hash, event) = (msg.contract_hash.as_str(), msg.event.clone());
    let mut execute = || -> FetchResult {
      let result = self.get_and_parse(contract_hash, event, msg.page_number)?;
      let conn = self.db_pool.get().expect("couldn't get db connection from pool");

      if result.txs.len() == 0 {
        info!("Done with {} events.", event);
        db::insert_backfill_completion(models::NewBackfillCompletion { contract_address: contract_hash, event_name: event.to_string().as_str() }, &conn)?;
        return Ok(NextFetch::poll(&msg));
      }

      for tx in result.txs {
        for (i, ev) in tx.events.iter().enumerate() {
          let persist = match event {
            Event::Minted => persist_mint_event,
            Event::Burnt => persist_burn_event,
            Event::Swapped => persist_swap_event,
            Event::Claimed => persist_claim_event,
          };
          if let Err(err) = persist(&conn, &tx, &ev, &i.try_into().unwrap()) {
            match err {
              diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) => {
                if db::backfill_completed(&conn, contract_hash, event.to_string().as_str())? {
                  info!("Fetched till last inserted {} event.", event);
                  return Ok(NextFetch::poll(&msg));
                }
                debug!("Ignoring duplicate {} entry", event)
              },
              _ => return Err(FetchError::from(err))
            }
          }
        }
      }

      debug!("Going to next page of {}.", event);
      return Ok(NextFetch::paginate(&msg));
    };

    // handle retrying
    let result = execute();
    match result {
      Ok(next_msg) => self.coordinator.do_send(next_msg),
      Err(e) => {
        error!("{:#?}", e);
        error!("Unhandled error while fetching, retrying in 10 seconds..");
        self.coordinator.do_send(NextFetch::retry(&msg));
      }
    }
  }
}

fn persist_mint_event(conn: &PgConnection, tx: &responses::ViewBlockTx, event: &responses::ViewBlockEvent, event_sequence: &i32) -> PersistResult {
  let name = event.name.as_str();
  if name != "Mint" {
    return Ok(())
  }

  let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
  let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
  let amount = event.params.get("amount").unwrap().as_str().expect("Malformed response!");

  let transfer_event = tx.events.iter().find(|&event| event.name.as_str() == "TransferFromSuccess").unwrap();
  let token_amount = transfer_event.params.get("amount").unwrap().as_str().expect("Malformed response!");
  let zil_amount = tx.value.as_str();

  let add_liquidity = models::NewLiquidityChange {
    transaction_hash: &tx.hash,
    event_sequence: &event_sequence,
    block_height: &tx.block_height,
    block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
    initiator_address: address,
    token_address: pool,
    change_amount: &BigDecimal::from_str(amount).unwrap(),
    token_amount: &BigDecimal::from_str(token_amount).unwrap(),
    zil_amount: &BigDecimal::from_str(zil_amount).unwrap(),
  };

  debug!("Inserting: {:?}", add_liquidity);
  db::insert_liquidity_change(add_liquidity, &conn)
}

fn persist_burn_event(conn: &PgConnection, tx: &responses::ViewBlockTx, event: &responses::ViewBlockEvent, event_sequence: &i32) -> PersistResult {
  let name = event.name.as_str();
  if name != "Burnt" {
    return Ok(())
  }
  let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
  let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
  let amount = event.params.get("amount").unwrap().as_str().expect("Malformed response!");

  let transfer_event = tx.events.iter().find(|&event| event.name.as_str() == "TransferSuccess").unwrap();
  let token_amount = transfer_event.params.get("amount").unwrap().as_str().expect("Malformed response!");
  let zil_amount = tx.internal_transfers[0].get("value").unwrap().as_str().expect("Malformed response!");

  let remove_liquidity = models::NewLiquidityChange {
    transaction_hash: &tx.hash,
    event_sequence: &event_sequence,
    block_height: &tx.block_height,
    block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
    initiator_address: address,
    token_address: pool,
    change_amount: &BigDecimal::from_str(amount).unwrap().neg(),
    token_amount: &BigDecimal::from_str(token_amount).unwrap(),
    zil_amount: &BigDecimal::from_str(zil_amount).unwrap(),
  };

  debug!("Inserting: {:?}", remove_liquidity);
  db::insert_liquidity_change(remove_liquidity, &conn)
}

fn persist_swap_event(conn: &PgConnection, tx: &responses::ViewBlockTx, event: &responses::ViewBlockEvent, event_sequence: &i32) -> PersistResult {
  let name = event.name.as_str();
  if name != "Swapped" {
    return Ok(())
  }

  let address = event.params.get("address").unwrap().as_str().expect("Malformed response!");
  let pool = event.params.get("pool").unwrap().as_str().expect("Malformed response!");
  let input_amount = event.params.pointer("/input/0/params/0").unwrap().as_str().expect("Malformed response!");
  let output_amount = event.params.pointer("/output/0/params/0").unwrap().as_str().expect("Malformed response!");
  let input_name = event.params.pointer("/input/1/name").unwrap().as_str().expect("Malformed response!");
  let input_denom = input_name.split(".").last().expect("Malformed response!");

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
    event_sequence: &event_sequence,
    block_height: &tx.block_height,
    block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
    initiator_address: address,
    token_address: pool,
    token_amount: &token_amount,
    zil_amount: &zil_amount,
    is_sending_zil: &is_sending_zil,
  };

  debug!("Inserting: {:?}", new_swap);
  db::insert_swap(new_swap, &conn)
}

fn persist_claim_event(conn: &PgConnection, tx: &responses::ViewBlockTx, event: &responses::ViewBlockEvent, event_sequence: &i32) -> PersistResult {
  let name = event.name.as_str();
  if name != "Claimed" {
    return Ok(())
  }

  let epoch_number = event.params.get("epoch_number").unwrap().as_str().expect("Malformed response!");
  let address = event.params.pointer("/data/0/params/0").unwrap().as_str().expect("Malformed response!");
  let amount = event.params.pointer("/data/0/params/1").unwrap().as_str().expect("Malformed response!");
  let (_hrp, data) = decode(&event.address).expect("Could not decode bech32 address string!");
  let bytes = Vec::<u8>::from_base32(&data).unwrap();
  let distributor_address = format!("0x{}", encode(&bytes));

  let new_claim = models::NewClaim {
    transaction_hash: &tx.hash,
    event_sequence: &event_sequence,
    block_height: &tx.block_height,
    block_timestamp: &chrono::NaiveDateTime::from_timestamp(tx.timestamp / 1000, (tx.timestamp % 1000).try_into().unwrap()),
    initiator_address: address,
    distributor_address: &distributor_address,
    epoch_number: &epoch_number.parse::<i32>().expect("Malformed response"),
    amount: &BigDecimal::from_str(amount).unwrap(),
  };

  debug!("Inserting: {:?}", new_claim);
  db::insert_claim(new_claim, &conn)
}
