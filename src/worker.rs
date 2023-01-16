use actix::prelude::*;
use bech32::{encode, ToBase32};
use bigdecimal::{BigDecimal};
use chrono::{NaiveDateTime};
use diesel::PgConnection;
use diesel::r2d2::{Pool, ConnectionManager};
use hex;
use ring::{digest};
use serde_json::Value;
use std::time::{Duration};
use std::convert::TryInto;
use std::ops::Neg;
use std::cmp::{max, min};
use std::str::FromStr;

use crate::db;
use crate::models;
use crate::utils;
use crate::rpc::{ZilliqaClient, TxResult};
use crate::constants::{Event, Network};

#[derive(Clone)]
pub struct WorkerConfig {
  network: Network,
  contract_hash: String,
  pool_contract_hashes: Vec<String>,
  distributor_contract_hashes: Vec<String>,
  min_sync_height: u32,
  rpc_url: String,
}

impl WorkerConfig {
  pub fn new(
    network: Network,
    contract_hash: &str,
    pool_contract_hashes: Vec<&str>,
    distributor_contract_hashes: Vec<&str>,
    min_sync_height: u32,
    rpc_url: String,
  ) -> Self {
    Self {
      network: network.clone(),
      contract_hash: contract_hash.to_owned(),
      pool_contract_hashes: pool_contract_hashes.into_iter().map(|h| h.to_owned()).collect(),
      distributor_contract_hashes: distributor_contract_hashes.into_iter().map(|h| h.to_owned()).collect(),
      min_sync_height,
      rpc_url,
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
    info!("Coordinator starting sync with {}.", config.rpc_url);

    let arbiter = SyncArbiter::start(5, move || EventFetchActor::new(config.clone(), db_pool.clone(), address.clone()));
    let sync_start_block = std::env::var("FORCE_SYNC_HEIGHT").unwrap_or("0".to_string()).parse::<u32>().expect("invalid env value for FORCE_SYNC_HEIGHT");
    arbiter.do_send(Fetch::query_new_blocks(sync_start_block));
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

  fn handle(&mut self, next_msg: NextFetch, ctx: &mut Context<Self>) -> Self::Result {
    let maybe_msg = next_msg.get_next();
    match maybe_msg {
      Some(msg) => {
        ctx.run_later(Duration::from_secs(next_msg.delay), move |worker, _| {
          let arbiter = worker.arbiter.as_ref().unwrap();
          arbiter.do_send(msg);
        });
      },
      None => (),
    }
  }
}

#[derive(Debug, Clone)]
struct ChainEvent {
  block_height: i32,
  block_timestamp: NaiveDateTime,
  tx_hash: String,
  event_index: i32,
  contract_address: String,
  initiator_address: String,
  name: String,
  params: Value,
}

#[derive(Clone)]
struct QueryNewBlocksParams {
  prev_height: u32,
}

#[derive(Clone)]
struct ProcessBlockParams {
  height: u32,
}

#[derive(Clone)]
enum FetchJob {
  QueryNewBlocksParams(QueryNewBlocksParams),
  ProcessBlockParams(ProcessBlockParams),
}

/// Define messages
/// All messages return unit result, as error handling
/// is done within the handler itself.

// Messages for coordinator
#[derive(Message)]
#[rtype(result = "()")]
struct NextFetch {
  msg: Option<Fetch>,
  delay: u64,
}

impl NextFetch {
  fn from(msg: Fetch, delay: Option<u64>) -> Self {
    Self {
      msg: Some(msg),
      delay: delay.unwrap_or(1),
    }
  }

  fn empty() -> Self {
    Self { msg: None, delay: 1 }
  }

  fn retry(msg: &Fetch) -> Self {
    Self { msg: Some(msg.clone()), delay: 5 }
  }

  fn get_next(&self) -> Option<Fetch> {
    self.msg.clone()
  }
}

// Messages for fetch actors
#[derive(Message, Clone)]
#[rtype(result = "()")]
struct Fetch {
  job: FetchJob,
}

impl Fetch {
  fn query_new_blocks(prev_height: u32) -> Fetch {
    let job = FetchJob::QueryNewBlocksParams(QueryNewBlocksParams{ prev_height });
    Self { job }
  }
  fn process_block(height: u32) -> Fetch {
    let job = FetchJob::ProcessBlockParams(ProcessBlockParams{ height });
    Self { job }
  }
}

/// The actual fetch result
type FetchResult = Result<NextFetch, utils::FetchError>;

type PersistResult = Result<bool, diesel::result::Error>;

/// Define fetch actor
struct EventFetchActor {
  config: WorkerConfig,
  coordinator: Addr<Coordinator>,
  zil_client: ZilliqaClient,
  db_pool: Pool<ConnectionManager<PgConnection>>
}

impl EventFetchActor {
  fn new(config: WorkerConfig, db_pool: Pool<ConnectionManager<PgConnection>>, coordinator: Addr<Coordinator>) -> Self {
    let zil_client = ZilliqaClient::new(&config.rpc_url);
    Self {
      zil_client,
      config,
      coordinator,
      db_pool,
    }
  }

  /// query the chain for new blocks from provided previous height
  //  if in_prev_height = 0, previous height will be inferred from database block_syncs table
  //  or zilswap_min_sync_at on config file.
  //  queues up to 100 new blocks with `ProcessBlock` job for syncing.
  fn query_new_blocks(&self, in_prev_height: u32) -> FetchResult {
    trace!("QueryNewBlocks: handle");
    let conn = self.db_pool.get().expect("couldn't get db connection from pool");

    let new_prev_height = conn.build_transaction()
      .read_write()
      .run::<_, utils::FetchError, _>(|| {
        let prev_height = match in_prev_height == 0 {
          true => {
            info!("QueryNewBlocks: min_sync_height {}", self.config.min_sync_height);
            let last_sync_height: u32 = db::last_sync_height(&conn)?.try_into().expect("invalid last sync height");

            info!("QueryNewBlocks: last_sync_height {}", last_sync_height);
            let min_height = self.config.min_sync_height;
            max(last_sync_height, min_height)
          },
          false => in_prev_height,
        };
        let chain_height = self.zil_client.get_latest_block()?;
        if prev_height >= chain_height {
          return Ok(prev_height)
        }

        info!("QueryNewBlocks: sync {}/{}", prev_height + 1, chain_height);

        let query_count: u32 = min(100, chain_height - prev_height).try_into().expect("invalid chain height");
        let last_height = prev_height + query_count - 1;
        trace!("QueryNewBlocks: from {} - {}", prev_height, last_height);

        let new_prev_height = last_height;
        let start_height = prev_height + 1;

        for height in start_height..=last_height {
          let msg = Fetch::process_block(height);
          let next_msg = NextFetch::from(msg, None);
          self.coordinator.do_send(next_msg)
        }
        Ok(new_prev_height)
      })?;

    let msg = Fetch::query_new_blocks(new_prev_height);
    Ok(NextFetch::from(msg, Some(20)))
  }

  /// query one single block from chain based on given height.
  //  list all transactions on block and process all one by one.
  fn process_block(&self, height: u32) -> FetchResult {
    trace!("ProcessBlock: handle {}", height);
    let conn = self.db_pool.get().expect("couldn't get db connection from pool");

    conn.build_transaction()
      .read_write()
      .run::<_, utils::FetchError, _>(|| {
        let block = self.zil_client.get_block(&height)?;

        if block.body.block_hash == "0000000000000000000000000000000000000000000000000000000000000000" {
          trace!("ProcessBlock: block not available on node {}", height);
          return Ok(())
        }

        let block_height = block.header.block_num.parse::<u32>().expect("invalid block height");
        let timestamp = block.header.timestamp.parse::<i64>().expect("invalid block timestamp");
        let timestamp_seconds = timestamp / 1000;
        let block_timestamp = chrono::NaiveDateTime::from_timestamp(timestamp_seconds / 1000, (timestamp_seconds % 1000).try_into().unwrap());
        let num_txs = block.header.num_txns as i32;

        let new_block_sync = models::NewBlockSync {
          block_height: &(block_height as i32),
          block_timestamp: &block_timestamp,
          num_txs: &num_txs,
        };

        if block.header.num_txns > 0 {
          let txs_result = self.zil_client.get_block_txs(&height)?;
          let block_txs = txs_result.list();

          trace!("ProcessBlock: block {} found txs {}", height, block_txs.len());
          for tx_hash in block_txs {
            self.process_tx(&conn, tx_hash, &new_block_sync)?;
          }
        }

        db::insert_block_sync(&conn, new_block_sync)?;
        debug!("ProcessBlock: block complete {} {}", &block_height, &num_txs);
        Ok(())
      })?;

    Ok(NextFetch::empty())
  }

  /// query one single block from chain based on given height.
  //  list all transactions on block and queue all with `SaveTx` job.
  fn process_tx(&self, conn: &PgConnection, tx_hash: String, block: &models::NewBlockSync) -> Result<(), utils::FetchError> {
    
    trace!("ProcessTx: handle {} {}", block.block_height, tx_hash);

    let tx_result = self.zil_client.get_transaction(&tx_hash)?;
    let events = tx_result.receipt.events();
    let events_len = events.len();
    if events_len > 0 {
      trace!("ProcessTx: processing events {}", events_len);
    }

    let pubkey_hex = &tx_result.sender_pub_key[2..];
    let sender_pubkey = hex::decode(pubkey_hex).expect("invalid public key");
    let pub_key_hash: Vec<u8> = digest::digest(&digest::SHA256, &sender_pubkey).as_ref().to_vec();
    let address_bytes = &pub_key_hash[pub_key_hash.len() - 20..];
    let initiator_address = format!("0x{}", hex::encode(&address_bytes));

    let formatted_tx_hash = format!("0x{}", &tx_hash).as_str().to_owned();

    for (event_index, event) in events.iter().enumerate() {
      let event_type = match Event::from_str(event._eventname.as_str()) {
        Some(event_type) => event_type,
        None => continue,
      };
      match event_type {
        Event::Minted | Event::Burnt | Event::Swapped => {
          if !self.config.pool_contract_hashes.contains(&event.address) { continue }
        },
        Event::Claimed => {
          if !self.config.distributor_contract_hashes.contains(&event.address) { continue }
        }
      };

      debug!("ProcessTx: event {} {} {}", &formatted_tx_hash, event_index, event._eventname);

      let chain_event = ChainEvent {
        block_height: block.block_height.clone(),
        block_timestamp: block.block_timestamp.clone(),
        tx_hash: formatted_tx_hash.clone(),
        event_index: event_index.clone() as i32,
        contract_address: event.address.clone(),
        initiator_address: initiator_address.clone(),
        name: event._eventname.clone(),
        params: event.params.clone(),
      };

      debug!("chainEvent: {:?}", chain_event);

      self.process_event(conn, &block, &tx_result, &chain_event)?;
    }
    Ok(())
  }

  /// poll chain events from database and persist events into database
  //  queue events for retry if failed.
  fn process_event(&self, conn: &PgConnection, block: &models::NewBlockSync, tx_result: &TxResult, event: &ChainEvent) -> PersistResult {
    let event_type = Event::from_str(event.name.as_str()).unwrap();
    println!("{}", event_type);
    let persist = match event_type {
      Event::Minted => persist_mint_event,
      Event::Burnt => persist_burn_event,
      Event::Swapped => persist_swap_event,
      Event::Claimed => persist_claim_event,
    };
    persist(conn, &block, &tx_result, &event)
  }
}

impl Actor for EventFetchActor {
  type Context = SyncContext<Self>;

  fn started(&mut self, _: &mut SyncContext<Self>) {
    info!("Event fetch actor started up.")
  }
}

impl Handler<Fetch> for EventFetchActor {
  type Result = ();

  fn handle(&mut self, msg: Fetch, _ctx: &mut SyncContext<Self>) -> () {
    let job = msg.job.clone();
    let result = match job {
      FetchJob::QueryNewBlocksParams(params) => {
        let prev_height = params.prev_height;
        self.query_new_blocks(prev_height)
      }
      FetchJob::ProcessBlockParams(params) => {
        let height = params.height;
        self.process_block(height)
      }
    };

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

fn persist_mint_event(conn: &PgConnection, _block: &models::NewBlockSync, tx_result: &TxResult, chain_event: &ChainEvent) -> PersistResult {
  let name = chain_event.name.as_str();
  if name != "PoolMinted" {
    return Ok(false)
  }

  let minter_address = chain_event.params.pointer("/0/value").unwrap().as_str().expect("Malformed event log!");
  let router_address = chain_event.params.pointer("/1/value").unwrap().as_str().expect("Malformed event log!");
  let amount_0 = chain_event.params.pointer("/2/value").unwrap().as_str().expect("Malformed event log!");
  let amount_1 = chain_event.params.pointer("/3/value").unwrap().as_str().expect("Malformed event log!");
  let liquidity = chain_event.params.pointer("/4/value").unwrap().as_str().expect("Malformed event log!");

  let minter_address_bytes = hex::decode(&minter_address[2..]).unwrap().to_base32();
  let initiator_address_bech32 = encode("zil", &minter_address_bytes).expect("invalid sender address");

  let router_address_bytes = hex::decode(&router_address[2..]).unwrap().to_base32();
  let router_address_bech32 = encode("zil", &router_address_bytes).expect("invalid sender address");

  let add_liquidity = models::NewLiquidityChange {
    transaction_hash: &chain_event.tx_hash,
    event_sequence: &chain_event.event_index,
    block_height: &chain_event.block_height,
    block_timestamp: &chain_event.block_timestamp,
    initiator_address: &initiator_address_bech32,
    router_address: &router_address_bech32,
    pool_address: &chain_event.contract_address,
    amount_0: &BigDecimal::from_str(amount_0).unwrap(),
    amount_1: &BigDecimal::from_str(amount_1).unwrap(),
    liquidity: &BigDecimal::from_str(liquidity).unwrap(),
  };

  debug!("Inserting: {:?}", add_liquidity);
  db::insert_liquidity_change(add_liquidity, &conn).map(|_| true)
}

fn persist_burn_event(conn: &PgConnection, _block: &models::NewBlockSync, tx_result: &TxResult, chain_event: &ChainEvent) -> PersistResult {
  let name = chain_event.name.as_str();
  if name != "PoolBurnt" {
    return Ok(false)
  }

  let burner_address = chain_event.params.pointer("/0/value").unwrap().as_str().expect("Malformed event log!");
  let router_address = chain_event.params.pointer("/1/value").unwrap().as_str().expect("Malformed event log!");
  let amount_0 = chain_event.params.pointer("/2/value").unwrap().as_str().expect("Malformed event log!");
  let amount_1 = chain_event.params.pointer("/3/value").unwrap().as_str().expect("Malformed event log!");
  let liquidity = chain_event.params.pointer("/4/value").unwrap().as_str().expect("Malformed event log!");

  let burner_address_bytes = hex::decode(&burner_address[2..]).unwrap().to_base32();
  let initiator_address_bech32 = encode("zil", &burner_address_bytes).expect("invalid sender address");

  let router_address_bytes = hex::decode(&router_address[2..]).unwrap().to_base32();
  let router_address_bech32 = encode("zil", &router_address_bytes).expect("invalid sender address");

  let remove_liquidity = models::NewLiquidityChange {
    transaction_hash: &chain_event.tx_hash,
    event_sequence: &chain_event.event_index,
    block_height: &chain_event.block_height,
    block_timestamp: &chain_event.block_timestamp,
    initiator_address: &initiator_address_bech32,
    pool_address: &chain_event.contract_address,
    router_address: &router_address_bech32,
    amount_0: &BigDecimal::from_str(amount_0).unwrap().neg(),
    amount_1: &BigDecimal::from_str(amount_1).unwrap().neg(),
    liquidity: &BigDecimal::from_str(liquidity).unwrap().neg(),
  };

  debug!("Inserting: {:?}", remove_liquidity);
  db::insert_liquidity_change(remove_liquidity, &conn).map(|_| true)
}

fn persist_swap_event(conn: &PgConnection, _block: &models::NewBlockSync, _tx_result: &TxResult, chain_event: &ChainEvent) -> PersistResult {
  let name = chain_event.name.as_str();
  if name != "PoolSwapped" {
    return Ok(false)
  }

  let initiator_address = chain_event.params.pointer("/0/value").unwrap().as_str().expect("Malformed event log!");
  let router_address = chain_event.params.pointer("/1/value").unwrap().as_str().expect("Malformed event log!");
  let amount_0_in = chain_event.params.pointer("/2/value").unwrap().as_str().expect("Malformed event log!");
  let amount_1_in = chain_event.params.pointer("/3/value").unwrap().as_str().expect("Malformed event log!");
  let amount_0_out = chain_event.params.pointer("/4/value").unwrap().as_str().expect("Malformed event log!");
  let amount_1_out = chain_event.params.pointer("/5/value").unwrap().as_str().expect("Malformed event log!");

  let to_address = chain_event.params.pointer("/6/value").unwrap().as_str().expect("Malformed event log!");

  let initiator_address_bytes = hex::decode(&initiator_address[2..]).unwrap().to_base32();
  let initiator_address_bech32 = encode("zil", &initiator_address_bytes).expect("invalid sender address");

  let router_address_bytes = hex::decode(&router_address[2..]).unwrap().to_base32();
  let router_address_bech32 = encode("zil", &router_address_bytes).expect("invalid pool address");

  let to_address_bytes = hex::decode(&to_address[2..]).unwrap().to_base32();
  let to_address_bech32 = encode("zil", &to_address_bytes).expect("invalid recipient address");

  let new_swap = models::NewSwap {
    transaction_hash: &chain_event.tx_hash,
    event_sequence: &chain_event.event_index,
    block_height: &chain_event.block_height,
    block_timestamp: &chain_event.block_timestamp,
    initiator_address: &initiator_address_bech32,
    pool_address: &chain_event.contract_address,
    router_address: &router_address_bech32,
    to_address: &to_address_bech32,
    amount_0_in: &BigDecimal::from_str(amount_0_in).unwrap(),
    amount_1_in: &BigDecimal::from_str(amount_1_in).unwrap(),
    amount_0_out: &BigDecimal::from_str(amount_0_out).unwrap(),
    amount_1_out: &BigDecimal::from_str(amount_1_out).unwrap(),
  };

  debug!("Inserting: {:?}", new_swap);
  db::insert_swap(new_swap, &conn).map(|_| true)
}

fn persist_claim_event(conn: &PgConnection, _block: &models::NewBlockSync, _tx_result: &TxResult, chain_event: &ChainEvent) -> PersistResult {
  let name = chain_event.name.as_str();
  if name != "Claimed" {
    return Ok(false)
  }

  let epoch_number = chain_event.params.pointer("/0/value").unwrap().as_str().expect("Malformed event log!");
  let recipient_address = chain_event.params.pointer("/1/value/arguments/0").unwrap().as_str().expect("Malformed event log!");
  let amount = chain_event.params.pointer("/1/value/arguments/1").unwrap().as_str().expect("Malformed event log!");

  let address_bytes = hex::decode(&recipient_address[2..]).unwrap().to_base32();
  let initiator_address = encode("zil", &address_bytes).expect("invalid sender address");

  let new_claim = models::NewClaim {
    transaction_hash: &chain_event.tx_hash,
    event_sequence: &chain_event.event_index,
    block_height: &chain_event.block_height,
    block_timestamp: &chain_event.block_timestamp,
    initiator_address: &initiator_address,
    distributor_address: &chain_event.contract_address,
    epoch_number: &epoch_number.parse::<i32>().expect("Malformed event log"),
    amount: &BigDecimal::from_str(amount).unwrap(),
  };

  debug!("Inserting: {:?}", new_claim);
  db::insert_claim(new_claim, &conn).map(|_| true)
}
