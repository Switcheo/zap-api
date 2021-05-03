

//! Diesel does not support tokio, so we have to run it in separate threads using the web::block
//! function which offloads blocking code (like Diesel's) in order to not block the server's thread.

#[macro_use]
extern crate diesel;

#[macro_use]
extern crate diesel_migrations;
embed_migrations!();

use actix::{Actor};
use actix_cors::{Cors};
use actix_web::{get, web, App, Error, HttpResponse, HttpServer, Responder};
use bigdecimal::{BigDecimal, Signed, Zero};
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use hex::{encode};
use serde::{Deserialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{SystemTime};
use std::str;

mod db;
mod constants;
mod models;
mod schema;
mod worker;
mod responses;
mod pagination;
mod distribution;
mod utils;

use crate::constants::{Network};
use crate::distribution::{Distribution, EpochInfo};

type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;

#[derive(Deserialize)]
struct PaginationInfo {
  per_page: Option<i64>,
  page: Option<i64>,
}

#[derive(Deserialize)]
struct AddressInfo {
  pool: Option<String>,
  address: Option<String>,
}

#[derive(Deserialize)]
struct TimeInfo {
  timestamp: Option<i64>,
}

#[derive(Deserialize)]
struct PeriodInfo {
  from: Option<i64>,
  until: Option<i64>,
}

/// Test endpoint.
#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello zap!")
}

/// Gets swaps.
#[get("/swaps")]
async fn get_swaps(
    query: web::Query<PaginationInfo>,
    filter: web::Query<AddressInfo>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
    let conn = pool.get().expect("couldn't get db connection from pool");

    // use web::block to offload blocking Diesel code without blocking server thread
    let swaps = web::block(move || db::get_swaps(&conn, query.per_page, query.page, filter.pool.as_ref(), filter.address.as_ref()))
        .await
        .map_err(|e| {
            eprintln!("{}", e);
            HttpResponse::InternalServerError().finish()
        })?;

    Ok(HttpResponse::Ok().json(swaps))
}

/// Get liquidity changes.
#[get("/liquidity_changes")]
async fn get_liquidity_changes(
  query: web::Query<PaginationInfo>,
  filter: web::Query<AddressInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  // use web::block to offload blocking Diesel code without blocking server thread
  let liquidity_changes = web::block(move || db::get_liquidity_changes(&conn, query.per_page, query.page, filter.pool.as_ref(), filter.address.as_ref()))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(liquidity_changes))
}

/// Get the swap volume in zil / tokens for the given period for all pools.
#[get("/volume")]
async fn get_volume(
  query: web::Query<PeriodInfo>,
  filter: web::Query<AddressInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  let volumes = web::block(move || db::get_volume(&conn, filter.address.as_ref(), query.from, query.until))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(volumes))
}

/// Get pool transactions including both swaps and liquidity changes.
#[get("/transactions")]
async fn get_transactions(
  query: web::Query<PeriodInfo>,
  pagination: web::Query<PaginationInfo>,
  filter: web::Query<AddressInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  let transactions = web::block(move || db::get_transactions(&conn, filter.address.as_ref(), filter.pool.as_ref(), query.from, query.until, pagination.per_page, pagination.page))
      .await
      .map_err(|e| {
          eprintln!("load error {}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(transactions))
}

/// Get liquidity for all pools.
#[get("/liquidity")]
async fn get_liquidity(
  query: web::Query<TimeInfo>,
  filter: web::Query<AddressInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  // use web::block to offload blocking Diesel code without blocking server thread
  let liquidity = web::block(move || db::get_liquidity(&conn, query.timestamp, filter.address.as_ref()))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(liquidity))
}

/// Get time-weighted liquidity for all pools.
#[get("/weighted_liquidity")]
async fn get_weighted_liquidity(
  query: web::Query<PeriodInfo>,
  filter: web::Query<AddressInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  // use web::block to offload blocking Diesel code without blocking server thread
  let liquidity = web::block(move || db::get_time_weighted_liquidity(&conn, query.from, query.until, filter.address.as_ref()))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(liquidity))
}

/// Get distribution epoch information.
#[get("/epoch/info")]
async fn get_epoch_info() -> Result<HttpResponse, Error> {
  Ok(HttpResponse::Ok().json(EpochInfo::default()))
}

/// Generate data for the an ended epoch and save it to db.
// steps:
// get pools (filtered for the ones to award - epoch 0 all, epoch 1 only xsgd & gzil)
// for each pool:
// 1. get total time weighted liquidity from start_time to end_time
// 2. get time weighted liquidity from start_time to end_time for each address that has liquidity at start_time
// split reward by pool and time weighted liquidity
// if epoch 0, get swap_volume and split additional reward by volume
#[get("epoch/generate")]
async fn generate_epoch(
  pool: web::Data<DbPool>,
  network: web::Data<Network>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  let result = web::block(move || {
    let current_epoch = EpochInfo::default();
    let current_epoch_number = current_epoch.epoch_number();
    let mut epoch_number = std::cmp::max(0, current_epoch_number - 1) as i32;

    if epoch_number == 0 && db::epoch_exists(&conn, epoch_number)? {
      epoch_number = 1 // generate trader epoch
    }

    let epoch_info = EpochInfo::new(epoch_number as i64);

    let start = Some(epoch_info.current_epoch_start());
    let end = Some(epoch_info.current_epoch_end());

    let current_time = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .expect("invalid server time")
      .as_secs() as i64;

    if current_time < end.unwrap() {
      return Ok(String::from("Epoch not yet over!"))
    }

    if db::epoch_exists(&conn, epoch_number)? {
      return Ok(String::from("Epoch already generated!"))
    }

    // get pool TWAL and individual TWAL
    struct PoolDistribution {
      tokens: BigDecimal,
      weighted_liquidity: BigDecimal,
    }
    let pt = epoch_info.tokens_for_liquidity_providers();
    let distribution: HashMap<String, PoolDistribution> =
      if epoch_info.is_initial() {
        let total_liquidity: BigDecimal = db::get_time_weighted_liquidity(&conn, start, end, None)?.into_iter().map(|i| i.amount).sum();
        db::get_pools(&conn)?.into_iter().map(|pool| {
          (pool,
            PoolDistribution{ // share distribution fully
              tokens: utils::round_down(pt.clone(), 0),
              weighted_liquidity: total_liquidity.clone(),
            }
          )
        }).collect()
      } else {
        let pool_weights = network.incentived_pools();
        let total_weight: u32 = pool_weights.values().into_iter().sum();
        db::get_time_weighted_liquidity(&conn, start, end, None)?.into_iter().filter_map(|i| {
          if let Some(weight) = pool_weights.get(&i.pool) {
            Some((i.pool,
              PoolDistribution{ // each pool has a weighted allocation
                tokens: utils::round_down(pt.clone() * BigDecimal::from(*weight) / BigDecimal::from(total_weight), 0),
                weighted_liquidity: i.amount,
              }
            ))
          } else {
            None
          }
        }).collect()
      };

    let mut accumulator: HashMap<String, BigDecimal> = HashMap::new();

    // for each individual TWAL, calculate the tokens
    if !epoch_info.trader_epoch() {
      let user_liquidity = db::get_time_weighted_liquidity_by_address(&conn, start, end)?;
      for l in user_liquidity.into_iter() {
        if let Some(pool) = distribution.get(&l.pool) {
          let share = utils::round_down(l.amount * pool.tokens.clone() / pool.weighted_liquidity.clone(), 0);
          let current = accumulator.entry(l.address).or_insert(BigDecimal::default());
          *current += share
        }
      }
    }

    // if initial epoch, add distr for swap volumes
    let tt = epoch_info.tokens_for_traders();
    if tt.is_positive() {
      let total_volume: BigDecimal = db::get_volume(&conn, None, start, end)?.into_iter().map(|v| v.in_zil_amount + v.out_zil_amount).sum();
      let user_volume = db::get_volume_by_address(&conn, start, end)?;
      for v in user_volume.into_iter() {
        let share = utils::round_down(tt.clone() * v.amount.clone() / total_volume.clone(), 0);
        let current = accumulator.entry(v.address).or_insert(BigDecimal::default());
        *current += share
      }
    }

    // add developer share
    let dt = epoch_info.tokens_for_developers();
    if dt.is_positive() && !epoch_info.trader_epoch() {
      let current = accumulator.entry(network.developer_address()).or_insert(BigDecimal::default());
      *current += dt
    }

    let total_distributed = accumulator.values().fold(BigDecimal::default(), |acc, x| acc + x);
    if total_distributed > epoch_info.tokens_for_epoch() {
      panic!("Total distributed tokens > target tokens for epoch: {} > {}", total_distributed, epoch_info.tokens_for_epoch())
    }

    let leaves = Distribution::from(accumulator);
    let tree = distribution::construct_merkle_tree(leaves);
    let proofs = distribution::get_proofs(tree.clone());
    let records: Vec<models::NewDistribution> = proofs.into_iter().map(|(d, p)| {
      models::NewDistribution{
        epoch_number,
        address_bech32: d.address(),
        address_hex: encode(d.address_bytes()),
        amount: d.amount(),
        proof: p,
      }
    }).collect();

    if db::epoch_exists(&conn, epoch_number)? {
      return Ok(String::from("Epoch already generated!"))
    }

    for r in records.chunks(10000).into_iter() {
      db::insert_distributions(r.to_vec(), &conn).expect("Failed to insert distributions!");
    };

    Ok::<String, diesel::result::Error>(encode(tree.root().data().clone().1))
  }).await
    .map_err(|e| {
      eprintln!("{}", e);
      HttpResponse::InternalServerError().finish()
    })?;

  Ok(HttpResponse::Ok().json(result))
}

/// Gets distribution pool weights.
#[get("/distribution/pool_weights")]
async fn get_pool_weights(
    pool: web::Data<DbPool>,
    network: web::Data<Network>,
) -> Result<HttpResponse, Error> {
    let conn = pool.get().expect("couldn't get db connection from pool");

    // use web::block to offload blocking Diesel code without blocking server thread
    let pools = web::block(move || db::get_pools(&conn))
        .await
        .map_err(|e| {
            eprintln!("{}", e);
            HttpResponse::InternalServerError().finish()
        })?;

    let mut result: HashMap<String, u32> = pools.into_iter().map(|x| (x, 0)).collect();
    for (key, value) in network.incentived_pools().into_iter() {
      result.insert(key, value);
    }

    Ok(HttpResponse::Ok().json(result))
}

/// Get distribution data for the given address
// steps:
// get pools (filtered for the ones to award - epoch 0 all, epoch 1 only xsgd & gzil)
// for each pool:
// 1. get total time weighted liquidity from start_time to end_time
// 2. get time weighted liquidity from start_time to end_time for each address that has liquidity at start_time
// split reward by pool and time weighted liquidity
// if epoch 0, get swap_volume and split additional reward by volume
#[get("distribution/current/{address}")]
async fn get_current_distribution(
  pool: web::Data<DbPool>,
  network: web::Data<Network>,
  web::Path(address): web::Path<String>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");
  let epoch_info = EpochInfo::default();
  let start = Some(epoch_info.current_epoch_start());
  let end = Some(epoch_info.current_epoch_end());

  let result = web::block(move || {
    let mut accumulator: HashMap<String, BigDecimal> = HashMap::new();

    // get pool TWAL and individual TWAL
    struct PoolDistribution {
      tokens: BigDecimal,
      weighted_liquidity: BigDecimal,
    }
    let pt = epoch_info.tokens_for_liquidity_providers();
    let distribution: HashMap<String, PoolDistribution> =
      if epoch_info.is_initial() {
        let total_liquidity: BigDecimal = db::get_time_weighted_liquidity(&conn, start, end, None)?.into_iter().map(|i| i.amount).sum();
        db::get_pools(&conn)?.into_iter().map(|pool| {
          (pool,
            PoolDistribution{ // share distribution fully
              tokens: utils::round_down(pt.clone(), 0),
              weighted_liquidity: total_liquidity.clone(),
            }
          )
        }).collect()
      } else {
        let pool_weights = network.incentived_pools();
        let total_weight: u32 = pool_weights.values().into_iter().sum();
        db::get_time_weighted_liquidity(&conn, start, end, None)?.into_iter().filter_map(|i| {
          if let Some(weight) = pool_weights.get(&i.pool) {
            Some((i.pool,
              PoolDistribution{ // each pool has a weighted allocation
                tokens: utils::round_down(pt.clone() * BigDecimal::from(*weight) / BigDecimal::from(total_weight), 0),
                weighted_liquidity: i.amount,
              }
            ))
          } else {
            None
          }
        }).collect()
      };

    // for each individual TWAL, calculate the tokens
    let user_liquidity = db::get_time_weighted_liquidity(&conn, start, end, Some(&address))?;
    for l in user_liquidity.into_iter() {
      if let Some(pool) = distribution.get(&l.pool) {
        let share = utils::round_down(l.amount * pool.tokens.clone() / pool.weighted_liquidity.clone(), 0);
        let current = accumulator.entry(l.pool).or_insert(BigDecimal::default());
        *current += share
      }
    }

    // add developer share
    if network.developer_address() == address {
      let current = accumulator.entry("developer".to_string()).or_insert(BigDecimal::default());
      *current += epoch_info.tokens_for_developers()
    }

    Ok::<HashMap<String, BigDecimal>, diesel::result::Error>(accumulator)
  }).await
    .map_err(|e| {
      eprintln!("{}", e);
      HttpResponse::InternalServerError().finish()
    })?;

  Ok(HttpResponse::Ok().json(result))
}

/// Get epoch distribution data.
#[get("/epoch/data/{epoch_number}")]
async fn get_epoch_data(
  pool: web::Data<DbPool>,
  filter: web::Query<AddressInfo>,
  web::Path(epoch_number): web::Path<i32>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  let distributions = web::block(move || db::get_distributions(&conn, Some(epoch_number), filter.address.as_ref()))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(distributions))
}

/// Get distribution data by address.
#[get("/distribution/data/{address}")]
async fn get_distribution_data(
  pool: web::Data<DbPool>,
  web::Path(address): web::Path<String>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  let distributions = web::block(move || db::get_distributions_by_address(&conn, &address))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(distributions))
}

/// Get token pairs (motivated by CoinGecko API)
#[get("/pairs")]
async fn get_token_pairs(

) -> Result<HttpResponse, Error> {
  unsafe {
    match &TOKENS {
      Some(tokens) => {
        let pairs: Vec<models::TokenPair> = tokens.iter().map(|token| {
          models::TokenPair {
            ticker_id: format!("{}_{}", token.symbol, "ZIL"),
            base: token.symbol.clone(),
            target: "ZIL".to_string(),
          }
        }).collect();
        return Ok(HttpResponse::Ok().json(pairs));
      },
      None => Ok(HttpResponse::Ok().body("Tokens not loaded!"))
    }
  }
}

/// Get token tickers (motivated by CoinGecko API)
#[get("/tickers")]
async fn get_token_tickers(
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {

  let conn = pool.get().expect("couldn't get db connection from pool");

  let volumes = web::block(move || db::get_volume(&conn, None, None, None))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  let empty_volume = models::Volume {
    pool: "".to_string(),
    in_zil_amount: BigDecimal::zero(),
    out_token_amount: BigDecimal::zero(),
    out_zil_amount: BigDecimal::zero(),
    in_token_amount: BigDecimal::zero(),
  };

  unsafe {
    match &TOKENS {
      Some(tokens) => {
        let tickers: Vec<models::TokenTicker> = tokens.iter().map(|token| {
          let volume = volumes.iter()
            .find(|v| v.pool == token.address_bech32)
            .unwrap_or(&empty_volume);
          models::TokenTicker {
            ticker_id: format!("{}_{}", token.symbol, "ZIL"),
            base_currency: token.symbol.clone(),
            target_currency: "ZIL".to_string(),
            // last_price: 0.0,
            base_volume: (&volume.in_token_amount + &volume.out_token_amount),
            target_volume: (&volume.in_zil_amount + &volume.out_zil_amount),
            // bid: 0.0,
            // ask: 0.0,
            // high: 0.0,
            // low: 0.0,
          }
        }).collect();
        return Ok(HttpResponse::Ok().json(tickers));
      },
      None => Ok(HttpResponse::Ok().body("Tokens not loaded!"))
    }
  }
}

static mut TOKENS: Option<Vec<models::Token>> = None;

async fn reload_tokens() {
  let url = reqwest::Url::parse("https://api.zilstream.com/tokens").expect("couldn't parse token api url");
  let result = reqwest::get(url)
    .await.expect("couldn't retrieve tokens")
    .json::<Vec<responses::ZilStreamToken>>()
    .await.expect("couldn't parse token result");
  
    let mut tokens = result.iter().map(|item| {
      return models::Token {
        name: item.name.clone(),
        symbol: item.symbol.clone(),
        address_bech32: item.address_bech32.clone(),
        icon: item.icon.clone(),
        website: item.website.clone(),
        decimals: item.decimals,
        init_supply: item.init_supply.clone(),
        max_supply: item.max_supply.clone(),
        total_supply: item.total_supply.clone(),
        current_supply: item.current_supply.clone(),
      }
    }).collect::<Vec<models::Token>>();

    tokens.sort_by(|lhs, rhs| {
      // place ZWAP as the first token
      if lhs.symbol == "ZWAP" { return Ordering::Less; }
      if rhs.symbol == "ZWAP" { return Ordering::Greater; }
      return lhs.symbol.cmp(&rhs.symbol);
    });

  unsafe {
    TOKENS = Some(tokens);
  }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  std::env::set_var("RUST_LOG", "actix_web=info");
  env_logger::init();
  let env_path = std::env::var("ENV_FILE").unwrap_or(String::from("./.env"));
  dotenv::from_path(env_path).ok();

  // set up database connection pool
  let connspec = std::env::var("DATABASE_URL").expect("DATABASE_URL env var missing.");
  let manager = ConnectionManager::<PgConnection>::new(connspec);
  let pool = r2d2::Pool::builder()
      .build(manager)
      .expect("Failed to create db pool.");

  // get network
  let network_str = std::env::var("NETWORK").unwrap_or(String::from("testnet"));
  let network = match network_str.as_str() {
    "testnet" => Network::TestNet,
    "mainnet" => Network::MainNet,
    _ => panic!("Invalid network string")
  };

  // run worker
  let run_worker = std::env::var("RUN_WORKER").unwrap_or(String::from("false"));
  if run_worker == "true" || run_worker == "t" || run_worker == "1" {
    let _addr = worker::Coordinator::new(pool.clone()).start();
  }

  // run migrations
  let conn = pool.get().expect("couldn't get db connection from pool");
  embedded_migrations::run(&conn).expect("failed to run migrations.");

  // reload tokens list
  reload_tokens().await;

  let bind = std::env::var("BIND").or(Ok::<String, Error>(String::from("127.0.0.1:3000"))).unwrap();
  println!("Starting server at: {}", &bind);
  HttpServer::new(move || {
    App::new()
      .data(pool.clone())
      .data(network.clone())
      .wrap(Cors::permissive())
      .service(hello)
      .service(generate_epoch)
      .service(get_epoch_info)
      .service(get_epoch_data)
      .service(get_distribution_data)
      .service(get_current_distribution)
      .service(get_pool_weights)
      .service(get_swaps)
      .service(get_volume)
      .service(get_transactions)
      .service(get_liquidity_changes)
      .service(get_liquidity)
      .service(get_weighted_liquidity)

      .service(get_token_pairs)
      .service(get_token_tickers)
  })
  .bind(bind)?
  .run()
  .await
}
