

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
use bigdecimal::{BigDecimal};
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime};
use std::collections::HashMap;
use constants::{zwap_emission};

mod db;
mod constants;
mod models;
mod schema;
mod worker;
mod responses;
mod pagination;
mod utils;

use crate::constants::{Network};

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
    let swaps = web::block(move || db::fetch_swaps(&conn, query.per_page, query.page, filter.pool.as_ref(), filter.address.as_ref()))
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
  let liquidity_changes = web::block(move || db::fetch_liquidity_changes(&conn, query.per_page, query.page, filter.pool.as_ref(), filter.address.as_ref()))
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
  query: web::Query<TimeInfo>,
  filter: web::Query<AddressInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  // use web::block to offload blocking Diesel code without blocking server thread
  let liquidity = web::block(move || db::get_time_weighted_liquidity(&conn, Some(0), query.timestamp, filter.address.as_ref()))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(liquidity))
}

// generate epoch
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
  let epoch_info = epoch_info();

  let result = web::block(move || {
    // get weights
    let pools = if epoch_info.current_epoch == 0 {
      db::get_pools(&conn)?
        .into_iter().map(|p| {
          (p, 1 as u32)
        }).collect()
    } else {
      network.incentived_pools()
    };
    let total_weight: u32 = pools.values().into_iter().sum();

    // get pool TWAL and individual TWAL
    struct PoolDistribution {
      tokens: BigDecimal,
      weighted_liquidity: BigDecimal,
    }
    let totals: HashMap<String, PoolDistribution> = db::get_time_weighted_liquidity(&conn, None, None, None)?.into_iter().map(|i| {
      let weight: u32 = *pools.get(&i.pool).unwrap();
      (i.pool,
        PoolDistribution{
          tokens: utils::round(BigDecimal::from(epoch_info.tokens_per_epoch) * BigDecimal::from(10u64.pow(12)) * BigDecimal::from(total_weight) / BigDecimal::from(weight), 0),
          weighted_liquidity: i.amount
        }
      )
    }).collect();
    let breakdowns = db::get_time_weighted_liquidity_by_address(&conn, None, None)?;

    // for each individual TWAL, calculate the tokens
    let mut accumulator: HashMap<String, BigDecimal> = HashMap::new();
    for b in breakdowns.into_iter() {
      let pool = totals.get(&b.pool).unwrap();
      let share = utils::round(b.amount * pool.tokens.clone() / pool.weighted_liquidity.clone(), 0);
      let current = accumulator.entry(b.address).or_insert(BigDecimal::default());
      *current += share
    }

    // if 0 epoch, add volumes
    // TODO!

    Ok::<HashMap<String, BigDecimal>, diesel::result::Error>(accumulator)
  }).await
    .map_err(|e| {
      eprintln!("{}", e);
      HttpResponse::InternalServerError().finish()
    })?;

  Ok(HttpResponse::Ok().json(result))
}

#[derive(Serialize)]
struct EpochInfo {
  epoch_period: i64,
  tokens_per_epoch: u32,
  first_epoch_start: i64,
  next_epoch_start: i64,
  total_epoch: u32,
  current_epoch: i64,
}

/// Get distribution epoch information.
#[get("/epoch/info")]
async fn get_epoch_info() -> Result<HttpResponse, Error> {
  Ok(HttpResponse::Ok().json(epoch_info()))
}

fn epoch_info() -> EpochInfo {
  let first_epoch_start = zwap_emission::DISTRIBUTION_START_TIME;
  let epoch_period = zwap_emission::EPOCH_PERIOD;
  let total_epoch = zwap_emission::TOTAL_NUMBER_OF_EPOCH;
  let tokens_per_epoch = zwap_emission::TOKENS_PER_EPOCH;
  let current_time = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .expect("invalid server time")
    .as_secs() as i64;

  let current_epoch = std::cmp::max(0, (current_time - first_epoch_start) / epoch_period);
  let next_epoch_start =
    if current_time < first_epoch_start {
      first_epoch_start
    } else {
      std::cmp::min(current_epoch + 1, total_epoch.into()) * epoch_period + first_epoch_start
    };

  EpochInfo {
    epoch_period,
    tokens_per_epoch,
    first_epoch_start,
    next_epoch_start,
    total_epoch,
    current_epoch,
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
    let _addr = worker::Worker::new(pool.clone()).start();
  }

  // run migrations
  let conn = pool.get().expect("couldn't get db connection from pool");
  embedded_migrations::run(&conn).expect("failed to run migrations.");

  let bind = std::env::var("BIND").or(Ok::<String, Error>(String::from("127.0.0.1:3000"))).unwrap();
  println!("Starting server at: {}", &bind);
  HttpServer::new(move || {
    App::new()
      .data(pool.clone())
      .data(network.clone())
      .wrap(Cors::permissive())
      .service(hello)
      .service(get_epoch_info)
      .service(generate_epoch)
      .service(get_swaps)
      .service(get_volume)
      .service(get_liquidity_changes)
      .service(get_liquidity)
      .service(get_weighted_liquidity)
  })
  .bind(bind)?
  .run()
  .await
}
