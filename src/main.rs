

//! Diesel does not support tokio, so we have to run it in separate threads using the web::block
//! function which offloads blocking code (like Diesel's) in order to not block the server's thread.

#[macro_use]
extern crate diesel;

#[macro_use]
extern crate diesel_migrations;
embed_migrations!();

use actix::{Actor};
use actix_web::{get, web, App, Error, HttpResponse, HttpServer, Responder};
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime};
use constants::{zap_epoch};

mod db;
mod models;
mod schema;
mod worker;
mod responses;
mod pagination;
mod constants;

type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;

#[derive(Deserialize)]
struct PaginationInfo {
  per_page: Option<i64>,
  page: Option<i64>,
}

#[derive(Deserialize)]
struct TemporalPeriodInfo {
  from: Option<i64>,
  until: Option<i64>,
}

#[derive(Deserialize)]
struct AddressInfo {
  pool: Option<String>,
  address: Option<String>,
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


#[derive(Deserialize)]
struct LiquidityInfo {
  timestamp: Option<i64>,
  address: Option<String>,
}

/// Get liquidity for all pools.
#[get("/liquidity")]
async fn get_liquidity(
  query: web::Query<LiquidityInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  // use web::block to offload blocking Diesel code without blocking server thread
  let liquidity = web::block(move || db::get_liquidity(&conn, query.timestamp, query.address.as_ref()))
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
  query: web::Query<LiquidityInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  // use web::block to offload blocking Diesel code without blocking server thread
  let liquidity = web::block(move || db::get_time_weighted_liquidity(&conn, Some(0), query.timestamp, query.address.as_ref()))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(liquidity))
}

// generate epoch
// get pools (filtered for the ones to award - epoch 0 all, epoch 1 only xsgd & gzil)
// for each pool:
// get liquidity at start_time, * by duration
// get total time weighted liquidity?
// get time weighted liquidity grouped by address
// split reward by pool and time weighted liquidity
// if epoch 0, get swap_volume and split additional reward by volume

// get volume for period
#[get("/volume")]
async fn get_volume(
  query: web::Query<TemporalPeriodInfo>,
  filter: web::Query<AddressInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let conn = pool.get().expect("couldn't get db connection from pool");

  // use web::block to offload blocking Diesel code without blocking server thread
  let volumes = web::block(move || db::get_swap_volume(&conn, filter.address.as_ref(), query.from, query.until))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(volumes))
}

#[derive(Serialize)]
struct EpochInfoResponse {
  epoch_start: i64,
  max_epoch: u32,
  epoch_period: i64,
  next_epoch: i64,
  current_epoch: i64,
}

// get epoch data
#[get("/epoch/info")]
async fn get_epoch_info() -> Result<HttpResponse, Error> {
  let epoch_start = zap_epoch::EPOCH_START_TIME;
  let epoch_period = zap_epoch::EPOCH_PERIOD;
  let max_epoch = zap_epoch::MAX_EPOCH;
  let current_time = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .expect("invalid server time")
    .as_secs() as i64;

  let current_epoch = std::cmp::max(0, (current_time - epoch_start) / epoch_period);
  let next_epoch = if current_time < epoch_start {
    epoch_start
  }  else {
    std::cmp::min(current_epoch + 1, max_epoch.into()) * epoch_period + epoch_start
  };

  Ok(HttpResponse::Ok().json(EpochInfoResponse {
    epoch_start,
    epoch_period,
    current_epoch,
    next_epoch,
    max_epoch,
  }))
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
          .service(hello)
          .service(get_epoch_info)
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
