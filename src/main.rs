

//! Diesel does not support tokio, so we have to run it in separate threads using the web::block
//! function which offloads blocking code (like Diesel's) in order to not block the server's thread.

#[macro_use]
extern crate diesel;

#[macro_use]
extern crate diesel_migrations;
embed_migrations!();

#[macro_use]
extern crate log;

extern crate redis;

use actix::{Actor};
use actix_cors::{Cors};
use actix_web::{get, web, App, Error, HttpResponse, HttpServer, Responder, middleware::Logger};
use bigdecimal::{BigDecimal, Signed};
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use hex::{encode};
use serde::{Deserialize};
use std::collections::HashMap;
use std::time::{SystemTime};
use redis::Commands;

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
use crate::worker::{WorkerConfig};
use crate::distribution::{EpochInfo, Distribution, DistributionConfigs, Validate};

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
struct SwapInfo {
  pool: Option<String>,
  address: Option<String>,
  is_incoming: Option<bool>,
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

#[derive(Deserialize)]
struct ClaimInfo {
  address: Option<String>,
  distr_address: Option<String>,
  epoch_number: Option<i32>,
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
    filter: web::Query<SwapInfo>,
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
    let swaps = web::block(move || {
      let conn = pool.get().expect("couldn't get db connection from pool");
      db::get_swaps(&conn, query.per_page, query.page, filter.pool.as_deref(), filter.address.as_deref(), filter.is_incoming.as_ref())
    })
    .await.map_err(|e| {
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
  let liquidity_changes = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    db::get_liquidity_changes(&conn, query.per_page, query.page, filter.pool.as_deref(), filter.address.as_deref())
  })
  .await.map_err(|e| {
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
  let volumes = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    db::get_volume(&conn, filter.address.as_deref(), query.from, query.until)
  })
  .await.map_err(|e| {
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
  let transactions = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    db::get_transactions(&conn, filter.address.as_deref(), filter.pool.as_deref(), query.from, query.until, pagination.per_page, pagination.page)
  })
  .await.map_err(|e| {
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
  let liquidity = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    db::get_liquidity(&conn, query.timestamp, filter.address.as_deref())
  })
  .await.map_err(|e| {
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
  redis: web::Data<redis::Client>,
) -> Result<HttpResponse, Error> {
  let liquidity = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    let mut rconn = redis.get_connection().expect("couldn't get redis connection");
    db::get_time_weighted_liquidity(&conn, &mut rconn, query.from, query.until, filter.address.as_deref())
  })
  .await.map_err(|e| {
    eprintln!("{}", e);
    HttpResponse::InternalServerError().finish()
  })?;

  Ok(HttpResponse::Ok().json(liquidity))
}

/// Generate distribution data and save it to db.
// steps:
// get pools (filtered for the ones to award - epoch 0 all, epoch 1 only xsgd & gzil)
// for each pool:
// 1. get total time weighted liquidity from start_time to end_time
// 2. get time weighted liquidity from start_time to end_time for each address that has liquidity at start_time
// split reward by pool and time weighted liquidity
// if epoch 0, get swap_volume and split additional reward by volume
#[get("distribution/generate/{id}")]
async fn generate_epoch(
  pool: web::Data<DbPool>,
  distr_config: web::Data<DistributionConfigs>,
  redis: web::Data<redis::Client>,
  web::Path(id): web::Path<usize>,
) -> Result<HttpResponse, Error> {
  let result = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    let mut rconn = redis.get_connection().expect("couldn't get redis connection");
    if !var_enabled("RUN_GENERATE") {
      return Ok(String::from("Epoch generation disabled!"))
    }

    let distr = distr_config[id].clone();
    let current_epoch = EpochInfo::new(distr.emission(), None);
    let current_epoch_number = current_epoch.epoch_number();
    let epoch_number = std::cmp::max(0, current_epoch_number - 1);
    let epoch_info = EpochInfo::new(distr.emission(), Some(epoch_number as u32));

    if epoch_info.distribution_ended() {
      return Ok(String::from("Distribution ended!"))
    }

    let start = epoch_info.current_epoch_start();
    let end = epoch_info.current_epoch_end();

    let current_time = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .expect("invalid server time")
      .as_secs() as i64;

    if current_time < end.unwrap() {
      return Ok(String::from("Epoch not yet over!"))
    }

    if db::epoch_exists(&conn, distr.distributor_address(), &epoch_number)? {
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
        let total_liquidity: BigDecimal = db::get_time_weighted_liquidity(&conn, &mut rconn, start, end, None)?.into_iter().map(|i| i.amount).sum();
        db::get_pools(&conn)?.into_iter().map(|pool| {
          (pool,
            PoolDistribution{ // share distribution fully
              tokens: utils::round_down(pt.clone(), 0),
              weighted_liquidity: total_liquidity.clone(),
            }
          )
        }).collect()
      } else {
        let pool_weights = distr.incentivized_pools();
        let total_weight: u32 = pool_weights.values().into_iter().sum();
        db::get_time_weighted_liquidity(&conn, &mut rconn, start, end, None)?.into_iter().filter_map(|i| {
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
    let user_liquidity = db::get_time_weighted_liquidity_by_address(&conn, start, end)?;
    for l in user_liquidity.into_iter() {
      if let Some(pool) = distribution.get(&l.pool) {
        let share = utils::round_down(l.amount * pool.tokens.clone() / pool.weighted_liquidity.clone(), 0);
        let current = accumulator.entry(l.address).or_insert(BigDecimal::default());
        *current += share
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
    if dt.is_positive() {
      let current = accumulator.entry(distr.developer_address().to_owned()).or_insert(BigDecimal::default());
      *current += dt
    }

    let hive_address = "0x7ef6033783cef7720952394015da263a5501b8e3";
    let ht = match accumulator.get(hive_address) {
      Some (amount) => amount.clone(),
      None => BigDecimal::default(),
    };
    if ht.is_positive() {
      accumulator.remove(hive_address);

      let current = accumulator.entry(distr.developer_address().to_owned()).or_insert(BigDecimal::default());
      *current += ht
    }

    let total_distributed = accumulator.values().fold(BigDecimal::default(), |acc, x| acc + x);
    if total_distributed > epoch_info.tokens_for_epoch() {
      panic!("Total distributed tokens > target tokens for epoch: {} > {}", total_distributed, epoch_info.tokens_for_epoch())
    } else {
      info!("Total distributed tokens: {} out of max of {}", total_distributed, epoch_info.tokens_for_epoch());
    }

    let leaves = Distribution::from(accumulator);
    let tree = distribution::construct_merkle_tree(leaves);
    let proofs = distribution::get_proofs(tree.clone());
    let distributor_address = distr.distributor_address();
    let records: Vec<models::NewDistribution> = proofs.iter().map(|(d, p)| {
      models::NewDistribution{
        distributor_address: &distributor_address,
        epoch_number: &epoch_number,
        address_bech32: d.address_bech32(),
        address_hex: d.address_hex(),
        amount: d.amount(),
        proof: p.as_str(),
      }
    }).collect();

    if db::epoch_exists(&conn, &distributor_address, &epoch_number)? {
      return Ok(String::from("Epoch already generated!"))
    }

    for r in records.chunks(10000).into_iter() {
      db::insert_distributions(r.to_vec(), &conn).expect("Failed to insert distributions!");
    };

    Ok::<String, diesel::result::Error>(encode(tree.root().data().clone().1))
  })
  .await.map_err(|e| {
    eprintln!("{}", e);
    HttpResponse::InternalServerError().finish()
  })?;

  Ok(HttpResponse::Ok().json(result))
}

/// Get distribution config information.
#[get("/distribution/info")]
async fn get_distribution_info(
  distr_config: web::Data<DistributionConfigs>,
) -> Result<HttpResponse, Error> {
  Ok(HttpResponse::Ok().json(distr_config.get_ref()))
}

/// Get the current estimated distribution amounts for the given user address for the upcoming epochs
// steps:
// get pools (filtered for the ones to award - epoch 0 all, epoch 1 only xsgd & gzil)
// for each pool:
// 1. get total time weighted liquidity from start_time to end_time
// 2. get time weighted liquidity from start_time to end_time for each address that has liquidity at start_time
// split reward by pool and time weighted liquidity
// if epoch 0, get swap_volume and split additional reward by volume
#[get("/distribution/estimated_amounts/{user_address}")]
async fn get_distribution_amounts(
  pool: web::Data<DbPool>,
  distr_config: web::Data<DistributionConfigs>,
  redis: web::Data<redis::Client>,
  web::Path(user_address): web::Path<String>,
) -> Result<HttpResponse, Error> {
  let result = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    let mut rconn = redis.get_connection().expect("couldn't get redis connection");
    let mut r: HashMap<String, HashMap<String, BigDecimal>> = HashMap::new();

    for distr in distr_config.iter() {
      let mut accumulator: HashMap<String, BigDecimal> = HashMap::new();

      let epoch_info = EpochInfo::new(distr.emission(), None);
      let start = epoch_info.current_epoch_start();
      let end = epoch_info.current_epoch_end();

      // get pool TWAL and individual TWAL
      struct PoolDistribution {
        tokens: BigDecimal,
        weighted_liquidity: BigDecimal,
      }
      let pt = epoch_info.tokens_for_liquidity_providers();
      let distribution: HashMap<String, PoolDistribution> =
        if epoch_info.is_initial() {
          let total_liquidity: BigDecimal = db::get_time_weighted_liquidity(&conn, &mut rconn, start, end, None)?.into_iter().map(|i| i.amount).sum();
          db::get_pools(&conn)?.into_iter().map(|pool| {
            (pool,
              PoolDistribution{ // share distribution fully
                tokens: utils::round_down(pt.clone(), 0),
                weighted_liquidity: total_liquidity.clone(),
              }
            )
          }).collect()
        } else {
          let pool_weights = distr.incentivized_pools();
          let total_weight: u32 = pool_weights.values().into_iter().sum();
          db::get_time_weighted_liquidity(&conn, &mut rconn, start, end, None)?.into_iter().filter_map(|i| {
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
      let user_liquidity = db::get_time_weighted_liquidity(&conn, &mut rconn, start, end, Some(&user_address))?;
      for l in user_liquidity.into_iter() {
        if let Some(pool) = distribution.get(&l.pool) {
          let share = utils::round_down(l.amount * pool.tokens.clone() / pool.weighted_liquidity.clone(), 0);
          let current = accumulator.entry(l.pool).or_insert(BigDecimal::default());
          *current += share
        }
      }

      // add developer share
      if distr.developer_address() == user_address {
        let current = accumulator.entry("developer".to_string()).or_insert(BigDecimal::default());
        *current += epoch_info.tokens_for_developers()
      }

      r.insert(distr.distributor_address().to_string(), accumulator);
    }

    Ok::<HashMap<String, HashMap<String, BigDecimal>>, diesel::result::Error>(r)
  })
  .await.map_err(|e| {
    eprintln!("{}", e);
    HttpResponse::InternalServerError().finish()
  })?;

  Ok(HttpResponse::Ok().json(result))
}

/// Get distribution data by epoch.
#[get("/distribution/data/{distributor_address}/{epoch_number}")]
async fn get_distribution_data(
  pool: web::Data<DbPool>,
  filter: web::Query<AddressInfo>,
  web::Path((distributor_address, epoch_number)): web::Path<(String, i32)>,
) -> Result<HttpResponse, Error> {
  let distributions = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    db::get_distributions(&conn, Some(&distributor_address), Some(epoch_number), filter.address.as_deref())
  })
  .await.map_err(|e| {
    eprintln!("{}", e);
    HttpResponse::InternalServerError().finish()
  })?;

  Ok(HttpResponse::Ok().json(distributions))
}

/// Get distribution data for claimable (and unclaimed) epochs by user address.
#[get("/distribution/claimable_data/{user_address}")]
async fn get_distribution_data_by_address(
  pool: web::Data<DbPool>,
  web::Path(user_address): web::Path<String>,
) -> Result<HttpResponse, Error> {
  let distributions = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    db::get_unclaimed_distributions_by_address(&conn, &user_address)
  })
  .await.map_err(|e| {
    eprintln!("{}", e);
    HttpResponse::InternalServerError().finish()
  })?;

  Ok(HttpResponse::Ok().json(distributions))
}

/// Get claims history.
#[get("/claims")]
async fn get_claims(
  pagination: web::Query<PaginationInfo>,
  filter: web::Query<ClaimInfo>,
  pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
  let claims = web::block(move || {
    let conn = pool.get().expect("couldn't get db connection from pool");
    db::get_claims(&conn, filter.address.as_deref(), filter.distr_address.as_deref(), filter.epoch_number.as_ref(), pagination.per_page, pagination.page)
  })
  .await.map_err(|e| {
    eprintln!("{}", e);
    HttpResponse::InternalServerError().finish()
  })?;

  Ok(HttpResponse::Ok().json(claims))
}

fn var_enabled(var_str: &str) -> bool {
  let run = std::env::var(var_str).unwrap_or(String::from("false"));
  if run == "true" || run == "t" || run == "1" {
    return true
  }
  false
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  let env_path = std::env::var("ENV_FILE").unwrap_or(String::from("./.env"));
  dotenv::from_path(env_path).ok();
  env_logger::init_from_env(env_logger::Env::default().default_filter_or("zap_api=debug,actix_web=info")); // override with RUST_LOG env

  // set up database connection pool
  let connspec = std::env::var("DATABASE_URL").expect("DATABASE_URL env var missing.");
  let manager = ConnectionManager::<PgConnection>::new(connspec);
  let pool = r2d2::Pool::builder()
    .max_size(15)
    .build(manager)
    .expect("Failed to create db pool.");

  // set up redis connection
  let rconnspec = std::env::var("REDIS_URL").unwrap_or(String::from("redis://127.0.0.1/"));
  // let rmanager = redis::ConnectionManager::<PgConnection>::new(connspec);
  let redis = redis::Client::open(rconnspec).expect("Could not connect to redis");
  let mut con = redis.get_connection().expect("Failed to get redis connection");
  // throw away the result, just make sure it does not fail
  let _ : () = con.set("zap-api-redis:test", 42).expect("Failed to set value on redis");

  // get network
  let network_str = std::env::var("NETWORK").unwrap_or(String::from("testnet"));
  let network = match network_str.as_str() {
    "testnet" => Network::TestNet,
    "mainnet" => Network::MainNet,
    _ => panic!("Invalid network string")
  };

  // load config
  let config_file_path = std::env::var("CONFIG_FILE").unwrap_or(String::from("config/config.yml"));
  let f = std::fs::File::open(config_file_path)?;
  let data: serde_yaml::Value = serde_yaml::from_reader(f).expect("Could not read config.yml");
  let distr_configs = serde_yaml::from_value::<DistributionConfigs>(
    data[network.to_string()]["distributions"].clone()
  ).expect("Failed to parse distributions in config.yml");
  if let Err(e) = distr_configs.validate() {
    panic!("Error in config.yml: {:#?}", e);
  }

  // worker config
  let contract_hash = serde_yaml::from_value::<String>(data[network.to_string()]["zilswap_address_hex"].clone()).expect("invalid zilswap_address_hex");
  let distributor_contract_hashes = distr_configs.iter().map(|d| d.distributor_address()).collect();
  let worker_config = WorkerConfig::new(network, contract_hash.as_str(), distributor_contract_hashes);

  // get number of threads to run
  let threads_str = std::env::var("SERVER_THREADS").unwrap_or(String::from(""));

  // get conn pool
  let conn = pool.get().expect("couldn't get db connection from pool");

  // run migrations
  if var_enabled("RUN_MIGRATIONS") {
    info!("Running migrations..");
    embedded_migrations::run(&conn).expect("failed to run migrations.");
  }

  // run worker
  if var_enabled("RUN_WORKER") {
    info!("Running worker..");
    let _addr = worker::Coordinator::new(worker_config, pool.clone()).start();
  }

  let bind = std::env::var("BIND").or(Ok::<String, Error>(String::from("127.0.0.1:3000"))).unwrap();
  let mut server = HttpServer::new(move || {
    App::new()
      .wrap(Logger::default())
      .data(pool.clone())
      .data(distr_configs.clone())
      .data(redis.clone())
      .wrap(Cors::default()
        .max_age(Some(3600))
        .expose_any_header()
        .allow_any_header()
        .allow_any_method()
        .allow_any_origin()
        .send_wildcard())
      .service(hello)
      .service(generate_epoch)
      .service(get_claims)
      .service(get_distribution_info)
      .service(get_distribution_amounts)
      .service(get_distribution_data)
      .service(get_distribution_data_by_address)
      .service(get_swaps)
      .service(get_volume)
      .service(get_transactions)
      .service(get_liquidity_changes)
      .service(get_liquidity)
      .service(get_weighted_liquidity)
  });

  if let Ok(threads) = threads_str.parse::<usize>() {
    info!("Going to run server with {} threads..", threads);
    server = server.workers(threads);
  } else {
    info!("Going to run server with default threads..");
  }
  info!("Starting server at {}", &bind);

  server.bind(bind)?
    .run()
    .await
}
