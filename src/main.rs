

//! Diesel does not support tokio, so we have to run it in separate threads using the web::block
//! function which offloads blocking code (like Diesel's) in order to not block the server's thread.

#[macro_use]
extern crate diesel;

use actix::{Actor};
use actix_web::{get, web, App, Error, HttpResponse, HttpServer, Responder};
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use serde::Deserialize;

mod db;
mod models;
mod schema;
mod worker;
mod responses;
mod pagination;

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
    let swaps = web::block(move || db::fetch_swaps(&conn, query.per_page, query.page, &filter.pool, &filter.address))
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
  let liquidity_changes = web::block(move || db::fetch_liquidity_changes(&conn, query.per_page, query.page, &filter.pool, &filter.address))
      .await
      .map_err(|e| {
          eprintln!("{}", e);
          HttpResponse::InternalServerError().finish()
      })?;

  Ok(HttpResponse::Ok().json(liquidity_changes))
}

/// Get current liquidity.
///

// generate epoch

// get epoch

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  std::env::set_var("RUST_LOG", "actix_web=info");
  env_logger::init();
  dotenv::dotenv().ok();

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

  let bind = "127.0.0.1:3000";
  println!("Starting server at: {}", &bind);
  HttpServer::new(move || {
      App::new()
          .data(pool.clone())
          .service(hello)
          .service(get_swaps)
          .service(get_liquidity_changes)
  })
  .bind(bind)?
  .run()
  .await
}
