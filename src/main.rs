

//! Diesel does not support tokio, so we have to run it in separate threads using the web::block
//! function which offloads blocking code (like Diesel's) in order to not block the server's thread.

#[macro_use]
extern crate diesel;

use actix::{Actor, Addr, Arbiter, Context, System};
use actix_web::{get, web, App, Error, HttpResponse, HttpServer, Responder};
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};

mod db;
mod models;
mod schema;
mod worker;

type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;

/// Test endpoint.
#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello zap!")
}

/// Gets swaps.
#[get("/swaps")]
async fn get_swaps(
    pool: web::Data<DbPool>,
) -> Result<HttpResponse, Error> {
    let conn = pool.get().expect("couldn't get db connection from pool");

    // use web::block to offload blocking Diesel code without blocking server thread
    let swaps = web::block(move || db::fetch_swaps(&conn))
        .await
        .map_err(|e| {
            eprintln!("{}", e);
            HttpResponse::InternalServerError().finish()
        })?;

    Ok(HttpResponse::Ok().json(swaps))
}

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
  let _addr = worker::Worker.start();

  let bind = "127.0.0.1:3000";
  println!("Starting server at: {}", &bind);
  HttpServer::new(move || {
      App::new()
          .data(pool.clone())
          .service(hello)
          .service(get_swaps)
  })
  .bind(bind)?
  .run()
  .await
}
