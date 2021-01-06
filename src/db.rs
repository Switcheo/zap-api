
use bigdecimal::{BigDecimal};
use diesel::pg::Pg;
use diesel::prelude::*;

use crate::models;
use crate::pagination::*;

/// Run query using Diesel to find user by uid and return it.
pub fn fetch_swaps(
  conn: &PgConnection,
  per_page: Option<i64>,
  page: Option<i64>,
  pool: &Option<String>,
  address: &Option<String>,
) -> Result<PaginatedResult<models::Swap>, diesel::result::Error> {
  // It is common when using Diesel with Actix web to import schema-related
  // modules inside a function's scope (rather than the normal module's scope)
  // to prevent import collisions and namespace pollution.
  use crate::schema::swaps::dsl::*;

  let mut query = swaps.into_boxed::<Pg>();

  if let Some(pool) = pool {
    query = query.filter(token_address.eq(pool));
  }

  if let Some(address) = address {
    query = query.filter(initiator_address.eq(address));
  }

  Ok(query
    .order(block_timestamp.desc())
    .paginate(page)
    .per_page(per_page)
    .load_and_count_pages::<models::Swap>(conn)?)
}

/// Run query using Diesel to find user by uid and return it.
pub fn fetch_liquidity_changes(
  conn: &PgConnection,
  per_page: Option<i64>,
  page: Option<i64>,
  pool: &Option<String>,
  address: &Option<String>,
) -> Result<PaginatedResult<models::LiquidityChange>, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;

  let mut query = liquidity_changes.into_boxed::<Pg>();

  if let Some(pool) = pool {
    query = query.filter(token_address.eq(pool));
  }

  if let Some(address) = address {
    query = query.filter(initiator_address.eq(address));
  }

  Ok(query
    .order(block_timestamp.desc())
    .paginate(page)
    .per_page(per_page)
    .load_and_count_pages::<models::LiquidityChange>(conn)?
  )
}

/// Get liquidity for period
pub fn get_liquidity_for_period(
  conn: &PgConnection,
  pool: &Option<String>,
  start_timestamp: i64,
  end_timestamp: i64,
) -> Result<BigDecimal, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;

  let mut query = liquidity_changes.into_boxed::<Pg>();

  if let Some(pool) = pool {
    query = query.filter(token_address.eq(pool));
  }

  let res = query
    .select(diesel::dsl::sum(change_amount))
    .filter(block_timestamp.gt(chrono::NaiveDateTime::from_timestamp(start_timestamp, 0)))
    .filter(block_timestamp.lt(chrono::NaiveDateTime::from_timestamp(end_timestamp, 0)))
    .first(conn)?;

  match res {
    Some(num) => Ok(num),
    None => panic!("Null fetched for sum!")
  }
}

/// Inserts a new swap into the db.
pub fn insert_swap(
  new_swap: models::NewSwap,
  conn: &PgConnection,
) -> Result<(), diesel::result::Error> {
  use crate::schema::swaps::dsl::*;

  diesel::insert_into(swaps)
    .values(&new_swap)
    .execute(conn)?;

  Ok(())
}

/// Inserts a new liquidity change into the db.
pub fn insert_liquidity_change(
  new_liquidity_change: models::NewLiquidityChange,
  conn: &PgConnection,
) -> Result<(), diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;

  diesel::insert_into(liquidity_changes)
    .values(&new_liquidity_change)
    .execute(conn)?;

  Ok(())
}
