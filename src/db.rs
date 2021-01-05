
use diesel::prelude::*;

use crate::models;

/// Run query using Diesel to find user by uid and return it.
pub fn fetch_swaps(
  conn: &PgConnection,
) -> Result<Vec<models::Swap>, diesel::result::Error> {
  // It is common when using Diesel with Actix web to import schema-related
  // modules inside a function's scope (rather than the normal module's scope)
  // to prevent import collisions and namespace pollution.
  use crate::schema::swaps::dsl::*;

  Ok(swaps
    .limit(50)
    .load::<models::Swap>(conn)?
  )
}

/// Inserts a new swap into the db.
pub fn insert_swap(
  // prevent collision with `name` column imported inside the function
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
  // prevent collision with `name` column imported inside the function
  new_liquidity_change: models::NewLiquidityChange,
  conn: &PgConnection,
) -> Result<(), diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;

  diesel::insert_into(liquidity_changes)
    .values(&new_liquidity_change)
    .execute(conn)?;

  Ok(())
}
