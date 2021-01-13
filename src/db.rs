
use diesel::pg::Pg;
use diesel::prelude::*;

use crate::models;
use crate::pagination::*;

/// Run query using Diesel to find user by uid and return it.
pub fn fetch_swaps(
  conn: &PgConnection,
  per_page: Option<i64>,
  page: Option<i64>,
  pool: Option<&String>,
  address: Option<&String>,
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
  pool: Option<&String>,
  address: Option<&String>,
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

/// Get liquidity at a point in time filtered optionally by address.
pub fn get_liquidity(
  conn: &PgConnection,
  timestamp: Option<i64>,
  address: Option<&String>,
) -> Result<Vec<models::Liquidity>, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;

  let mut query = liquidity_changes
    .group_by(token_address)
    .select((
      diesel::dsl::sql::<diesel::sql_types::Text>("token_address AS pool"),
      diesel::dsl::sql::<diesel::sql_types::Numeric>("sum(change_amount) AS amount")
    ))
    .into_boxed::<Pg>();

  if let Some(address) = address {
    query = query.filter(initiator_address.eq(address));
  }

  if let Some(timestamp) = timestamp {
    query = query.filter(block_timestamp.le(chrono::NaiveDateTime::from_timestamp(timestamp, 0)))
  }

  Ok(query.load::<models::Liquidity>(conn)?)
}

/// Get volume in Zils.
/// pub fn get_swap_volume()

/// Get time-weighted liquidity for all pools over a period filtered optionally by address.
pub fn get_time_weighted_liquidity(
  conn: &PgConnection,
  start_timestamp: Option<i64>,
  end_timestamp: Option<i64>,
  address: Option<&String>,
) -> Result<Vec<models::Liquidity>, diesel::result::Error> {
  let address_fragment = match address {
    Some(_addr) => "AND initiator_address = $3", // bind later
    None => "AND '1' = $3", // bind to noop
  };
  let noop = String::from("1");

  let start_dt = match start_timestamp {
    Some(start_timestamp) => chrono::NaiveDateTime::from_timestamp(start_timestamp, 0),
    None => chrono::NaiveDateTime::from_timestamp(0, 0),
  };

  let end_dt = match end_timestamp {
    Some(end_timestamp) => chrono::NaiveDateTime::from_timestamp(end_timestamp, 0),
    None => chrono::Utc::now().naive_utc(),
  };

  let sql = format!("
    WITH t AS (
      SELECT change_amount, block_timestamp, token_address, row_number() OVER (PARTITION BY token_address ORDER BY block_timestamp ASC)
      FROM liquidity_changes
      WHERE block_timestamp > $1
      AND block_timestamp <= $2
      {}
    ),
    u AS (
      SELECT
        t.token_address AS token_address,
        t.block_timestamp AS end_timestamp,
        t2.block_timestamp AS start_timestamp,
        t2.change_amount AS change,
        t.row_number AS row_number,
        t2.row_number AS row_number_2,
        (t.block_timestamp - t2.block_timestamp) AS duration,
        SUM(t2.change_amount) OVER (PARTITION BY t.token_address ORDER BY t2.row_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current
      FROM t
      JOIN t t2 ON
        t2.row_number = t.row_number - 1 AND
        t2.token_address = t.token_address
      ORDER BY row_number
    ),
    data AS (
      SELECT
        *,
        EXTRACT(EPOCH FROM (end_timestamp - start_timestamp)) / 3600 * current AS weighted_liquidity
      FROM u
    )
    SELECT
      token_address AS pool,
      CAST(SUM(data.weighted_liquidity) AS NUMERIC(38, 0)) AS amount
    FROM data
    GROUP BY token_address;
  ", address_fragment);

  let query = diesel::sql_query(sql)
    .bind::<diesel::sql_types::Timestamp, _>(start_dt)
    .bind::<diesel::sql_types::Timestamp, _>(end_dt)
    .bind::<diesel::sql_types::Text, _>(address.unwrap_or(&noop));

  Ok(query.load::<models::Liquidity>(conn)?)
}

/// Get the liquidity over time of all pools
// let mut sql_for_graph = "
//   WITH t AS (
//     SELECT change_amount, block_timestamp, token_address, row_number() OVER (PARTITION BY token_address ORDER BY block_timestamp ASC)
//     FROM liquidity_changes
//   ),
//   u AS (
//     SELECT
//       t.token_address AS token_address,
//       t.block_timestamp AS end_timestamp,
//       t2.block_timestamp AS start_timestamp,
//       t2.change_amount AS change,
//       t.row_number AS row_number,
//       t2.row_number AS row_number_2,
//       (t.block_timestamp - t2.block_timestamp) AS duration,
//       SUM(t2.change_amount) OVER (PARTITION BY t.token_address ORDER BY t2.row_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current
//     FROM t
//     JOIN t t2 ON
//       t2.row_number = t.row_number - 1 AND
//       t2.token_address = t.token_address
//     ORDER BY row_number
//   ),
//   data AS (
//     SELECT
//       *,
//       EXTRACT(EPOCH FROM (end_timestamp - start_timestamp)) / 3600 * current AS weighted_liquidity
//     FROM u
//   )
//   SELECT
//     token_address,
//     start_timestamp,
//     end_timestamp,
//     duration,
//     change,
//     current,
//     SUM(data.weighted_liquidity) OVER (PARTITION BY data.token_address ORDER BY data.row_number_2 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cummulative_weighted_liquidity
//   FROM data
//   ORDER BY token_address ASC, start_timestamp ASC;
// ";

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

pub fn swap_exists(
  conn: &PgConnection,
  hash: String,
) -> Result<bool, diesel::result::Error> {
  use crate::schema::swaps::dsl::*;

  Ok(diesel::select(diesel::dsl::exists(swaps.filter(transaction_hash.eq(hash))))
    .get_result(conn)?)
}

pub fn liquidity_change_exists(
  conn: &PgConnection,
  hash: String,
) -> Result<bool, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;
  Ok(diesel::select(diesel::dsl::exists(liquidity_changes.filter(transaction_hash.eq(hash))))
    .get_result(conn)?)
}
