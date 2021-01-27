
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::dsl::{sql, exists};
use diesel::sql_types::{Text, Numeric, Timestamp};
use chrono::{NaiveDateTime, Utc};

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

/// Get all pools by token address.
pub fn get_pools(
  conn: &PgConnection,
) -> Result<Vec<String>, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;

  let query = liquidity_changes
    .select(token_address)
    .distinct();

  Ok(query.load(conn)?)
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
      sql::<Text>("token_address AS pool"),
      sql::<Numeric>("SUM(change_amount) AS amount")
    ))
    .into_boxed::<Pg>();

  if let Some(address) = address {
    query = query.filter(initiator_address.eq(address));
  }

  if let Some(timestamp) = timestamp {
    query = query.filter(block_timestamp.le(NaiveDateTime::from_timestamp(timestamp, 0)))
  }

  Ok(query.load::<models::Liquidity>(conn)?)
}

/// Gets the swap volume for all pools over the given period in zil / token amounts.
pub fn get_volume(
  conn: &PgConnection,
  address: Option<&String>,
  start_timestamp: Option<i64>,
  end_timestamp: Option<i64>,
) -> Result<Vec<models::Volume>, diesel::result::Error> {
  use crate::schema::swaps::dsl::*;

  let mut query = swaps
    .group_by(token_address)
    .select((
      sql::<Text>("token_address AS pool"),
      // in/out wrt pool
      sql::<Numeric>("SUM(zil_amount * CAST(is_sending_zil AS integer)) AS in_zil_amount"),
      sql::<Numeric>("SUM(token_amount * CAST(is_sending_zil AS integer)) AS out_token_amount"),
      sql::<Numeric>("SUM(zil_amount * CAST(NOT(is_sending_zil) AS integer)) AS out_zil_amount"),
      sql::<Numeric>("SUM(token_amount * CAST(NOT(is_sending_zil) AS integer)) AS in_token_amount"),
    ))
    .into_boxed::<Pg>();

    if let Some(address) = address {
      query = query.filter(initiator_address.eq(address));
    }

    // filter start time, exclusive
    if let Some(start_timestamp) = start_timestamp {
      query = query.filter(block_timestamp.gt(NaiveDateTime::from_timestamp(start_timestamp, 0)))
    }

    // filter end time, inclusive
    if let Some(end_timestamp) = end_timestamp {
      query = query.filter(block_timestamp.le(NaiveDateTime::from_timestamp(end_timestamp, 0)))
    }

    Ok(query.load::<models::Volume>(conn)?)
}


/// Gets the swap volume for all pools over the given period in zil amounts by address.
pub fn get_volume_by_address(
  conn: &PgConnection,
  start_timestamp: Option<i64>,
  end_timestamp: Option<i64>,
) -> Result<Vec<models::VolumeForUser>, diesel::result::Error> {
  use crate::schema::swaps::dsl::*;

  let mut query = swaps
    .group_by((token_address, initiator_address))
    .select((
      sql::<Text>("initiator_address AS address"),
      sql::<Text>("token_address AS pool"),
      sql::<Numeric>("zil_amount AS amount"),
    ))
    .into_boxed::<Pg>();

    // filter start time, exclusive
    if let Some(start_timestamp) = start_timestamp {
      query = query.filter(block_timestamp.gt(NaiveDateTime::from_timestamp(start_timestamp, 0)))
    }

    // filter end time, inclusive
    if let Some(end_timestamp) = end_timestamp {
      query = query.filter(block_timestamp.le(NaiveDateTime::from_timestamp(end_timestamp, 0)))
    }

    Ok(query.load::<models::VolumeForUser>(conn)?)
}

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
    Some(start_timestamp) => NaiveDateTime::from_timestamp(start_timestamp, 0),
    None => NaiveDateTime::from_timestamp(0, 0),
  };

  let end_dt = match end_timestamp {
    Some(end_timestamp) => NaiveDateTime::from_timestamp(end_timestamp, 0),
    None => Utc::now().naive_utc(),
  };

  // local test query
  // "WITH t AS (
  //   SELECT
  //     token_address,
  //     change_amount AS change,
  //     block_timestamp AS start_timestamp,
  //     ROW_NUMBER() OVER w AS row_number,
  //     LEAD(block_timestamp, 1, NOW()::timestamp) OVER w AS end_timestamp,
  //     SUM(change_amount) OVER (PARTITION BY token_address ORDER BY block_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current
  //   FROM liquidity_changes
  //   WINDOW w AS (PARTITION BY token_address ORDER BY block_timestamp ASC)
  // ),
  // data AS (
  //   SELECT
  //     *,
  //     EXTRACT(EPOCH FROM (end_timestamp - start_timestamp)) / 3600 * current AS weighted_liquidity
  //   FROM t
  // )
  // SELECT
  //   token_address AS pool,
  //   CAST(SUM(data.weighted_liquidity) AS NUMERIC(38, 0)) AS amount
  // FROM data
  // WHERE (
  //   current > 0
  //   AND
  //   (token_address, row_number) IN (SELECT token_address, MAX(row_number) FROM data GROUP BY token_address)
  // )
  // GROUP BY token_address;"

  let sql = format!("
    WITH t AS (
      SELECT
        token_address,
        change_amount AS change,
        block_timestamp AS start_timestamp,
        ROW_NUMBER() OVER w AS row_number,
        LEAD(block_timestamp, 1, $2) OVER w AS end_timestamp,
        SUM(change_amount) OVER (PARTITION BY token_address ORDER BY block_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current
      FROM liquidity_changes
      WHERE block_timestamp <= $2
      {}
      WINDOW w AS (PARTITION BY token_address ORDER BY block_timestamp ASC)
    ),
    data AS (
      SELECT
        *,
        EXTRACT(EPOCH FROM (end_timestamp - GREATEST(start_timestamp, $1 + INTERVAL '1 second'))) / 3600 * current AS weighted_liquidity
      FROM t
    )
    SELECT
      token_address AS pool,
      CAST(SUM(data.weighted_liquidity) AS NUMERIC(38, 0)) AS amount
    FROM data
    WHERE start_timestamp > $1
    OR (
      current > 0
      AND
      (token_address, row_number) IN (SELECT token_address, MAX(row_number) FROM data WHERE start_timestamp <= $1 GROUP BY token_address)
    )
    GROUP BY token_address;
  ", address_fragment);

  let query = diesel::sql_query(sql)
    .bind::<Timestamp, _>(start_dt)
    .bind::<Timestamp, _>(end_dt)
    .bind::<Text, _>(address.unwrap_or(&noop));

  Ok(query.load::<models::Liquidity>(conn)?)
}

/// Get time-weighted liquidity for all pools over a period grouped by address.
pub fn get_time_weighted_liquidity_by_address(
  conn: &PgConnection,
  start_timestamp: Option<i64>,
  end_timestamp: Option<i64>,
) -> Result<Vec<models::LiquidityFromProvider>, diesel::result::Error> {
  let start_dt = match start_timestamp {
    Some(start_timestamp) => NaiveDateTime::from_timestamp(start_timestamp, 0),
    None => NaiveDateTime::from_timestamp(0, 0),
  };

  let end_dt = match end_timestamp {
    Some(end_timestamp) => NaiveDateTime::from_timestamp(end_timestamp, 0),
    None => Utc::now().naive_utc(),
  };

  let sql = "
    WITH t AS (
      SELECT
        token_address,
        initiator_address,
        change_amount AS change,
        block_timestamp AS start_timestamp,
        ROW_NUMBER() OVER w AS row_number,
        LEAD(block_timestamp, 1, $2) OVER w AS end_timestamp,
        SUM(change_amount) OVER (PARTITION BY (token_address, initiator_address) ORDER BY block_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current
      FROM liquidity_changes
      WHERE block_timestamp <= $2
      WINDOW w AS (PARTITION BY (token_address, initiator_address) ORDER BY block_timestamp ASC)
    ),
    data AS (
      SELECT
        *,
        EXTRACT(EPOCH FROM (end_timestamp - GREATEST(start_timestamp, $1 + INTERVAL '1 second'))) / 3600 * current AS weighted_liquidity
      FROM t
    )
    SELECT
      token_address AS pool,
      initiator_address AS address,
      CAST(SUM(data.weighted_liquidity) AS NUMERIC(38, 0)) AS amount
    FROM data
    WHERE start_timestamp > $1
    OR (
      current > 0
      AND
      (token_address, initiator_address, row_number) IN (SELECT token_address, initiator_address, MAX(row_number)
        FROM data WHERE start_timestamp <= $1 GROUP BY (token_address, initiator_address))
    )
    GROUP BY (token_address, initiator_address);
  ";

  let query = diesel::sql_query(sql)
    .bind::<Timestamp, _>(start_dt)
    .bind::<Timestamp, _>(end_dt);

  Ok(query.load::<models::LiquidityFromProvider>(conn)?)
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

  Ok(diesel::select(exists(swaps.filter(transaction_hash.eq(hash))))
    .get_result(conn)?)
}

pub fn liquidity_change_exists(
  conn: &PgConnection,
  hash: String,
) -> Result<bool, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;
  Ok(diesel::select(exists(liquidity_changes.filter(transaction_hash.eq(hash))))
    .get_result(conn)?)
}
