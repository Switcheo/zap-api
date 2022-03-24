use diesel::debug_query;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::dsl::{sql, exists};
use diesel::sql_types::{Text, Numeric, Timestamp};
use chrono::{NaiveDateTime, Utc};
use redis::Commands;

use crate::models;
use crate::pagination::*;

/// Get paginated swaps.
pub fn get_swaps(
  conn: &PgConnection,
  per_page: Option<i64>,
  page: Option<i64>,
  pool: Option<&str>,
  address: Option<&str>,
  is_incoming: Option<&bool>,
) -> Result<PaginatedResult<models::Swap>, diesel::result::Error> {
  // It is common when using Diesel with Actix web to import schema-related
  // modules inside a function's scope (rather than the normal module's scope)
  // to prevent import collisions and namespace pollution.
  use crate::schema::swaps::dsl::*;

  let mut query = swaps.into_boxed::<Pg>();

  if let Some(pool) = pool {
    let pools = pool.split(",");
    for p in pools {
      query = query.or_filter(token_address.eq(p));
    }
  }

  if let Some(address) = address {
    query = query.filter(initiator_address.eq(address));
  }

  if let Some(is_incoming) = is_incoming {
    query = query.filter(is_sending_zil.eq(is_incoming))
  }

  Ok(query
    .order(block_timestamp.desc())
    .paginate(page)
    .per_page(per_page)
    .load_and_count_pages::<models::Swap>(conn)?)
}

/// Get paginated liquidity changes.
pub fn get_liquidity_changes(
  conn: &PgConnection,
  per_page: Option<i64>,
  page: Option<i64>,
  pool: Option<&str>,
  address: Option<&str>,
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

/// Get distributions by epoch, optionally filtered by address.
pub fn get_distributions(
  conn: &PgConnection,
  distr_address: Option<&str>,
  epoch: Option<i32>,
  address: Option<&str>,
) -> Result<Vec<models::Distribution>, diesel::result::Error> {
  use crate::schema::distributions::dsl::*;

  let mut query = distributions.into_boxed::<Pg>();

  if let Some(epoch) = epoch {
    query = query.filter(epoch_number.eq(epoch));
  }

  if let Some(address) = address {
    query = query.filter(address_bech32.eq(address));
  }

  if let Some(distr_address) = distr_address {
    query = query.filter(distributor_address.eq(distr_address));
  }

  Ok(query
    .order(address_bech32.asc())
    .load::<models::Distribution>(conn)?
  )
}

/// Get all distributions for an address.
pub fn get_distributions_by_address(
  conn: &PgConnection,
  address: &str,
) -> Result<Vec<models::Distribution>, diesel::result::Error> {
  use crate::schema::distributions::dsl::*;

  let query = distributions
    .order(epoch_number.asc())
    .filter(address_bech32.eq(address));

  Ok(query.load(conn)?)
}

/// Get a single claim by address, distributor address and epoch number
pub fn get_claim(
  conn: &PgConnection,
  address: &str,
  distr_address: &str,
  epoch: &i32,
) -> Result<Option<models::Claim>, diesel::result::Error> {
  use crate::schema::claims::dsl::*;

  Ok(claims
    .filter(initiator_address.eq(address))
    .filter(distributor_address.eq(distr_address))
    .filter(epoch_number.eq(epoch))
    .first(conn)
    .optional()
    .unwrap())
}

/// Get all claims, optionally filtered by address and/or distributor address
pub fn get_claims(
  conn: &PgConnection,
  address: Option<&str>,
  distr_address: Option<&str>,
  epoch: Option<&i32>,
  per_page: Option<i64>,
  page: Option<i64>,
) -> Result<PaginatedResult<models::Claim>, diesel::result::Error> {
  use crate::schema::claims::dsl::*;

  let mut query = claims.into_boxed::<Pg>();

  if let Some(address) = address {
    query = query.filter(initiator_address.eq(address));
  }

  if let Some(distr_address) = distr_address {
    query = query.filter(distributor_address.eq(distr_address));
  }

  if let Some(epoch) = epoch {
    query = query.filter(epoch_number.eq(epoch));
  }

  Ok(query
    .order(epoch_number.asc())
    .paginate(page)
    .per_page(per_page)
    .load_and_count_pages::<models::Claim>(conn)?
  )
}

/// Get unclaimed distributions for an address.
pub fn get_unclaimed_distributions_by_address(
  conn: &PgConnection,
  address: &str,
) -> Result<Vec<models::Distribution>, diesel::result::Error> {
  let sql = "
    SELECT d.id, d.distributor_address, d.epoch_number,
    d.address_bech32, d.address_hex, d.amount, d.proof
    FROM distributions d
    LEFT OUTER JOIN claims c
    ON d.distributor_address = c.distributor_address
    AND d.epoch_number = c.epoch_number
    AND d.address_bech32 = c.initiator_address
    WHERE address_bech32 = $1
    AND c.id IS NULL
  ";

  let query = diesel::sql_query(sql)
    .bind::<Text, _>(address);

  Ok(query.load::<models::Distribution>(conn)?)
}

/// Get all pools.
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
  address: Option<&str>,
) -> Result<Vec<models::Liquidity>, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;

  let mut query = liquidity_changes
    .group_by(token_address)
    .select((
      sql::<Text>("token_address AS pool"),
      sql::<Numeric>("SUM(change_amount) AS amount"),
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
  address: Option<&str>,
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

    // filter start time, inclusive
    if let Some(start_timestamp) = start_timestamp {
      query = query.filter(block_timestamp.ge(NaiveDateTime::from_timestamp(start_timestamp, 0)))
    }

    // filter end time, exclusive
    if let Some(end_timestamp) = end_timestamp {
      query = query.filter(block_timestamp.lt(NaiveDateTime::from_timestamp(end_timestamp, 0)))
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
      sql::<Text>("token_address AS pool"),
      sql::<Text>("initiator_address AS address"),
      sql::<Numeric>("SUM(zil_amount) AS amount"),
    ))
    .into_boxed::<Pg>();

    // filter start time, inclusive
    if let Some(start_timestamp) = start_timestamp {
      query = query.filter(block_timestamp.ge(NaiveDateTime::from_timestamp(start_timestamp, 0)))
    }

    // filter end time, exclusive
    if let Some(end_timestamp) = end_timestamp {
      query = query.filter(block_timestamp.lt(NaiveDateTime::from_timestamp(end_timestamp, 0)))
    }

    Ok(query.load::<models::VolumeForUser>(conn)?)
}

/// Get time-weighted liquidity for all pools over a period filtered optionally by address.
pub fn get_time_weighted_liquidity(
  conn: &PgConnection,
  cache: &mut redis::Connection,
  start_timestamp: Option<i64>,
  end_timestamp: Option<i64>,
  address: Option<&str>,
) -> Result<Vec<models::Liquidity>, diesel::result::Error> {
  let address_fragment = match address {
    Some(_addr) => "AND initiator_address = $3", // bind later
    None => "AND '1' = $3", // bind to noop
  };
  let noop = "1";

  let start_dt = match start_timestamp {
    Some(start_timestamp) => NaiveDateTime::from_timestamp(start_timestamp, 0),
    None => NaiveDateTime::from_timestamp(0, 0),
  };

  let end_dt = match end_timestamp {
    Some(end_timestamp) => NaiveDateTime::from_timestamp(end_timestamp, 0),
    None => Utc::now().naive_utc(),
  };

  let network = std::env::var("NETWORK").unwrap_or(String::from("testnet"));
  let cache_key = format!("zap-api-cache:{}:get_time_weighted_liquidity:{}:{}:{}", network, start_timestamp.unwrap_or(0).to_string(), end_timestamp.unwrap_or(0).to_string(), address.unwrap_or(""));
  let cache_value: Option<String> = cache.get(cache_key.clone()).unwrap_or(None);
  match cache_value {
    Some (serialized) => {
      match serde_json::from_str::<Vec<models::Liquidity>>(&serialized) {
        Ok(result) => return Ok(result),
        _ => {}
      }
    }
    _ => {}
  }

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
        SUM(change_amount) OVER (PARTITION BY token_address ORDER BY block_timestamp ASC, transaction_hash ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current
      FROM liquidity_changes
      WHERE block_timestamp < $2
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
    WHERE start_timestamp >= $1
    OR (
      current > 0
      AND
      (token_address, row_number) IN (SELECT token_address, MAX(row_number) FROM data WHERE start_timestamp < $1 GROUP BY token_address)
    )
    GROUP BY token_address;
  ", address_fragment);

  let query = diesel::sql_query(sql)
    .bind::<Timestamp, _>(start_dt)
    .bind::<Timestamp, _>(end_dt)
    .bind::<Text, _>(address.unwrap_or(&noop));

  trace!("{}", debug_query(&query).to_string());

  let result = query.load::<models::Liquidity>(conn)?;

  let cache_value: String = serde_json::to_string(&result).expect("failed to serialize result to cache");
  let _ = cache.set_ex::<String, String, ()>(cache_key, cache_value, 60).unwrap_or_else(|e| { // 1min cache
    error!("{}", e)
  });

  Ok(result)
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
        SUM(change_amount) OVER (PARTITION BY (token_address, initiator_address) ORDER BY block_timestamp ASC, transaction_hash ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current
      FROM liquidity_changes
      WHERE block_timestamp < $2
      WINDOW w AS (PARTITION BY (token_address, initiator_address) ORDER BY block_timestamp ASC)
    ),
    data AS (
      SELECT
        *,
        (EXTRACT(EPOCH FROM (end_timestamp - GREATEST(start_timestamp, $1 + INTERVAL '1 second'))) - 1) / 3600 * current AS weighted_liquidity
      FROM t
    )
    SELECT
      token_address AS pool,
      initiator_address AS address,
      CAST(SUM(data.weighted_liquidity) AS NUMERIC(38, 0)) AS amount
    FROM data
    WHERE start_timestamp >= $1
    OR (
      current > 0
      AND
      (token_address, initiator_address, row_number) IN (SELECT token_address, initiator_address, MAX(row_number)
        FROM data WHERE start_timestamp < $1 GROUP BY (token_address, initiator_address))
    )
    GROUP BY (token_address, initiator_address);
  ";

  let query = diesel::sql_query(sql)
    .bind::<Timestamp, _>(start_dt)
    .bind::<Timestamp, _>(end_dt);

  trace!("{}", debug_query(&query).to_string());

  Ok(query.load::<models::LiquidityFromProvider>(conn)?)
}

/// List LP transactions
pub fn get_transactions(
  conn: &PgConnection,
  address: Option<&str>,
  pool: Option<&str>,
  start_timestamp: Option<i64>,
  end_timestamp: Option<i64>,
  per_page: Option<i64>,
  page: Option<i64>,
) -> Result<PaginatedResult<models::PoolTx>, diesel::result::Error> {
  use crate::schema::pool_txs::dsl::*;

  let mut query = pool_txs.into_boxed::<Pg>();

  if let Some(pool) = pool {
    let pools = pool.split(",");
    for p in pools {
      query = query.or_filter(token_address.eq(p));
    }
  }

  if let Some(address) = address {
    query = query.filter(initiator_address.eq(address));
  }

  // filter start time, inclusive
  if let Some(start_timestamp) = start_timestamp {
    query = query.filter(block_timestamp.ge(NaiveDateTime::from_timestamp(start_timestamp, 0)))
  }

  // filter end time, exclusive
  if let Some(end_timestamp) = end_timestamp {
    query = query.filter(block_timestamp.lt(NaiveDateTime::from_timestamp(end_timestamp, 0)))
  }

  Ok(query
    .order(block_timestamp.desc())
    .paginate(page)
    .per_page(per_page)
    .load_and_count_pages::<models::PoolTx>(conn)?)
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

/// Inserts multiple distributions into the db.
pub fn insert_distributions(
  new_distribution: Vec<models::NewDistribution>,
  conn: &PgConnection,
) -> Result<(), diesel::result::Error> {
  use crate::schema::distributions::dsl::*;

  diesel::insert_into(distributions)
    .values(&new_distribution)
    .execute(conn)?;

  Ok(())
}

/// Inserts a new claim into the db.
pub fn insert_claim(
  new_claim: models::NewClaim,
  conn: &PgConnection,
) -> Result<(), diesel::result::Error> {
  use crate::schema::claims::dsl::*;

  diesel::insert_into(claims)
    .values(&new_claim)
    .execute(conn)?;

  Ok(())
}

/// Inserts a backfill completion into the db ignoring duplicates.
pub fn insert_backfill_completion(
  new_backfill_completion: models::NewBackfillCompletion,
  conn: &PgConnection,
) -> Result<(), diesel::result::Error> {
  use crate::schema::backfill_completions::dsl::*;

  let res = diesel::insert_into(backfill_completions)
    .values(&new_backfill_completion)
    .execute(conn);

  if let Err(e) = res {
    match e {
      diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) =>
        debug!("Ignoring duplicate backfill_completion entry"),
      _ => return Err(e)
    }
  }

  Ok(())
}

pub fn swap_exists(
  conn: &PgConnection,
  hash: &str,
) -> Result<bool, diesel::result::Error> {
  use crate::schema::swaps::dsl::*;

  Ok(diesel::select(exists(swaps.filter(transaction_hash.eq(hash))))
    .get_result(conn)?)
}

pub fn liquidity_change_exists(
  conn: &PgConnection,
  hash: &str,
) -> Result<bool, diesel::result::Error> {
  use crate::schema::liquidity_changes::dsl::*;
  Ok(diesel::select(exists(liquidity_changes.filter(transaction_hash.eq(hash))))
    .get_result(conn)?)
}

pub fn epoch_exists(
  conn: &PgConnection,
  distr_address: &str,
  epoch: &i32,
) -> Result<bool, diesel::result::Error> {
  use crate::schema::distributions::dsl::*;

  Ok(diesel::select(exists(distributions.filter(epoch_number.eq(epoch)).filter(distributor_address.eq(distr_address))))
    .get_result(conn)?)
}

pub fn backfill_completed(
  conn: &PgConnection,
  address: &str,
  event: &str,
) -> Result<bool, diesel::result::Error> {
  use crate::schema::backfill_completions::dsl::*;

  Ok(diesel::select(exists(backfill_completions.filter(contract_address.eq(address)).filter(event_name.eq(event))))
    .get_result(conn)?)
}
