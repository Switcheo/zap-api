
use diesel::prelude::*;

use crate::models;

/// Run query using Diesel to find user by uid and return it.
pub fn fetch_swaps(
  conn: &PgConnection,
) -> Result<Vec<models::Swap>, diesel::result::Error> {
  use crate::schema::swaps::dsl::*;

  Ok(swaps
    .limit(50)
    .load::<models::Swap>(conn)
    .expect("Could not load swaps."))
}
