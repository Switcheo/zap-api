use bigdecimal::{BigDecimal, Zero};
use num_bigint::BigInt;

pub fn round_down(bd: BigDecimal, round_digits: i64) -> BigDecimal {
  let (bigint, decimal_part_digits) = bd.as_bigint_and_exponent();
  let need_to_round_digits = decimal_part_digits - round_digits;
  if round_digits >= 0 && need_to_round_digits <= 0 {
      return bd.clone();
  }
  let mut number = bigint.clone();
  if number < BigInt::zero() {
      number = -number;
  }
  for _ in 0..(need_to_round_digits - 1) {
      number /= 10;
  }
  bd.with_scale(round_digits)
}

#[derive(Debug)]
pub enum FetchError {
    // We will defer to the parse error implementation for their error.
    // Supplying extra info requires adding more data to the type.
    Fetch(reqwest::Error),
    Parse(serde_json::Error),
    Database(diesel::result::Error),
}

impl From<reqwest::Error> for FetchError {
  fn from(err: reqwest::Error) -> FetchError {
    FetchError::Fetch(err)
  }
}

impl From<serde_json::Error> for FetchError {
  fn from(err: serde_json::Error) -> FetchError {
    FetchError::Parse(err)
  }
}

impl From<diesel::result::Error> for FetchError {
  fn from(err: diesel::result::Error) -> FetchError {
    FetchError::Database(err)
  }
}
