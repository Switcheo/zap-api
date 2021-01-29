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
