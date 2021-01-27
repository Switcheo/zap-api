use bigdecimal::{BigDecimal, Signed, Zero};
use num_bigint::BigInt;

/// This method is only public in BigDecimal v2.0.0, but we can't use
/// it as diesel only supports v1.x.x.
pub fn round(bd: BigDecimal, round_digits: i64) -> BigDecimal {
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
  let digit = number % 10;
  if digit <= BigInt::from(4) {
      bd.with_scale(round_digits)
  } else if bigint.is_negative() {
      bd.with_scale(round_digits) - BigDecimal::new(BigInt::from(1), round_digits)
  } else {
      bd.with_scale(round_digits) + BigDecimal::new(BigInt::from(1), round_digits)
  }
}
