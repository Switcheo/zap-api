use bigdecimal::{BigDecimal, One};
use crate::models::{PoolReserves};

pub enum TradeDirection {
  ExactTokenForZil,
  TokenForExactZil,
  ExactZilForToken,
  ZilForExactToken,
  TokenForExactToken,
  ExactTokenForToken,
}

#[derive(Debug)]
pub struct LiquidityPool {
  token_address: String,
  zil_reserve: BigDecimal,
  token_reserve: BigDecimal,
  fee_rate: BigDecimal,
}

impl LiquidityPool {
  pub fn new(reserves: &PoolReserves) -> LiquidityPool {
    LiquidityPool {
      token_address: reserves.token_address.clone(),
      token_reserve: reserves.token_amount.clone(),
      zil_reserve: reserves.zil_amount.clone(),
      fee_rate: BigDecimal::from(0.003),
    }
  }

  fn get_epsilon(amount: &BigDecimal, n_reserve: &BigDecimal, d_reserve: &BigDecimal) -> BigDecimal {
    (amount * n_reserve / d_reserve).with_scale(0)
  }

  fn get_expected_output(&self, in_amount: &BigDecimal, in_reserve: &BigDecimal, out_reserve: &BigDecimal) -> BigDecimal {
    let input_after_fee = in_amount * (BigDecimal::one() - self.fee_rate.clone());
    let numerator = input_after_fee.clone() * out_reserve;
    let denominator = in_reserve + input_after_fee;
    (numerator / denominator).with_scale(0)
  }

  fn get_expected_input(&self, out_amount: &BigDecimal, in_reserve: &BigDecimal, out_reserve: &BigDecimal) -> BigDecimal {
    let numerator = in_reserve * out_amount;
    let denominator = (out_reserve - out_amount) * (BigDecimal::one() - self.fee_rate.clone());
    (numerator / denominator).with_scale(0) + BigDecimal::one()
  }

  fn compute_slippage(&self, diff: &BigDecimal, divisor: &BigDecimal) -> BigDecimal {
    ((divisor - diff) / divisor) - BigDecimal::from(0.3)
  }

  pub fn rate(&self, dir: TradeDirection, amount: &BigDecimal, out_pool: Option<LiquidityPool>) -> (BigDecimal, BigDecimal) {
    match dir {
      TradeDirection::ExactZilForToken => {
        // zil to zrc2
        let epsilon_output = LiquidityPool::get_epsilon(&amount, &self.token_reserve, &self.zil_reserve);
        let expected_output = self.get_expected_output(&amount, &self.zil_reserve, &self.token_reserve);
        let expected_slippage = self.compute_slippage(&expected_output, &epsilon_output);
        (expected_output, expected_slippage)
      },
      TradeDirection::ExactTokenForZil => {
        // zrc2 to zil
        let epsilon_output = LiquidityPool::get_epsilon(&amount, &self.zil_reserve, &self.token_reserve);
        let expected_output = self.get_expected_output(&amount, &self.token_reserve, &self.zil_reserve);
        let expected_slippage = self.compute_slippage(&expected_output, &epsilon_output);
        (expected_output, expected_slippage)
      },
      TradeDirection::ExactTokenForToken => {
        // zrc2 to zrc2
        let pool2 = out_pool.unwrap();
        let int_epsilon_output = LiquidityPool::get_epsilon(&amount, &self.zil_reserve, &self.token_reserve);
        let int_output = self.get_expected_output(&amount, &self.token_reserve, &self.zil_reserve);

        let epsilon_output = LiquidityPool::get_epsilon(&int_epsilon_output, &pool2.token_reserve, &self.zil_reserve);
        let expected_output = self.get_expected_output(&int_output, &self.zil_reserve, &self.token_reserve);
        let expected_slippage = self.compute_slippage(&expected_output, &epsilon_output);
        (expected_output, expected_slippage)
      }
      TradeDirection::ZilForExactToken => {
        // zil to zrc2
        let epsilon_input = LiquidityPool::get_epsilon(&amount, &self.token_reserve, &self.zil_reserve);
        let expected_input = self.get_expected_input(&amount, &self.zil_reserve, &self.token_reserve);
        let expected_slippage = self.compute_slippage(&epsilon_input, &expected_input);
        (expected_input, expected_slippage)
      },
      TradeDirection::TokenForExactZil => {
        // zrc2 to zil
        let epsilon_input = LiquidityPool::get_epsilon(&amount, &self.zil_reserve, &self.token_reserve);
        let expected_input = self.get_expected_input(&amount, &self.token_reserve, &self.zil_reserve);
        let expected_slippage = self.compute_slippage(&epsilon_input, &expected_input);
        (expected_input, expected_slippage)
      },
      TradeDirection::TokenForExactToken => {
        // zrc2 to zrc2
        let pool2 = out_pool.unwrap();
        let int_epsilon_output = LiquidityPool::get_epsilon(&amount, &self.zil_reserve, &self.token_reserve);
        let int_output = self.get_expected_input(&amount, &self.token_reserve, &self.zil_reserve);

        let epsilon_output = LiquidityPool::get_epsilon(&int_epsilon_output, &pool2.token_reserve, &self.zil_reserve);
        let expected_output = self.get_expected_output(&int_output, &self.zil_reserve, &self.token_reserve);
        let expected_slippage = self.compute_slippage(&expected_output, &epsilon_output);
        (expected_output, expected_slippage)
      }
    }
  }

  pub fn rate_exact_token_for_zil(&self, token_amount: BigDecimal) -> (BigDecimal, BigDecimal) {
    self.rate(TradeDirection::ExactTokenForZil, &token_amount, None)
  }

  pub fn rate_token_for_exact_zil(&self, zil_amount: BigDecimal) -> (BigDecimal, BigDecimal) {
    self.rate(TradeDirection::TokenForExactZil, &zil_amount, None)
  }

  pub fn rate_exact_zil_for_token(&self, zil_amount: BigDecimal) -> (BigDecimal, BigDecimal) {
    self.rate(TradeDirection::ExactZilForToken, &zil_amount, None)
  }

  pub fn rate_zil_for_exact_token(&self, token_amount: BigDecimal) -> (BigDecimal, BigDecimal) {
    self.rate(TradeDirection::ZilForExactToken, &token_amount, None)
  }

  pub fn rate_exact_token_for_token(&self, in_amount: BigDecimal, out_pool: LiquidityPool) -> (BigDecimal, BigDecimal) {
    self.rate(TradeDirection::TokenForExactToken, &in_amount, Some(out_pool))
  }

  pub fn rate_token_for_exact_token(&self, out_amount: BigDecimal, out_pool: LiquidityPool) -> (BigDecimal, BigDecimal) {
    self.rate(TradeDirection::ExactTokenForToken, &out_amount, Some(out_pool))
  }
}
