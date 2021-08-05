use bech32::{decode, FromBase32};
use bigdecimal::{BigDecimal, Zero};
use hex::{encode};
use ring::{digest};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::convert::{TryInto};
use std::time::{SystemTime};
use std::str::{FromStr};
use trees::{Tree, TreeWalk, Node, walk::Visit};

#[derive(Debug, Clone)]
pub struct InvalidConfigError {
  details: String
}

pub trait Validate {
  fn validate(&self) -> Result<(), InvalidConfigError>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmissionConfig {
  epoch_period: i64,
  tokens_per_epoch: String,
  tokens_for_retroactive_distribution: String,
  retroactive_distribution_cutoff_time: i64,
  distribution_start_time: i64,
  total_number_of_epochs: u32,
  initial_epoch_number: u32,
  developer_token_ratio_bps: u16,
  trader_token_ratio_bps: u16,
}

impl Validate for EmissionConfig {
  fn validate(&self) -> Result<(), InvalidConfigError> {
    let mut errs = vec![];
    if self.retroactive_distribution_cutoff_time > 0 && self.initial_epoch_number < 1 {
      errs.push("initial_epoch_number must be more than 0")
    }
    if self.total_number_of_epochs == 0 {
      errs.push("total_number_of_epochs must be more than 0")
    }
    match BigDecimal::from_str(self.tokens_for_retroactive_distribution.as_str()) {
      Ok(r) => {
        if self.retroactive_distribution_cutoff_time > 0 && r.is_zero() {
          errs.push("tokens_for_retroactive_distribution must be more than 0")
        }
      }
      Err(_) => errs.push("tokens_for_retroactive_distribution is invalid")
    }
    match BigDecimal::from_str(self.tokens_per_epoch.as_str()) {
      Ok(r) => {
        if r.is_zero() {
          errs.push("tokens_per_epoch must be more than 0")
        }
      }
      Err(_) => errs.push("tokens_per_epoch is invalid")
    }
    if errs.len() > 0 {
      Err(InvalidConfigError{details: errs.join("\n")})
    } else {
      Ok(())
    }
  }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DistributionConfig {
  name: String,
  reward_token: String,
  distributor_name: String,
  distributor_address_hex: String,
  developer_address: String,
  emission_info: EmissionConfig,
  incentived_pools: HashMap<String, u32>,
}

impl DistributionConfig {
  pub fn emission(&self) -> EmissionConfig {
    self.emission_info.clone()
  }

  pub fn developer_address(&self) -> &str {
    self.developer_address.as_str()
  }

  pub fn distributor_address(&self) -> &str {
    self.distributor_address_hex.as_str()
  }

  pub fn incentived_pools(&self) -> HashMap<String, u32> {
    self.incentived_pools.clone()
  }
}

pub type DistributionConfigs = Vec<DistributionConfig>;
impl Validate for DistributionConfigs {
  fn validate(&self) -> Result<(), InvalidConfigError> {
    if self.len() == 0 {
      return Err(InvalidConfigError{details: "No distributions found".to_owned()})
    }
    for d in self {
      if let Err(e) = d.emission_info.validate() {
        return Err(InvalidConfigError{details: format!("Distribution for '{}' is invalid: {:?}", d.name, e)})
      }
    }
    Ok(())
  }
}

#[derive(Serialize)]
pub struct EpochInfo {
  emission_info: EmissionConfig,
  retroactive_distribution_epoch_number: Option<u32>,
  first_epoch_number: u32,
  first_epoch_start: i64,
  last_epoch_number: u32,
  current_epoch_number: u32,
  current_epoch_start: Option<i64>,
  current_epoch_end: Option<i64>,
  tokens_for_epoch: BigDecimal,
  next_epoch_start: Option<i64>,
}

impl EpochInfo {
  pub fn new(emission: EmissionConfig, epoch_number: Option<u32>) -> EpochInfo {
    let current_epoch_number = match epoch_number {
      Some(n) => n,
      None => {
        let current_time = SystemTime::now()
          .duration_since(SystemTime::UNIX_EPOCH)
          .expect("invalid server time")
          .as_secs() as i64;
        let epochs_after_start = (current_time - emission.distribution_start_time) as f64 / emission.epoch_period as f64;
        std::cmp::max(0, epochs_after_start.ceil() as u32 + emission.initial_epoch_number - 1)
      }
    };

    let retroactive_distribution_epoch_number =
      if emission.retroactive_distribution_cutoff_time > 0 {
        Some(emission.initial_epoch_number - 1)
      } else {
        None
      };

    let first_epoch_number = emission.initial_epoch_number;
    let last_epoch_number = emission.total_number_of_epochs + emission.initial_epoch_number - 1;

    let first_epoch_start = emission.distribution_start_time;

    let (current_epoch_start, current_epoch_end) =
      if current_epoch_number < emission.initial_epoch_number {
        (Some(0), Some(emission.retroactive_distribution_cutoff_time))
      } else if current_epoch_number <= last_epoch_number {
        let start = i64::from(current_epoch_number - emission.initial_epoch_number) * emission.epoch_period + first_epoch_start;
        (Some(start), Some(start + emission.epoch_period))
      } else {
        (None, None)
      };

    let next_epoch_start =
      if current_epoch_number < emission.initial_epoch_number {
        Some(first_epoch_start)
      } else if current_epoch_number < last_epoch_number {
        current_epoch_end
      } else {
        None
      };

    let tokens_for_epoch =
      if current_epoch_number < emission.initial_epoch_number {
        BigDecimal::from_str(emission.tokens_for_retroactive_distribution.as_str()).unwrap()
      } else if current_epoch_number <= last_epoch_number {
        BigDecimal::from_str(emission.tokens_per_epoch.as_str()).unwrap()
      } else {
        BigDecimal::from(0)
      };

    Self {
      emission_info: emission,
      retroactive_distribution_epoch_number,
      first_epoch_number,
      first_epoch_start,
      last_epoch_number,
      current_epoch_number,
      current_epoch_start,
      current_epoch_end,
      tokens_for_epoch,
      next_epoch_start,
    }
  }

  pub fn is_initial(&self) -> bool {
    self.current_epoch_number < self.first_epoch_number
  }

  pub fn epoch_number(&self) -> i32 {
    self.current_epoch_number.try_into().unwrap()
  }

  pub fn current_epoch_start(&self) -> Option<i64> {
    self.current_epoch_start
  }

  pub fn current_epoch_end(&self) -> Option<i64> {
    self.current_epoch_end
  }

  pub fn distribution_ended(&self) -> bool {
    self.current_epoch_number > self.last_epoch_number
  }

  pub fn tokens_for_epoch(&self) -> BigDecimal {
    self.tokens_for_epoch.clone()
  }

  pub fn tokens_for_developers(&self) -> BigDecimal {
    self.tokens_for_epoch() * BigDecimal::from(self.emission_info.developer_token_ratio_bps) / BigDecimal::from(10000)
  }

  pub fn tokens_for_users(&self) -> BigDecimal {
    self.tokens_for_epoch() - self.tokens_for_developers()
  }

  pub fn tokens_for_traders(&self) -> BigDecimal {
    if self.is_initial() {
      self.tokens_for_users() * BigDecimal::from(self.emission_info.trader_token_ratio_bps) / BigDecimal::from(10000)
    } else {
      BigDecimal::default()
    }
  }

  pub fn tokens_for_liquidity_providers(&self) -> BigDecimal {
    self.tokens_for_users() - self.tokens_for_traders()
  }
}

#[derive(Serialize, Clone)]
pub struct Distribution {
  address: Vec::<u8>,
  address_hex: String,
  address_human: String,
  amount: BigDecimal,
  hash: Vec::<u8>,
}

impl Distribution {
  pub fn new(address: String, amount: BigDecimal) -> Distribution {
    let (_hrp, data) = decode(address.as_str()).expect("Could not decode bech32 string!");
    let bytes = Vec::<u8>::from_base32(&data).unwrap();
    let hash = hash(&bytes, &amount);
    let hex = encode(&bytes);
    Distribution{address_human: address, address_hex: hex, address: bytes, amount, hash}
  }

  pub fn from(map: HashMap<String, BigDecimal>) -> Vec<Distribution> {
    let mut arr: Vec<Distribution> = vec![];
    for (k, v) in map.into_iter() {
      let d = Distribution::new(k, v);
      arr.push(d);
    }
    arr
  }

  pub fn address_bech32(&self) -> &str {
    self.address_human.as_str()
  }

  pub fn address_hex(&self) -> &str {
    self.address_hex.as_str()
  }

  pub fn amount(&self) -> &BigDecimal {
    &self.amount
  }

  pub fn hash(&self) -> Vec::<u8> {
    self.hash.clone()
  }
}

fn hash(address: &Vec::<u8>, amount: &BigDecimal) -> Vec<u8> {
  // convert the amount to big-endian bytes
  let (big, exp) = amount.as_bigint_and_exponent();
  if exp != 0 {
    panic!("Non-integer distribution amount received!");
  }
  let (_sign, bytes) = big.to_bytes_be();
  let zeroes = vec![0; 16 - bytes.len()];
  let amount_bytes = [zeroes, bytes].concat();
  // println!("amount_bytes: {:?}", amount_bytes);

  // hash the amount bytes
  let digest = digest::digest(&digest::SHA256, &amount_bytes);
  // println!("digest: {:?}", digest);
  let amount_hash = digest.as_ref();

  // concat 20 address bytes to the 32 bytes amount hash
  let value_to_hash = [address.to_vec(), amount_hash.to_vec()].concat();

  // debug: hash the concatted value
  let final_hash = digest::digest(&digest::SHA256, &value_to_hash);
  // println!("value to hash: {}", encode(value_to_hash.to_vec()));
  // println!("final hash: {}", encode(final_hash.as_ref().to_vec()));

  final_hash.as_ref().to_vec()
}

type Data = (Option<Distribution>, Vec<u8>);
type MerkleTree = Tree<Data>;

pub fn construct_merkle_tree(data: Vec<Distribution>) -> MerkleTree {
  // println!("Build tree:");
  let mut leaves: Vec<MerkleTree> = vec![];
  for d in data.into_iter() {
    let hash = d.hash.clone();
    leaves.push(MerkleTree::new((Some(d), hash)));
  }
  build_parents(leaves)
}

fn build_parents(mut input: Vec<MerkleTree>) -> MerkleTree {
  // println!("Build parents:");
  input.sort_by_key(|c| c.data().1.clone()); // sort by hash
  let mut children = std::collections::VecDeque::from(input);
  let mut nodes: Vec<MerkleTree> = vec![];
  loop {
    let c = children.pop_front();
    match c {
      Some(c1) => {
        let maybe_c2 = children.pop_front();
        match maybe_c2 {
          Some(c2) => {
            // println!("Joining:\n{:?}\n{:?}", encode(c1.data().1.clone()), encode(c2.data().1.clone()));
            let concat = [c1.data().1.clone(), c2.data().1.clone()].concat();
            let hash = digest::digest(&digest::SHA256, &concat);
            // println!("Hash:\n{:?}", encode(hash.as_ref().to_vec()));
            let mut parent = MerkleTree::new((None, hash.as_ref().to_vec()));
            parent.push_back(c1);
            parent.push_back(c2);
            nodes.push(parent);
          }
          None => {
            // println!("Orphan:\n{:?}", encode(c1.data().1.clone()));
            nodes.push(c1)
          }
        }
      }
      None => {
        if nodes.len() == 1 {
          return nodes[0].clone()
        }
        return build_parents(nodes)
      }
    }
  }
}

pub fn get_proofs(tree: MerkleTree) -> Vec<(Distribution, String)> {
  let mut res: Vec<(Distribution, String)> = vec![];
  let mut walk = TreeWalk::from(tree);
  loop {
    let node = walk.next();
    match node {
      Some(Visit::Leaf(leaf)) => res.push((leaf.data().0.clone().unwrap(), get_proof(&leaf))),
      None => return res,
      _ => (),
    }
  }
}

fn get_proof(leaf: &Node<Data>) -> String {
  let mut res = String::new();
  let mut needle = leaf;
  // push node hash
  res.push_str(encode(leaf.data().1.clone()).as_str());
  loop {
    if let Some(parent) = needle.parent() {
      // find sibling
      let mut sibling = parent.front().unwrap();
      if sibling.data().1 == needle.data().1 {
        sibling = parent.back().unwrap();
      }
      // push sibling hash
      res.push_str(" ");
      res.push_str(encode(sibling.data().1.clone()).as_str());
      needle = parent
    } else { // no parent, we are at the root
      // push root hash
      res.push_str(" ");
      res.push_str(encode(needle.data().1.clone()).as_str());
      break
    }
  }
  res
}
