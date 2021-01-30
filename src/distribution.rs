
use bech32::{decode, FromBase32};
use bigdecimal::{BigDecimal};
use hex::{encode};
use ring::{digest};
use serde::{Serialize};
use std::collections::HashMap;
use std::time::{SystemTime};
use trees::{Tree, TreeWalk, Node, walk::Visit};

use crate::constants::{zwap_emission};

#[derive(Serialize)]
pub struct EpochInfo {
  epoch_period: i64,
  tokens_per_epoch: u32,
  first_epoch_start: i64,
  next_epoch_start: i64,
  total_epoch: u32,
  current_epoch: i64,
}

impl EpochInfo {
  pub fn new(epoch_number: i64) -> EpochInfo {
    let first_epoch_start = zwap_emission::DISTRIBUTION_START_TIME;
    let epoch_period = zwap_emission::EPOCH_PERIOD;
    let total_epoch = zwap_emission::TOTAL_NUMBER_OF_EPOCH;
    let tokens_per_epoch = zwap_emission::TOKENS_PER_EPOCH;

    let next_epoch_start =
      if epoch_number == 0 {
        first_epoch_start
      } else {
        std::cmp::min(epoch_number + 1, total_epoch.into()) * epoch_period + first_epoch_start
      };

    EpochInfo {
      epoch_period,
      tokens_per_epoch,
      first_epoch_start,
      next_epoch_start,
      total_epoch,
      current_epoch: epoch_number,
    }
  }

  pub fn default() -> EpochInfo {
    let current_time = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .expect("invalid server time")
      .as_secs() as i64;

    let current_epoch = std::cmp::max(0, (current_time - zwap_emission::DISTRIBUTION_START_TIME) / zwap_emission::EPOCH_PERIOD);

    EpochInfo::new(current_epoch)
  }

  pub fn is_initial(&self) -> bool {
    self.current_epoch == 0
  }

  pub fn epoch_number(&self) -> i64 {
    self.current_epoch
  }

  pub fn current_epoch_start(&self) -> i64 {
    if self.is_initial() {
      0
    } else {
      std::cmp::min(self.current_epoch, zwap_emission::TOTAL_NUMBER_OF_EPOCH.into()) * zwap_emission::EPOCH_PERIOD + zwap_emission::DISTRIBUTION_START_TIME
    }
  }

  pub fn next_epoch_start(&self) -> i64 {
    self.next_epoch_start
  }

  pub fn tokens_for_epoch(&self) -> BigDecimal {
    if self.is_initial() {
      BigDecimal::from(50_000)* BigDecimal::from(10u64.pow(12))
    } else {
      BigDecimal::from(self.tokens_per_epoch)* BigDecimal::from(10u64.pow(12))
    }
  }

  pub fn tokens_for_developers(&self) -> BigDecimal {
    self.tokens_for_epoch() * BigDecimal::from(15) / BigDecimal::from(100)
  }

  pub fn tokens_for_users(&self) -> BigDecimal {
    self.tokens_for_epoch() - self.tokens_for_developers()
  }

  pub fn tokens_for_traders(&self) -> BigDecimal {
    if self.is_initial() {
      self.tokens_for_users() * BigDecimal::from(20) / BigDecimal::from(100)
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
  address_human: String,
  address: Vec::<u8>,
  amount: BigDecimal,
  hash: Vec::<u8>,
}

impl Distribution {
  pub fn new(address: String, amount: BigDecimal) -> Distribution {
    let (_hrp, data) = decode(address.as_str()).expect("Could not decode bech32 string!");
    let bytes = Vec::<u8>::from_base32(&data).unwrap();
    let hash = hash(&bytes, &amount);
    Distribution{address_human: address, address: bytes, amount, hash}
  }

  pub fn from(map: HashMap<String, BigDecimal>) -> Vec<Distribution> {
    let mut arr: Vec<Distribution> = vec![];
    for (k, v) in map.into_iter() {
      let d = Distribution::new(k, v);
      arr.push(d);
    }
    arr
  }

  pub fn address(&self) -> String {
    self.address_human.clone()
  }

  pub fn address_bytes(&self) -> Vec<u8> {
    self.address.clone()
  }

  pub fn amount(&self) -> BigDecimal {
    self.amount.clone()
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
