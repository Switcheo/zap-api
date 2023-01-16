use std::{fmt};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Event {
  Minted,
  Burnt,
  Swapped,
  Claimed,
}

impl Event {
  pub fn from_str(input: &str) -> Option<Event> {
    match input {
      "PoolMinted" => Some(Event::Minted),
      "PoolBurnt" => Some(Event::Burnt),
      "PoolSwapped" => Some(Event::Swapped),
      "Claimed" => Some(Event::Claimed),
      _ => None,
    }
  }
}

impl fmt::Display for Event {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Event::Minted => write!(f, "PoolMinted"),
      Event::Burnt => write!(f, "PoolBurnt"),
      Event::Swapped => write!(f, "PoolSwapped"),
      Event::Claimed => write!(f, "Claimed"),
    }
  }
}

#[derive(Clone)]
pub enum Network {
  MainNet,
  TestNet,
  LocalHost
}

impl fmt::Display for Network {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Network::MainNet => write!(f, "mainnet"),
      Network::TestNet => write!(f, "testnet"),
      Network::LocalHost => write!(f, "localhost"),
    }
  }
}

