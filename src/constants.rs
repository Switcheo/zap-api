
use std::{fmt};

pub mod zap_epoch {
  pub static EPOCH_START_TIME: i64 = 1612339200;
  pub static EPOCH_PERIOD: i64 = 604800; // one week
  pub static MAX_EPOCH: u32 = 152;
}

pub enum Event {
  Minted,
  Burnt,
  Swapped,
}

impl fmt::Display for Event {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Event::Minted => write!(f, "Mint"),
      Event::Burnt => write!(f, "Burnt"),
      Event::Swapped => write!(f, "Swapped"),
    }
  }
}

pub enum Network {
  MainNet,
  TestNet,
}

impl Network {
  pub fn first_txn_hash_for(&self, event: &Event) -> &str {
    match *self {
      Network::MainNet => {
        match event {
          Event::Minted => "0x00a0e4800c709f38d97cd7e769c756a8c059e6aca5eaeace81509c8ab7eccdf4",
          Event::Burnt => "0x9f53e01877d7b40db95d332c3172e1f0d628fd68eabc1e7fcf3316be5619fd50",
          Event::Swapped => "0xee5c4cc44822ee48d2ea8466a4a03c9adb41635caac600e27700fc5e81d8d2dc",
        }
      },
      Network::TestNet => {
        match event {
          Event::Minted => "0x9b8b5695c406d71137f5f420a67cf6b352ae865068530d293116dde072dbfdf6",
          Event::Burnt => "",
          Event::Swapped => "",
        }
      },
    }
  }

  pub fn contract_hash(&self) -> String {
    String::from(match *self {
      Network::TestNet => "0x1a62dd9c84b0c8948cb51fc664ba143e7a34985c",
      Network::MainNet => "0xBa11eB7bCc0a02e947ACF03Cc651Bfaf19C9EC00",
    })
  }
}

impl fmt::Display for Network {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Network::MainNet => write!(f, "mainnet"),
      Network::TestNet => write!(f, "testnet"),
    }
  }
}
