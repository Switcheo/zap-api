use std::{fmt};
use std::collections::HashMap;

pub mod zwap_emission {
  pub static RETROACTIVE_DISTRIBUTION_CUTOFF_TIME: i64 = 1610964000;
  pub static DISTRIBUTION_START_TIME: i64 = 1612339200;
  pub static EPOCH_PERIOD: i64 = 604800; // one week
  pub static TOTAL_NUMBER_OF_EPOCH: u32 = 152 + 1; // +1 for dummy epoch released for retroactive traders airdrop
  pub static TOKENS_PER_EPOCH: u32 = 6250;
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

#[derive(Clone)]
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
          Event::Burnt => "0xfbec07771a8cabd7c90c084b4ca77bf0a7216970eae5f68233b21cf13c947a3f",
          Event::Swapped => "0x1b660c073e30157e50085848c3595fda62c87ce1e7db328a46f8eb48c4c36957",
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

  pub fn incentived_pools(&self) -> HashMap<String, u32> {
    match *self {
      Network::TestNet => [
        (String::from("zil1fytuayks6njpze00ukasq3m4y4s44k79hvz8q5"), 3), // gZIL
        (String::from("zil10a9z324aunx2qj64984vke93gjdnzlnl5exygv"), 2), // XSGD
        (String::from("zil1ktmx2udqc77eqq0mdjn8kqdvwjf9q5zvy6x7vu"), 5), // ZWAP
        (String::from("zil14jmjrkvfcz2uvj3y69kl6gas34ecuf2j5ggmye"), 1), // REDC
      ].iter().cloned().collect(),
      Network::MainNet => [
        (String::from("zil1p5suryq6q647usxczale29cu3336hhp376c627"), 176), // ZWAP
        (String::from("zil14pzuzq6v6pmmmrfjhczywguu0e97djepxt8g3e"), 123), // gZIL
        (String::from("zil1zu72vac254htqpg3mtywdcfm84l3dfd9qzww8t"), 123), // XSGD
        (String::from("zil18f5rlhqz9vndw4w8p60d0n7vg3n9sqvta7n6t2"), 14), // PORT
        (String::from("zil1l0g8u6f9g0fsvjuu74ctyla2hltefrdyt7k5f4"), 14), // ZLP
        (String::from("zil1nr5ek93jmams8nqqcxhc8ux7g47rzhf63gcm4l"), 14), // REDC
      ].iter().cloned().collect(),
    }
  }

  pub fn developer_address(&self) -> String {
    String::from(match *self {
      Network::TestNet => "zil1ua2dhnlykmxtnuaudmqd3uju6altn6lq0lqvl9",
      Network::MainNet => "zil1ytk3ykwlc2vy8fyp7wqp492zjassj5mxzgscv6",
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
