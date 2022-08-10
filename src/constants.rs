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
      "Mint" => Some(Event::Minted),
      "Burnt" => Some(Event::Burnt),
      "Swapped" => Some(Event::Swapped),
      "Claimed" => Some(Event::Claimed),
      _ => None,
    }
  }
}

impl fmt::Display for Event {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Event::Minted => write!(f, "Mint"),
      Event::Burnt => write!(f, "Burnt"),
      Event::Swapped => write!(f, "Swapped"),
      Event::Claimed => write!(f, "Claimed"),
    }
  }
}

#[derive(Clone)]
pub enum Network {
  MainNet,
  TestNet,
}

impl fmt::Display for Network {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Network::MainNet => write!(f, "mainnet"),
      Network::TestNet => write!(f, "testnet"),
    }
  }
}

#[derive(Clone)]
pub enum ChainEventStatus {
  NotStarted,
  Processing,
  Completed,
}

impl ChainEventStatus {
  pub fn to_string(&self) -> String {
    match self {
      ChainEventStatus::NotStarted => "not_started".to_string(),
      ChainEventStatus::Processing => "processing".to_string(),
      ChainEventStatus::Completed => "completed".to_string(),
    }
  }

  pub fn from_str(value: String) -> Option<Self> {
    match value.as_str() {
      "not_started" => Some(ChainEventStatus::NotStarted),
      "processing" => Some(ChainEventStatus::Processing),
      "completed" => Some(ChainEventStatus::Completed),
      _ => None,
    }
  }
}
