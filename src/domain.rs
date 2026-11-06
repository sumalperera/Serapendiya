use std::{fmt, str::FromStr};

use crate::error::Error;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StreamName(String);

impl StreamName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for StreamName {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let valid = !value.is_empty()
            && value.len() <= 249
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
        if valid {
            Ok(Self(value.to_owned()))
        } else {
            Err(Error::InvalidValue {
                kind: "stream name",
                value: value.to_owned(),
            })
        }
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionId(u32);

impl PartitionId {
    pub fn new(value: u32) -> Self {
        Self(value)
    }
    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Offset(u64);

impl Offset {
    pub const ZERO: Self = Self(0);
    pub fn new(value: u64) -> Self {
        Self(value)
    }
    pub fn get(self) -> u64 {
        self.0
    }
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_names_are_bounded_and_ascii() {
        assert!("orders.v1".parse::<StreamName>().is_ok());
        assert!("".parse::<StreamName>().is_err());
        assert!("orders/one".parse::<StreamName>().is_err());
    }

    #[test]
    fn offsets_use_checked_arithmetic() {
        assert_eq!(Offset::new(41).checked_next().map(Offset::get), Some(42));
        assert_eq!(Offset::new(u64::MAX).checked_next(), None);
    }

    #[test]
    fn partition_zero_is_valid() {
        assert_eq!(PartitionId::new(0).get(), 0);
    }
}
