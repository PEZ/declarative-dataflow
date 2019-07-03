pub use num_rational::Rational32;

#[cfg(feature = "uuid")]
pub use uuid::Uuid;

/// @TODO
pub trait AsValue: timely::Data + timely::ExchangeData
    + differential_dataflow::Data + differential_dataflow::ExchangeData
    + PartialOrd + Ord + Eq + Clone + core::fmt::Debug { }

pub trait AsAid: timely::Data + timely::ExchangeData
    + differential_dataflow::Data + differential_dataflow::ExchangeData
    + PartialOrd + Ord + Eq + Clone + core::fmt::Debug { }

/// A unique entity identifier.
pub type Eid = u64;

/// A unique attribute identifier.
pub type Aid = String; // u32

/// Possible data values.
///
/// This enum captures the currently supported data types, and is the
/// least common denominator for the types of records moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// An attribute identifier
    Aid(Aid),
    /// A string
    String(String),
    /// A boolean
    Bool(bool),
    /// A 64 bit signed integer
    Number(i64),
    /// A 32 bit rational
    Rational32(Rational32),
    /// An entity identifier
    Eid(Eid),
    /// Milliseconds since midnight, January 1, 1970 UTC
    Instant(u64),
    /// A 16 byte unique identifier.
    #[cfg(feature = "uuid")]
    Uuid(Uuid),
    /// A fixed-precision real number.
    #[cfg(feature = "real")]
    Real(fixed::types::I16F16),
}

impl Value {
    /// Helper to create an Aid value from a string representation.
    pub fn aid(v: &str) -> Self {
        Value::Aid(v.to_string())
    }

    /// Helper to create a UUID value from a string representation.
    #[cfg(feature = "uuid")]
    pub fn uuid_str(v: &str) -> Self {
        let uuid = Uuid::parse_str(v).expect("failed to parse UUID");
        Value::Uuid(uuid)
    }
}

impl std::convert::From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

#[cfg(feature = "real")]
impl std::convert::From<f64> for Value {
    fn from(v: f64) -> Self {
        let real =
            fixed::types::I16F16::checked_from_float(v).expect("failed to convert to I16F16");

        Value::Real(real)
    }
}

#[cfg(feature = "serde_json")]
impl std::convert::From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Aid(v) => serde_json::Value::String(v),
            Value::String(v) => serde_json::Value::String(v),
            Value::Bool(v) => serde_json::Value::Bool(v),
            Value::Number(v) => serde_json::Value::Number(serde_json::Number::from(v)),
            _ => unimplemented!(),
        }
    }
}

impl std::convert::From<Value> for Eid {
    fn from(v: Value) -> Eid {
        if let Value::Eid(eid) = v {
            eid
        } else {
            panic!("Value {:?} can't be converted to Eid", v);
        }
    }
}

// /// @FIXME
// pub fn next_id() -> V::Eid {
//     ID.fetch_add(1, atomic::Ordering::SeqCst) as V::Eid
// }
