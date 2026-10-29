//! Total order over [`crate::model::Value`] for Min/Max merge policies.
use crate::{Error, Result, model::Value};

pub fn compare(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    let rank = |v: &Value| match v {
        Value::Null => 0,
        Value::Integer(_) | Value::Real(_) => 1,
        Value::Text(_) => 2,
        Value::Blob(_) => 3,
    };
    let ra = rank(a);
    let rb = rank(b);
    if ra != rb {
        return Ok(ra.cmp(&rb));
    }
    Ok(match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Real(a), Value::Real(b)) => {
            a.partial_cmp(b).ok_or_else(|| Error::Invalid("NaN policy value".into()))?
        }
        (Value::Integer(a), Value::Real(b)) => integer_real(*a, *b)?,
        (Value::Real(a), Value::Integer(b)) => integer_real(*b, *a)?.reverse(),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
        _ => return Err(Error::Invalid("policy value types".into())),
    })
}

fn integer_real(a: i64, b: f64) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if b.is_nan() {
        return Err(Error::Invalid("NaN policy value".into()));
    }
    if b >= 9223372036854775808.0 {
        return Ok(Ordering::Less);
    }
    if b < -9223372036854775808.0 {
        return Ok(Ordering::Greater);
    }
    let integer = b as i64;
    let order = a.cmp(&integer);
    if order != Ordering::Equal {
        return Ok(order);
    }
    Ok(if b.fract() > 0.0 {
        Ordering::Less
    } else if b.fract() < 0.0 {
        Ordering::Greater
    } else {
        Ordering::Equal
    })
}
