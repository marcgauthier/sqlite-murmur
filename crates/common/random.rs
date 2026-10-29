//! Small random-byte and unbiased selection helpers backed by AWS-LC.
#![allow(dead_code)] // Each consumer uses a subset of these source-shared helpers.

use std::io;

pub(crate) fn fill(output: &mut [u8]) -> io::Result<()> {
    aws_lc_rs::rand::fill(output).map_err(|_| io::Error::other("AWS-LC randomness failed"))
}

pub(crate) fn bytes<const N: usize>() -> [u8; N] {
    let mut output = [0; N];
    fill(&mut output).expect("AWS-LC randomness failed");
    output
}

pub(crate) fn u64() -> u64 {
    u64::from_le_bytes(bytes())
}

trait Source {
    fn word(&mut self) -> io::Result<u64>;
}

struct System;
impl Source for System {
    fn word(&mut self) -> io::Result<u64> {
        let mut word = [0; 8];
        fill(&mut word)?;
        Ok(u64::from_le_bytes(word))
    }
}

fn below_with(source: &mut impl Source, upper: usize) -> io::Result<usize> {
    assert!(upper > 0, "random upper bound must be nonzero");
    if upper == 1 {
        return Ok(0);
    }
    let bound = u64::try_from(upper).expect("usize fits in u64");
    // Reject the short remainder of the 2^64 domain before reducing modulo
    // bound; each remaining result has the same number of representatives.
    let threshold = bound.wrapping_neg() % bound;
    loop {
        let word = source.word()?;
        if word >= threshold {
            return Ok((word % bound) as usize);
        }
    }
}

pub(crate) fn below(upper: usize) -> usize {
    below_with(&mut System, upper).expect("AWS-LC randomness failed")
}

fn shuffle_with<T>(source: &mut impl Source, values: &mut [T]) -> io::Result<()> {
    for i in (1..values.len()).rev() {
        let j = below_with(source, i + 1)?;
        values.swap(i, j);
    }
    Ok(())
}

pub(crate) fn shuffle<T>(values: &mut [T]) {
    shuffle_with(&mut System, values).expect("AWS-LC randomness failed")
}

fn sample_with<I: IntoIterator>(
    source: &mut impl Source,
    values: I,
    amount: usize,
) -> io::Result<Vec<I::Item>> {
    let mut sample = Vec::new();
    if amount == 0 {
        return Ok(sample);
    }
    for (seen, value) in values.into_iter().enumerate() {
        if sample.len() < amount {
            sample.push(value);
        } else {
            let slot = below_with(
                source,
                seen.checked_add(1).expect("sample population overflow"),
            )?;
            if slot < amount {
                sample[slot] = value;
            }
        }
    }
    Ok(sample)
}

pub(crate) fn sample<I: IntoIterator>(values: I, amount: usize) -> Vec<I::Item> {
    sample_with(&mut System, values, amount).expect("AWS-LC randomness failed")
}

pub(crate) fn choose<I: IntoIterator>(values: I) -> Option<I::Item> {
    sample(values, 1).pop()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Words(std::vec::IntoIter<u64>);
    impl Source for Words {
        fn word(&mut self) -> io::Result<u64> {
            self.0
                .next()
                .ok_or_else(|| io::Error::other("injected entropy failure"))
        }
    }
    fn words(input: &[u64]) -> Words {
        Words(input.to_vec().into_iter())
    }

    #[test]
    fn rejection_sampling_boundaries_and_failure() {
        let mut source = words(&[0, 4]); // 0 is rejected for bound 3.
        assert_eq!(below_with(&mut source, 3).unwrap(), 1);
        assert!(source.0.next().is_none());
        assert_eq!(below_with(&mut words(&[]), 1).unwrap(), 0);
        assert_eq!(below_with(&mut words(&[u64::MAX]), 2).unwrap(), 1);
        assert!(below_with(&mut words(&[]), 2).is_err());
        assert!(below_with(&mut words(&[0]), 3).is_err());
        if usize::BITS == 64 {
            assert_eq!(
                below_with(&mut words(&[0, u64::MAX]), usize::MAX).unwrap(),
                0
            );
        }
    }

    #[test]
    fn shuffle_preserves_elements_and_handles_small_slices() {
        let mut values = [0, 1, 2, 3];
        shuffle_with(&mut words(&[4, 3, 2]), &mut values).unwrap();
        assert_eq!(values, [1, 2, 3, 0]);
        shuffle_with(&mut words(&[]), &mut values[..0]).unwrap();
        shuffle_with(&mut words(&[]), &mut values[..1]).unwrap();
        assert!(shuffle_with(&mut words(&[]), &mut values).is_err());
    }

    #[test]
    fn reservoir_sampling_edges_and_equal_two_item_choices() {
        assert!(sample_with(&mut words(&[]), 0..0, 3).unwrap().is_empty());
        assert!(sample_with(&mut words(&[]), 0..10, 0).unwrap().is_empty());
        assert_eq!(
            sample_with(&mut words(&[]), 0..3, usize::MAX).unwrap(),
            [0, 1, 2]
        );
        assert_eq!(sample_with(&mut words(&[3, 3]), 0..4, 2).unwrap(), [2, 1]);
        assert_eq!(sample_with(&mut words(&[0]), 0..2, 1).unwrap(), [1]);
        assert_eq!(sample_with(&mut words(&[1]), 0..2, 1).unwrap(), [0]);
        assert!(sample_with(&mut words(&[]), 0..2, 1).is_err());
    }

    #[test]
    fn system_entropy_and_public_helpers() {
        let mut output = [0; 32];
        fill(&mut output).unwrap();
        let _ = bytes::<16>();
        let _ = u64();
        assert_eq!(below(1), 0);
        assert_eq!(choose(std::iter::empty::<u8>()), None);
        assert_eq!(choose([7]), Some(7));
    }
}
