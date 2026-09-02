// SPDX-License-Identifier: AGPL-3.0-or-later
//! pg_lsn helpers. LSNs travel through SQL as text ("X/Y") and through Rust as u64.

pub fn parse(text: &str) -> Option<u64> {
    let (hi, lo) = text.trim().split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) | lo)
}

pub fn format(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for text in ["0/0", "0/16B3748", "1/0", "FFFFFFFF/FFFFFFFF"] {
            assert_eq!(format(parse(text).unwrap()), text);
        }
        assert_eq!(parse("garbage"), None);
    }
}
