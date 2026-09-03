// SPDX-License-Identifier: AGPL-3.0-or-later
//! Decoder for the pgoutput binary protocol, proto_version 1.
//!
//! Only the message kinds the worker needs are modelled. Origin ('O'),
//! logical message ('M') and type ('Y') messages are ignored.

use std::collections::HashMap;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct RelationColumn {
    pub name: String,
    pub is_key: bool,
    pub typid: u32,
    pub typmod: i32,
    /// Filled lazily via format_type() so decoded text can be cast back.
    pub type_name: Option<String>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub replident: u8,
    pub columns: Vec<RelationColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnValue {
    Null,
    /// Unchanged TOASTed value that pgoutput did not send.
    Unchanged,
    Text(String),
}

pub type Tuple = Vec<ColumnValue>;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Message {
    Begin { final_lsn: u64, xid: u32 },
    Commit { commit_lsn: u64, end_lsn: u64 },
    Relation(Relation),
    Insert { relid: u32, new: Tuple },
    Update { relid: u32, old: Option<(u8, Tuple)>, new: Tuple },
    Delete { relid: u32, key_kind: u8, old: Tuple },
    Truncate { relids: Vec<u32> },
    Ignored(u8),
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.pos + n > self.data.len() {
            return Err(format!(
                "truncated pgoutput message: wanted {n} bytes at offset {}, have {}",
                self.pos,
                self.data.len()
            ));
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16, String> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, String> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn cstring(&mut self) -> Result<String, String> {
        let rest = &self.data[self.pos..];
        let end =
            rest.iter().position(|b| *b == 0).ok_or_else(|| "unterminated string in pgoutput message".to_string())?;
        let s = String::from_utf8_lossy(&rest[..end]).into_owned();
        self.pos += end + 1;
        Ok(s)
    }
    fn tuple(&mut self) -> Result<Tuple, String> {
        let ncols = self.i16()?;
        let mut values = Vec::with_capacity(ncols.max(0) as usize);
        for _ in 0..ncols {
            let tag = self.u8()?;
            values.push(match tag {
                b'n' => ColumnValue::Null,
                b'u' => ColumnValue::Unchanged,
                b't' => {
                    let len = self.i32()?;
                    let bytes = self.take(len.max(0) as usize)?;
                    ColumnValue::Text(String::from_utf8_lossy(bytes).into_owned())
                }
                other => return Err(format!("unknown tuple column tag {other:#x}")),
            });
        }
        Ok(values)
    }
}

pub fn decode(data: &[u8]) -> Result<Message, String> {
    let mut r = Reader { data, pos: 0 };
    let kind = r.u8()?;
    let msg = match kind {
        b'B' => {
            let final_lsn = r.u64()?;
            let _commit_ts = r.u64()?;
            let xid = r.u32()?;
            Message::Begin { final_lsn, xid }
        }
        b'C' => {
            let _flags = r.u8()?;
            let commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            let _ts = r.u64()?;
            Message::Commit { commit_lsn, end_lsn }
        }
        b'R' => {
            let id = r.u32()?;
            let namespace = r.cstring()?;
            let name = r.cstring()?;
            let replident = r.u8()?;
            let ncols = r.i16()?;
            let mut columns = Vec::with_capacity(ncols.max(0) as usize);
            for _ in 0..ncols {
                let flags = r.u8()?;
                let name = r.cstring()?;
                let typid = r.u32()?;
                let typmod = r.i32()?;
                columns.push(RelationColumn { name, is_key: flags & 1 == 1, typid, typmod, type_name: None });
            }
            Message::Relation(Relation { id, namespace, name, replident, columns })
        }
        b'I' => {
            let relid = r.u32()?;
            let tag = r.u8()?;
            if tag != b'N' {
                return Err(format!("insert message: expected 'N', got {tag:#x}"));
            }
            Message::Insert { relid, new: r.tuple()? }
        }
        b'U' => {
            let relid = r.u32()?;
            let mut tag = r.u8()?;
            let mut old = None;
            if tag == b'K' || tag == b'O' {
                old = Some((tag, r.tuple()?));
                tag = r.u8()?;
            }
            if tag != b'N' {
                return Err(format!("update message: expected 'N', got {tag:#x}"));
            }
            Message::Update { relid, old, new: r.tuple()? }
        }
        b'D' => {
            let relid = r.u32()?;
            let key_kind = r.u8()?;
            if key_kind != b'K' && key_kind != b'O' {
                return Err(format!("delete message: expected 'K' or 'O', got {key_kind:#x}"));
            }
            Message::Delete { relid, key_kind, old: r.tuple()? }
        }
        b'T' => {
            let n = r.i32()?;
            let _flags = r.u8()?;
            let mut relids = Vec::with_capacity(n.max(0) as usize);
            for _ in 0..n {
                relids.push(r.u32()?);
            }
            Message::Truncate { relids }
        }
        b'O' | b'M' | b'Y' => Message::Ignored(kind),
        other => return Err(format!("unknown pgoutput message kind {other:#x}")),
    };
    Ok(msg)
}

/// One decoded row change inside a source transaction.
#[derive(Debug)]
pub enum Change {
    Insert { relid: u32, new: Tuple },
    Update { relid: u32, old: Option<(u8, Tuple)>, new: Tuple },
    Delete { relid: u32, key_kind: u8, old: Tuple },
    Truncate { relids: Vec<u32> },
}

/// A complete source transaction (Begin .. Commit).
#[derive(Debug)]
pub struct SourceTransaction {
    pub xid: u32,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub changes: Vec<Change>,
}

/// Groups decoded messages into complete transactions and keeps the relation cache.
#[derive(Default)]
pub struct Decoder {
    pub relations: HashMap<u32, Relation>,
    current: Option<SourceTransaction>,
    pub complete: Vec<SourceTransaction>,
}

impl Decoder {
    pub fn feed(&mut self, data: &[u8]) -> Result<(), String> {
        match decode(data)? {
            Message::Begin { final_lsn, xid } => {
                if self.current.is_some() {
                    return Err("BEGIN while a transaction is already open".to_string());
                }
                self.current = Some(SourceTransaction { xid, commit_lsn: final_lsn, end_lsn: 0, changes: Vec::new() });
            }
            Message::Commit { commit_lsn, end_lsn } => {
                let mut tx = self.current.take().ok_or("COMMIT without BEGIN")?;
                tx.commit_lsn = commit_lsn;
                tx.end_lsn = end_lsn;
                self.complete.push(tx);
            }
            Message::Relation(rel) => {
                // Keep resolved type names when the relation definition is unchanged.
                let mut rel = rel;
                if let Some(prev) = self.relations.get(&rel.id) {
                    for (col, old) in rel.columns.iter_mut().zip(prev.columns.iter()) {
                        if col.name == old.name && col.typid == old.typid && col.typmod == old.typmod {
                            col.type_name = old.type_name.clone();
                        }
                    }
                }
                self.relations.insert(rel.id, rel);
            }
            Message::Insert { relid, new } => self.push(Change::Insert { relid, new })?,
            Message::Update { relid, old, new } => self.push(Change::Update { relid, old, new })?,
            Message::Delete { relid, key_kind, old } => self.push(Change::Delete { relid, key_kind, old })?,
            Message::Truncate { relids } => self.push(Change::Truncate { relids })?,
            Message::Ignored(_) => {}
        }
        Ok(())
    }

    fn push(&mut self, change: Change) -> Result<(), String> {
        self.current.as_mut().ok_or("row change outside a transaction")?.changes.push(change);
        Ok(())
    }

    /// True when the last fed message left a transaction open.
    pub fn has_incomplete(&self) -> bool {
        self.current.is_some()
    }

    /// Drop an unfinished trailing transaction; it will be re-read next round.
    pub fn discard_incomplete(&mut self) {
        self.current = None;
    }

    pub fn take_complete(&mut self) -> Vec<SourceTransaction> {
        std::mem::take(&mut self.complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    #[test]
    fn decodes_relation_and_insert() {
        let mut rel = vec![b'R'];
        rel.extend(16u32.to_be_bytes());
        rel.extend(cstr("public"));
        rel.extend(cstr("orders"));
        rel.push(b'd');
        rel.extend(2i16.to_be_bytes());
        rel.push(1);
        rel.extend(cstr("id"));
        rel.extend(20u32.to_be_bytes());
        rel.extend((-1i32).to_be_bytes());
        rel.push(0);
        rel.extend(cstr("k"));
        rel.extend(23u32.to_be_bytes());
        rel.extend((-1i32).to_be_bytes());
        match decode(&rel).unwrap() {
            Message::Relation(r) => {
                assert_eq!(r.name, "orders");
                assert!(r.columns[0].is_key);
                assert_eq!(r.columns[1].typid, 23);
            }
            other => panic!("{other:?}"),
        }

        let mut ins = vec![b'I'];
        ins.extend(16u32.to_be_bytes());
        ins.push(b'N');
        ins.extend(2i16.to_be_bytes());
        ins.push(b't');
        ins.extend(2i32.to_be_bytes());
        ins.extend(b"42");
        ins.push(b'n');
        match decode(&ins).unwrap() {
            Message::Insert { relid, new } => {
                assert_eq!(relid, 16);
                assert_eq!(new, vec![ColumnValue::Text("42".into()), ColumnValue::Null]);
            }
            other => panic!("{other:?}"),
        }
    }
}
