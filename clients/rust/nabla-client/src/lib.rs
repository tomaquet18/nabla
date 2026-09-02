//! Reference client for nabla view subscriptions.
//!
//! The protocol, as implemented by [`Subscription`]:
//!
//! 1. Connect and `LISTEN "nabla:<view>"` (the channel is the qualified view
//!    name as stored in `nabla.views.name`).
//! 2. Bootstrap atomically: in one `REPEATABLE READ` transaction read
//!    `nabla.status(view)` and `SELECT * FROM <view>`, and emit
//!    [`Event::Snapshot`] whose `cursor` is the view's current sequence number.
//! 3. Follow: on every notification, and on a fallback timer, call
//!    `nabla.changes(view, cursor, batch)` until it returns fewer rows than
//!    `batch`; consecutive rows sharing `(xid, lsn)` form one
//!    [`Event::Transaction`]; the cursor advances only after the event was
//!    handed to the caller.
//! 4. Resync: a `lagged` error, a `stale` error, an epoch change, or a lost
//!    connection produce [`Event::Resync`] followed by a new bootstrap.
//!
//! Buffers are bounded: at most one batch plus the trailing (possibly
//! incomplete) transaction is held, and notifications are coalesced.

use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;

use futures::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_postgres::{AsyncMessage, Client, IsolationLevel, NoTls};

#[derive(Debug)]
pub enum Error {
    Postgres(tokio_postgres::Error),
    /// The server violated the subscription protocol (for example a gap in
    /// sequence numbers). Not recoverable by resync.
    Protocol(String),
    NotConnected,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Postgres(e) => write!(f, "postgres: {e}"),
            Error::Protocol(m) => write!(f, "protocol violation: {m}"),
            Error::NotConnected => write!(f, "not connected"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Error::Postgres(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Insert,
    Delete,
}

#[derive(Debug, Clone)]
pub struct Delta {
    pub seq: i64,
    pub op: Op,
    pub row: Value,
}

#[derive(Debug, Clone)]
pub enum ResyncReason {
    /// The cursor fell behind `nabla.retain_deltas`.
    Lagged,
    /// The view was refreshed (`nabla.refresh`).
    EpochChanged { from: i32, to: i32 },
    /// The view is no longer maintained; bootstrap is retried with backoff.
    Stale { reason: String },
    /// The connection was lost and re-established.
    Disconnected,
}

impl fmt::Display for ResyncReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResyncReason::Lagged => write!(f, "lagged"),
            ResyncReason::EpochChanged { from, to } => write!(f, "epoch changed ({from} -> {to})"),
            ResyncReason::Stale { reason } => write!(f, "stale: {reason}"),
            ResyncReason::Disconnected => write!(f, "disconnected"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    Snapshot { epoch: i32, frontier: String, cursor: i64, rows: Vec<Value> },
    Transaction { xid: Option<i64>, lsn: String, epoch: i32, deltas: Vec<Delta> },
    Resync { reason: ResyncReason },
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Rows per `nabla.changes` call.
    pub batch: i32,
    /// Fallback poll when no notification arrives.
    pub poll_interval: Duration,
    /// Keep the `_nabla_*` columns in snapshot rows and deltas.
    pub keep_hidden: bool,
    /// Longest wait between bootstrap attempts on a stale view.
    pub max_backoff: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options { batch: 1000, poll_interval: Duration::from_secs(1), keep_hidden: false, max_backoff: Duration::from_secs(30) }
    }
}

struct Conn {
    client: Client,
    /// Wake-ups from `nabla:<view>` notifications; coalesced, bounded.
    notifications: mpsc::Receiver<()>,
}

struct ChangeRow {
    seq: i64,
    lsn: String,
    xid: Option<i64>,
    op: Op,
    row: Value,
    epoch: i32,
}

enum State {
    NeedBootstrap,
    Following,
}

enum Failure {
    Lagged,
    Stale(String),
    Disconnected,
    Other(Error),
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_view(view: &str) -> String {
    match view.split_once('.') {
        Some((schema, name)) => format!("{}.{}", quote_ident(schema), quote_ident(name)),
        None => quote_ident(view),
    }
}

fn strip_hidden(mut row: Value, keep: bool) -> Value {
    if !keep {
        if let Value::Object(map) = &mut row {
            map.retain(|k, _| !k.starts_with("_nabla_"));
        }
    }
    row
}

fn classify(e: tokio_postgres::Error) -> Failure {
    if let Some(db) = e.as_db_error() {
        let message = db.message();
        if message.contains("lagged behind retention") {
            return Failure::Lagged;
        }
        if let Some((_, reason)) = message.split_once(" is stale: ") {
            return Failure::Stale(reason.to_string());
        }
        return Failure::Other(Error::Postgres(e));
    }
    // Anything that is not a server-reported error is a transport problem.
    Failure::Disconnected
}

pub struct Subscription {
    config: String,
    view: String,
    options: Options,
    conn: Option<Conn>,
    connected_once: bool,
    state: State,
    epoch: i32,
    cursor: i64,
    /// Events ready to hand out, with the cursor to adopt once handed out.
    pending: VecDeque<(Event, Option<i64>)>,
    /// Rows of a transaction that may continue in the next batch.
    tail: Vec<ChangeRow>,
    stale_backoff: Duration,
}

impl Subscription {
    /// Connect to `config` (a libpq-style connection string) and prepare to
    /// follow `view` (schema-qualified, as given to `nabla.create_view`).
    pub async fn open(config: &str, view: &str) -> Result<Self> {
        Self::open_with(config, view, Options::default()).await
    }

    pub async fn open_with(config: &str, view: &str, options: Options) -> Result<Self> {
        let mut sub = Subscription {
            config: config.to_string(),
            view: view.trim().to_lowercase(),
            options,
            conn: None,
            connected_once: false,
            state: State::NeedBootstrap,
            epoch: 0,
            cursor: 0,
            pending: VecDeque::new(),
            tail: Vec::new(),
            stale_backoff: Duration::from_secs(1),
        };
        sub.connect().await?;
        Ok(sub)
    }

    async fn connect(&mut self) -> Result<()> {
        let (client, mut connection) = tokio_postgres::connect(&self.config, NoTls).await?;
        let (tx, rx) = mpsc::channel::<()>(64);
        tokio::spawn(async move {
            let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
            while let Some(message) = messages.next().await {
                match message {
                    Ok(AsyncMessage::Notification(_)) => {
                        // Coalesce: a full channel already carries a wake-up.
                        let _ = tx.try_send(());
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            // Dropping `tx` closes the channel: the subscription sees Disconnected.
        });
        client.batch_execute(&format!("LISTEN {}", quote_ident(&format!("nabla:{}", self.view)))).await?;
        self.conn = Some(Conn { client, notifications: rx });
        self.connected_once = true;
        self.state = State::NeedBootstrap;
        self.tail.clear();
        Ok(())
    }

    /// Reconnect with backoff after a lost connection.
    async fn reconnect(&mut self) {
        let mut delay = Duration::from_millis(500);
        loop {
            if self.connect().await.is_ok() {
                self.pending.push_back((Event::Resync { reason: ResyncReason::Disconnected }, None));
                return;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
        }
    }

    /// The next event. Blocks until a transaction arrives; a lost notification
    /// only delays it by `poll_interval`.
    pub async fn next(&mut self) -> Result<Event> {
        loop {
            if let Some((event, cursor)) = self.pending.pop_front() {
                if let Some(c) = cursor {
                    self.cursor = c;
                }
                return Ok(event);
            }
            if self.conn.is_none() {
                self.reconnect().await;
                continue;
            }
            match self.state {
                State::NeedBootstrap => match self.bootstrap().await {
                    Ok(None) => {}
                    Ok(Some((reason, backoff))) => {
                        self.pending.push_back((Event::Resync { reason: ResyncReason::Stale { reason } }, None));
                        tokio::time::sleep(backoff).await;
                    }
                    Err(Failure::Disconnected) => self.conn = None,
                    Err(Failure::Other(e)) => return Err(e),
                    Err(Failure::Lagged) | Err(Failure::Stale(_)) => {}
                },
                State::Following => {
                    if self.tail.is_empty() && !self.wait_for_wakeup().await {
                        self.conn = None;
                        continue;
                    }
                    match self.fetch().await {
                        Ok(()) => {}
                        Err(Failure::Lagged) => {
                            let reason = match self.current_epoch().await {
                                Ok(Some(to)) if to != self.epoch => ResyncReason::EpochChanged { from: self.epoch, to },
                                _ => ResyncReason::Lagged,
                            };
                            self.pending.push_back((Event::Resync { reason }, None));
                            self.tail.clear();
                            self.state = State::NeedBootstrap;
                        }
                        Err(Failure::Stale(reason)) => {
                            self.pending.push_back((Event::Resync { reason: ResyncReason::Stale { reason } }, None));
                            self.tail.clear();
                            self.state = State::NeedBootstrap;
                        }
                        Err(Failure::Disconnected) => {
                            self.tail.clear();
                            self.conn = None;
                        }
                        Err(Failure::Other(e)) => return Err(e),
                    }
                }
            }
        }
    }

    /// Wait for a notification or the fallback timer. False when the
    /// connection is gone.
    async fn wait_for_wakeup(&mut self) -> bool {
        let conn = self.conn.as_mut().expect("connected");
        match tokio::time::timeout(self.options.poll_interval, conn.notifications.recv()).await {
            Ok(Some(())) => {
                while conn.notifications.try_recv().is_ok() {}
                true
            }
            Ok(None) => false,
            Err(_elapsed) => true,
        }
    }

    /// One REPEATABLE READ transaction: status and full content from the same
    /// snapshot. Returns the stale reason and the backoff to sleep when the
    /// view is stale.
    async fn bootstrap(&mut self) -> std::result::Result<Option<(String, Duration)>, Failure> {
        let keep = self.options.keep_hidden;
        let conn = self.conn.as_mut().ok_or(Failure::Other(Error::NotConnected))?;
        let result: std::result::Result<Option<Event>, tokio_postgres::Error> = async {
            let tx = conn
                .client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await?;
            let status = tx
                .query_one(
                    "SELECT status, epoch, frontier_lsn::text, current_seq, stale_reason FROM nabla.status($1)",
                    &[&self.view],
                )
                .await?;
            let state: String = status.get(0);
            if state == "stale" {
                let reason: Option<String> = status.get(4);
                tx.commit().await?;
                return Ok(Some(Event::Resync {
                    reason: ResyncReason::Stale { reason: reason.unwrap_or_else(|| "reason not recorded".into()) },
                }));
            }
            let epoch: i32 = status.get(1);
            let frontier: String = status.get(2);
            let cursor: i64 = status.get(3);
            let rows = tx.query(&format!("SELECT to_jsonb(v) FROM {} AS v", quote_view(&self.view)), &[]).await?;
            tx.commit().await?;
            let rows = rows.into_iter().map(|r| strip_hidden(r.get::<_, Value>(0), keep)).collect();
            Ok(Some(Event::Snapshot { epoch, frontier, cursor, rows }))
        }
        .await;
        match result {
            Ok(Some(Event::Resync { reason: ResyncReason::Stale { reason } })) => {
                let backoff = self.stale_backoff;
                self.stale_backoff = (self.stale_backoff * 2).min(self.options.max_backoff);
                Ok(Some((reason, backoff)))
            }
            Ok(Some(Event::Snapshot { epoch, frontier, cursor, rows })) => {
                self.epoch = epoch;
                self.cursor = cursor;
                self.stale_backoff = Duration::from_secs(1);
                self.state = State::Following;
                self.tail.clear();
                self.pending.push_back((Event::Snapshot { epoch, frontier, cursor, rows }, Some(cursor)));
                Ok(None)
            }
            Ok(_) => Ok(None),
            Err(e) => Err(classify(e)),
        }
    }

    async fn current_epoch(&mut self) -> Result<Option<i32>> {
        let conn = self.conn.as_mut().ok_or(Error::NotConnected)?;
        let row = conn.client.query_opt("SELECT epoch FROM nabla.status($1)", &[&self.view]).await?;
        Ok(row.map(|r| r.get(0)))
    }

    /// Fetch one batch after the last known row and turn complete
    /// transactions into pending events.
    async fn fetch(&mut self) -> std::result::Result<(), Failure> {
        let after = self.tail.last().map_or(self.cursor, |r| r.seq);
        let batch = self.options.batch;
        let keep = self.options.keep_hidden;
        let conn = self.conn.as_mut().ok_or(Failure::Other(Error::NotConnected))?;
        let rows = conn
            .client
            .query(
                "SELECT seq, lsn::text, xid, op::text, row, epoch FROM nabla.changes($1, $2, $3)",
                &[&self.view, &after, &batch],
            )
            .await
            .map_err(classify)?;
        let full = rows.len() as i32 >= batch;
        let mut expected = after + 1;
        let mut parsed = Vec::with_capacity(rows.len());
        for r in rows {
            let seq: i64 = r.get(0);
            if seq != expected {
                return Err(Failure::Other(Error::Protocol(format!(
                    "expected seq {expected}, got {seq}: deltas of a view must be contiguous"
                ))));
            }
            expected += 1;
            let op: String = r.get(3);
            let op = match op.as_str() {
                "I" => Op::Insert,
                "D" => Op::Delete,
                other => return Err(Failure::Other(Error::Protocol(format!("unknown op {other:?}")))),
            };
            parsed.push(ChangeRow {
                seq,
                lsn: r.get(1),
                xid: r.get(2),
                op,
                row: strip_hidden(r.get(4), keep),
                epoch: r.get(5),
            });
        }
        let mut all = std::mem::take(&mut self.tail);
        all.extend(parsed);

        // Group consecutive rows by (xid, lsn); the same xid with another lsn
        // is another transaction.
        let mut groups: Vec<Vec<ChangeRow>> = Vec::new();
        for row in all {
            match groups.last_mut() {
                Some(g) if g[0].xid == row.xid && g[0].lsn == row.lsn => g.push(row),
                _ => groups.push(vec![row]),
            }
        }
        if full {
            // The last group may continue in the next batch.
            if let Some(tail) = groups.pop() {
                self.tail = tail;
            }
        }
        for group in groups {
            if let Some(other) = group.iter().find(|r| r.epoch != self.epoch) {
                self.pending.push_back((
                    Event::Resync { reason: ResyncReason::EpochChanged { from: self.epoch, to: other.epoch } },
                    None,
                ));
                self.tail.clear();
                self.state = State::NeedBootstrap;
                return Ok(());
            }
            let last_seq = group.last().map(|r| r.seq).expect("non-empty group");
            let xid = group[0].xid;
            let lsn = group[0].lsn.clone();
            let epoch = group[0].epoch;
            let deltas = group.into_iter().map(|r| Delta { seq: r.seq, op: r.op, row: r.row }).collect();
            self.pending.push_back((Event::Transaction { xid, lsn, epoch, deltas }, Some(last_seq)));
        }
        Ok(())
    }

    /// Read-your-writes: block until the view's frontier reaches `lsn`
    /// (typically `pg_current_wal_lsn()` right after a commit).
    pub async fn wait_for(&self, lsn: &str, timeout: Duration) -> Result<bool> {
        let conn = self.conn.as_ref().ok_or(Error::NotConnected)?;
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let row = conn
            .client
            .query_one("SELECT nabla.wait_for($1, $2::pg_lsn, $3)", &[&self.view, &lsn, &timeout_ms])
            .await?;
        Ok(row.get(0))
    }

    pub fn cursor(&self) -> i64 {
        self.cursor
    }

    pub fn epoch(&self) -> i32 {
        self.epoch
    }
}
