//! Reference client for nabla view subscriptions.
//!
//! The protocol, as implemented by [`Subscription`]:
//!
//! 1. Connect, read the canonical view name from `nabla.status(view)` and
//!    `LISTEN "nabla:<name>"`.
//! 2. Bootstrap atomically: in one `REPEATABLE READ` transaction read
//!    `nabla.status(view)`, `nabla.visible_columns(view)` and the view's rows,
//!    and emit [`Event::Snapshot`] whose `cursor` is the view's current
//!    sequence number and whose `epoch` every later `changes()` call carries.
//!    While the view is `initializing` or `refreshing` the client waits and
//!    retries; a `failed` view is an error carrying the recorded reason.
//! 3. Follow: on every notification, and on a fallback timer, call
//!    `nabla.changes(view, cursor, epoch, batch)` until it returns fewer rows
//!    than `batch` (the server never splits a transaction); consecutive rows
//!    sharing `(xid, lsn)` form one [`Event::Transaction`]; the cursor
//!    advances only after the event was handed to the caller.
//! 4. Resync: SQLSTATE `NB001` (lagged), `NB002` (stale) or `NB003` (epoch
//!    changed), or a lost connection, produce [`Event::Resync`] followed by a
//!    new bootstrap. No error message text is ever inspected.
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

pub const SQLSTATE_LAGGED: &str = "NB001";
pub const SQLSTATE_STALE: &str = "NB002";
pub const SQLSTATE_EPOCH_CHANGED: &str = "NB003";
pub const SQLSTATE_FAILED: &str = "NB006";

#[derive(Debug)]
pub enum Error {
    Postgres(tokio_postgres::Error),
    /// The server violated the subscription protocol (for example a gap in
    /// sequence numbers). Not recoverable by resync.
    Protocol(String),
    NotConnected,
    /// The view's build or rebuild failed (status `failed`, NB006); not
    /// recoverable by resync.
    ViewFailed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Postgres(e) => write!(f, "postgres: {e}"),
            Error::Protocol(m) => write!(f, "protocol violation: {m}"),
            Error::NotConnected => write!(f, "not connected"),
            Error::ViewFailed(r) => write!(f, "view failed to build: {r}"),
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
    /// The cursor fell behind `nabla.retain_deltas` (NB001).
    Lagged,
    /// The view was refreshed (NB003).
    EpochChanged { from: i32, to: i32 },
    /// The view is no longer maintained (NB002); bootstrap retries with backoff.
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
    /// Rows per `nabla.changes` call (a trailing transaction may exceed it).
    pub batch: i32,
    /// Fallback poll when no notification arrives.
    pub poll_interval: Duration,
    /// Include the `_nabla_*` maintenance columns in snapshot rows and deltas.
    pub keep_hidden: bool,
    /// Longest wait between bootstrap attempts on a stale view.
    pub max_backoff: Duration,
    /// Wait between bootstrap attempts while the view is being built.
    pub build_poll: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            batch: 1000,
            poll_interval: Duration::from_secs(1),
            keep_hidden: false,
            max_backoff: Duration::from_secs(30),
            build_poll: Duration::from_millis(200),
        }
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
}

enum State {
    NeedBootstrap,
    Following,
}

enum Failure {
    Lagged,
    Stale(String),
    EpochChanged { from: i32, to: i32 },
    Disconnected,
    Other(Error),
}

/// Outcome of one bootstrap attempt.
enum Boot {
    Ready,
    /// The view is being built or rebuilt: try again shortly, silently.
    Building,
    Stale(String),
    Failed(String),
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

/// Classify a server error by SQLSTATE only.
fn classify(e: tokio_postgres::Error, epoch: i32) -> Failure {
    let Some(db) = e.as_db_error() else {
        // Anything that is not a server-reported error is a transport problem.
        return Failure::Disconnected;
    };
    match db.code().code() {
        SQLSTATE_LAGGED => Failure::Lagged,
        SQLSTATE_STALE => Failure::Stale(db.detail().unwrap_or("reason not recorded").to_string()),
        SQLSTATE_EPOCH_CHANGED => {
            // DETAIL is "epoch N -> M"; the client's epoch is N.
            let to = db
                .detail()
                .and_then(|d| d.rsplit(' ').next())
                .and_then(|m| m.parse::<i32>().ok())
                .unwrap_or(epoch);
            Failure::EpochChanged { from: epoch, to }
        }
        SQLSTATE_FAILED => Failure::Other(Error::ViewFailed(db.detail().unwrap_or("reason not recorded").to_string())),
        _ => Failure::Other(Error::Postgres(e)),
    }
}

pub struct Subscription {
    config: String,
    /// Canonical view name as stored by nabla (`nabla.status(...).name`).
    view: String,
    options: Options,
    conn: Option<Conn>,
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
            view: view.trim().to_string(),
            options,
            conn: None,
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
        // The channel is derived from the canonical name nabla stores.
        let row = client.query_one("SELECT name FROM nabla.status($1)", &[&self.view]).await?;
        self.view = row.get(0);
        client.batch_execute(&format!("LISTEN {}", quote_ident(&format!("nabla:{}", self.view)))).await?;
        self.conn = Some(Conn { client, notifications: rx });
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

    fn resync(&mut self, reason: ResyncReason) {
        self.pending.push_back((Event::Resync { reason }, None));
        self.tail.clear();
        self.state = State::NeedBootstrap;
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
                    Ok(Boot::Ready) => {}
                    Ok(Boot::Building) => tokio::time::sleep(self.options.build_poll).await,
                    Ok(Boot::Stale(reason)) => {
                        let backoff = self.stale_backoff;
                        self.stale_backoff = (self.stale_backoff * 2).min(self.options.max_backoff);
                        self.pending.push_back((Event::Resync { reason: ResyncReason::Stale { reason } }, None));
                        tokio::time::sleep(backoff).await;
                    }
                    Ok(Boot::Failed(reason)) => return Err(Error::ViewFailed(reason)),
                    Err(Failure::Disconnected) => self.conn = None,
                    Err(Failure::Other(e)) => return Err(e),
                    Err(_) => {}
                },
                State::Following => {
                    if self.tail.is_empty() && !self.wait_for_wakeup().await {
                        self.conn = None;
                        continue;
                    }
                    match self.fetch().await {
                        Ok(()) => {}
                        Err(Failure::Lagged) => self.resync(ResyncReason::Lagged),
                        Err(Failure::Stale(reason)) => self.resync(ResyncReason::Stale { reason }),
                        Err(Failure::EpochChanged { from, to }) => self.resync(ResyncReason::EpochChanged { from, to }),
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

    /// One REPEATABLE READ transaction: status, visible columns and content
    /// from the same snapshot.
    async fn bootstrap(&mut self) -> std::result::Result<Boot, Failure> {
        let keep = self.options.keep_hidden;
        let view = self.view.clone();
        let conn = self.conn.as_mut().ok_or(Failure::Other(Error::NotConnected))?;
        let result: std::result::Result<(Boot, Option<Event>), tokio_postgres::Error> = async {
            let tx = conn
                .client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await?;
            let status = tx
                .query_one(
                    "SELECT status, epoch, frontier, current_seq, stale_reason FROM nabla.status($1)",
                    &[&view],
                )
                .await?;
            let state: String = status.get(0);
            let reason: Option<String> = status.get(4);
            let reason = reason.unwrap_or_else(|| "reason not recorded".into());
            match state.as_str() {
                "initializing" | "refreshing" => {
                    tx.commit().await?;
                    return Ok((Boot::Building, None));
                }
                "failed" => {
                    tx.commit().await?;
                    return Ok((Boot::Failed(reason), None));
                }
                "stale" => {
                    tx.commit().await?;
                    return Ok((Boot::Stale(reason), None));
                }
                _ => {}
            }
            let epoch: i32 = status.get(1);
            let frontier: String = status.get(2);
            let cursor: i64 = status.get(3);
            let sql = if keep {
                format!("SELECT to_jsonb(v) FROM {} AS v", quote_view(&view))
            } else {
                let columns: Vec<String> = tx.query_one("SELECT nabla.visible_columns($1)", &[&view]).await?.get(0);
                let list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
                format!("SELECT to_jsonb(v) FROM (SELECT {} FROM {}) AS v", list.join(", "), quote_view(&view))
            };
            let rows = tx.query(&sql, &[]).await?;
            tx.commit().await?;
            let rows = rows.into_iter().map(|r| r.get::<_, Value>(0)).collect();
            Ok((Boot::Ready, Some(Event::Snapshot { epoch, frontier, cursor, rows })))
        }
        .await;
        match result {
            Ok((Boot::Ready, Some(Event::Snapshot { epoch, frontier, cursor, rows }))) => {
                self.epoch = epoch;
                self.cursor = cursor;
                self.stale_backoff = Duration::from_secs(1);
                self.state = State::Following;
                self.tail.clear();
                self.pending.push_back((Event::Snapshot { epoch, frontier, cursor, rows }, Some(cursor)));
                Ok(Boot::Ready)
            }
            Ok((outcome, _)) => Ok(outcome),
            Err(e) => Err(classify(e, self.epoch)),
        }
    }

    /// Fetch one batch after the last known row and turn complete
    /// transactions into pending events.
    async fn fetch(&mut self) -> std::result::Result<(), Failure> {
        let after = self.tail.last().map_or(self.cursor, |r| r.seq);
        let batch = self.options.batch;
        let epoch = self.epoch;
        let keep = self.options.keep_hidden;
        let conn = self.conn.as_mut().ok_or(Failure::Other(Error::NotConnected))?;
        let rows = conn
            .client
            .query(
                "SELECT seq, lsn, xid, op, row FROM nabla.changes($1, $2, $3, $4, $5)",
                &[&self.view, &after, &epoch, &batch, &keep],
            )
            .await
            .map_err(|e| classify(e, epoch))?;
        // The server returns whole transactions; a trailing one may exceed
        // `batch`, so "full" means "possibly more after this".
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
            parsed.push(ChangeRow { seq, lsn: r.get(1), xid: r.get(2), op, row: r.get(4) });
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
            // Defensive: the server promises whole transactions, but keep the
            // last group until the next call confirms nothing continues it.
            if let Some(tail) = groups.pop() {
                self.tail = tail;
            }
        }
        for group in groups {
            let last_seq = group.last().map(|r| r.seq).expect("non-empty group");
            let xid = group[0].xid;
            let lsn = group[0].lsn.clone();
            let deltas = group.into_iter().map(|r| Delta { seq: r.seq, op: r.op, row: r.row }).collect();
            self.pending.push_back((Event::Transaction { xid, lsn, epoch, deltas }, Some(last_seq)));
        }
        Ok(())
    }

    /// Read-your-writes: block until the view's frontier reaches `lsn`
    /// (the `X/Y` text form, for example `pg_current_wal_lsn()::text` taken
    /// right after a commit, or a `lsn` from a delta).
    pub async fn wait_for(&self, lsn: &str, timeout: Duration) -> Result<bool> {
        let conn = self.conn.as_ref().ok_or(Error::NotConnected)?;
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let row = conn
            .client
            .query_one("SELECT nabla.wait_for($1, $2::text, $3)", &[&self.view, &lsn, &timeout_ms])
            .await?;
        Ok(row.get(0))
    }

    /// The canonical view name (as stored by nabla).
    pub fn view(&self) -> &str {
        &self.view
    }

    pub fn cursor(&self) -> i64 {
        self.cursor
    }

    pub fn epoch(&self) -> i32 {
        self.epoch
    }
}
