//! Oracle live mode (LIVE-19): LogMiner (`DBMS_LOGMNR`, dictionary from the online catalog) over
//! the redo / archive logs. Every poll mines the SCN window from the start of the oldest
//! transaction that was open at the last checkpoint up to the current SCN, with
//! `COMMITTED_DATA_ONLY`, so changes arrive per committed transaction in commit order; only
//! transactions committed after the checkpoint are returned. `SQL_REDO` is parsed into change
//! events (the session NLS formats are fixed in `oracle_connection`).
//!
//! Needs `ARCHIVELOG` mode and supplemental logging (at least minimal + primary key; with
//! `ALL` columns logged, updates arrive as complete rows instead of key + changed columns).

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime};
use log::warn;
use tokio::sync::watch;

use crate::{
    migrator::migrator_steps::ChangeSource,
    models::{
        change::{ChangeEvent, ChangeOp, Checkpoint},
        data_definitions::{short_name, DataDefinition},
        value::{from_hex, Value},
    },
    DMError, DMResult,
};

use super::oracle_connection::Oracle;

pub struct OracleCdcState {
    /// `TABLE_NAME` -> definition of the streamed tables of the schema
    tables: HashMap<String, DataDefinition>,
    /// Mining window start (applied state)
    start_scn: u64,
    /// Commit SCN up to which every transaction was applied
    last_commit: u64,
    /// (start_scn, last_commit) after the events returned by the last poll are applied
    pending: Option<(u64, u64)>,
    /// LogMiner in a PDB finds the log files itself; in a non-CDB / the root they are added
    in_pdb: bool,
}

const PREREQUISITES: &str = "SELECT log_mode, supplemental_log_data_min, supplemental_log_data_pk, supplemental_log_data_all, cdb, SYS_CONTEXT('USERENV', 'CON_NAME') FROM v$database";
const POSITION: &str = "SELECT d.current_scn, (SELECT MIN(t.start_scn) FROM v$transaction t) FROM v$database d";
const ONLINE_LOGS: &str = "SELECT MIN(f.member) FROM v$log l JOIN v$logfile f ON f.group# = l.group# \
     WHERE l.status <> 'UNUSED' AND l.next_change# > :1 GROUP BY l.group#";
const ARCHIVED_LOGS: &str = "SELECT MIN(a.name) FROM v$archived_log a WHERE a.next_change# > :1 AND a.first_change# <= :2 \
     AND a.name IS NOT NULL AND a.deleted = 'NO' AND a.standby_dest = 'NO' \
     AND NOT EXISTS (SELECT 1 FROM v$log l WHERE l.sequence# = a.sequence# AND l.thread# = a.thread# AND l.status <> 'UNUSED') \
     GROUP BY a.thread#, a.sequence#";
const START: &str = "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, ENDSCN => :2, OPTIONS => \
     DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.NO_ROWID_IN_STMT); END;";
const CONTENTS: &str = "SELECT commit_scn, table_name, sql_redo, csf, commit_timestamp FROM v$logmnr_contents \
     WHERE seg_owner = :1 AND operation_code IN (1, 2, 3) AND commit_scn > :2";

/// A literal of a `SQL_REDO` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Lit {
    Value(Value),
    /// LOB placeholders (`EMPTY_CLOB()`) and functions not understood: column left out
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Redo {
    Insert(Vec<(String, Lit)>),
    Update { set: Vec<(String, Lit)>, condition: Vec<(String, Lit)> },
    Delete(Vec<(String, Lit)>),
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// `"quoted"` identifier
    Ident(String),
    /// `'text'`
    Str(String),
    /// keyword, function name or unquoted number (upper case)
    Word(String),
    Punct(char),
}

fn tokenize(sql: &str) -> DMResult<Vec<Tok>> {
    let bad = || DMError::Error(format!("cannot parse LogMiner SQL_REDO: {sql}"));
    let mut out = Vec::new();
    let mut it = sql.chars().peekable();
    while let Some(&c) = it.peek() {
        match c {
            c if c.is_whitespace() => {
                it.next();
            }
            '"' | '\'' => {
                it.next();
                let mut s = String::new();
                loop {
                    match it.next().ok_or_else(bad)? {
                        q if q == c && it.peek() == Some(&c) => {
                            it.next();
                            s.push(c);
                        }
                        q if q == c => break,
                        other => s.push(other),
                    }
                }
                out.push(if c == '"' { Tok::Ident(s) } else { Tok::Str(s) });
            }
            '(' | ')' | ',' | '=' | ';' | '.' => {
                it.next();
                out.push(Tok::Punct(c));
            }
            _ => {
                let mut s = String::new();
                while let Some(&c) = it.peek() {
                    let number_part = (c == '.' || c == '+' || c == '-') && s.chars().next().is_some_and(|f| f.is_ascii_digit() || f == '-');
                    if c.is_alphanumeric() || matches!(c, '_' | '$' | '#') || number_part || (s.is_empty() && c == '-') {
                        s.push(c);
                        it.next();
                    } else {
                        break;
                    }
                }
                if s.is_empty() {
                    return Err(bad());
                }
                out.push(Tok::Word(s.to_uppercase()));
            }
        }
    }
    Ok(out)
}

/// `\00e9` escapes of `UNISTR('..')` (surrogate pairs included).
fn unistr(s: &str) -> String {
    let mut units: Vec<u16> = Vec::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if chars.peek() == Some(&'\\') {
                chars.next();
                units.push('\\' as u16);
                continue;
            }
            let hex: String = chars.by_ref().take(4).collect();
            if let Ok(u) = u16::from_str_radix(&hex, 16) {
                units.push(u);
                continue;
            }
        }
        let mut buf = [0u16; 2];
        units.extend_from_slice(c.encode_utf16(&mut buf));
    }
    String::from_utf16_lossy(&units)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn word(&mut self, w: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Word(x)) if x == w) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn punct(&mut self, p: char) -> bool {
        if self.peek() == Some(&Tok::Punct(p)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// `"SCHEMA"."TABLE"`
    fn table(&mut self) -> Option<()> {
        matches!(self.next()?, Tok::Ident(_)).then_some(())?;
        if self.punct('.') {
            matches!(self.next()?, Tok::Ident(_)).then_some(())?;
        }
        Some(())
    }

    fn ident(&mut self) -> Option<String> {
        match self.next()? {
            Tok::Ident(s) => Some(s),
            _ => None,
        }
    }

    /// Arguments of a function call after its `(`, up to the matching `)`; nested calls make the
    /// result unknown.
    fn args(&mut self) -> Option<Option<Vec<String>>> {
        let mut args = Vec::new();
        let mut known = true;
        let mut depth = 1;
        loop {
            match self.next()? {
                Tok::Punct('(') => {
                    depth += 1;
                    known = false;
                }
                Tok::Punct(')') => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(known.then_some(args));
                    }
                }
                Tok::Str(s) | Tok::Word(s) if depth == 1 => args.push(s),
                _ => {}
            }
        }
    }

    fn literal(&mut self) -> Option<Lit> {
        Some(match self.next()? {
            Tok::Str(s) => Lit::Value(Value::String(s)),
            Tok::Word(w) if w == "NULL" => Lit::Value(Value::Null),
            Tok::Word(w) if w == "TRUE" || w == "FALSE" => Lit::Value(Value::Bool(w == "TRUE")),
            Tok::Word(w) if self.punct('(') => {
                let Some(args) = self.args()? else { return Some(Lit::Unknown) };
                let first = args.first().cloned().unwrap_or_default();
                let dt = |f: &str| NaiveDateTime::parse_from_str(&first, f).ok().map(|t| Lit::Value(Value::DateTime(t)));
                match w.as_str() {
                    "HEXTORAW" => from_hex(&first).map(|b| Lit::Value(Value::Bytes(b))).unwrap_or(Lit::Unknown),
                    "TO_DATE" => dt("%Y-%m-%d %H:%M:%S").unwrap_or(Lit::Unknown),
                    "TO_TIMESTAMP" => dt("%Y-%m-%d %H:%M:%S%.f").unwrap_or(Lit::Unknown),
                    "TO_TIMESTAMP_TZ" => DateTime::parse_from_str(&first, "%Y-%m-%d %H:%M:%S%.f %:z")
                        .map(|t| Lit::Value(Value::DateTimeTz(t.to_utc())))
                        .unwrap_or(Lit::Unknown),
                    "UNISTR" => Lit::Value(Value::String(unistr(&first))),
                    "TO_NUMBER" | "TO_BINARY_FLOAT" | "TO_BINARY_DOUBLE" | "TO_DSINTERVAL" | "TO_YMINTERVAL" => Lit::Value(Value::String(first)),
                    _ => Lit::Unknown,
                }
            }
            // unquoted numbers (BINARY_DOUBLE `1.5E+000`)
            Tok::Word(w) => Lit::Value(Value::String(w)),
            _ => return None,
        })
    }

    /// `"A" = lit AND "B" IS NULL ...`
    fn conditions(&mut self) -> Option<Vec<(String, Lit)>> {
        let mut out = Vec::new();
        loop {
            match self.peek()? {
                Tok::Ident(_) => {
                    let col = self.ident()?;
                    if self.word("IS") {
                        self.word("NULL").then_some(())?;
                        out.push((col, Lit::Value(Value::Null)));
                    } else {
                        self.punct('=').then_some(())?;
                        out.push((col, self.literal()?));
                    }
                }
                _ => return None,
            }
            if !self.word("AND") {
                return Some(out);
            }
        }
    }

    fn parse(&mut self) -> Option<Redo> {
        if self.word("INSERT") {
            self.word("INTO").then_some(())?;
            self.table()?;
            self.punct('(').then_some(())?;
            let mut cols = Vec::new();
            loop {
                cols.push(self.ident()?);
                if !self.punct(',') {
                    break;
                }
            }
            self.punct(')').then_some(())?;
            self.word("VALUES").then_some(())?;
            self.punct('(').then_some(())?;
            let mut values = Vec::with_capacity(cols.len());
            loop {
                values.push(self.literal()?);
                if !self.punct(',') {
                    break;
                }
            }
            self.punct(')').then_some(())?;
            (cols.len() == values.len()).then(|| Redo::Insert(cols.into_iter().zip(values).collect()))
        } else if self.word("UPDATE") {
            self.table()?;
            self.word("SET").then_some(())?;
            let mut set = Vec::new();
            loop {
                let col = self.ident()?;
                self.punct('=').then_some(())?;
                set.push((col, self.literal()?));
                if !self.punct(',') {
                    break;
                }
            }
            let condition = if self.word("WHERE") { self.conditions()? } else { Vec::new() };
            Some(Redo::Update { set, condition })
        } else if self.word("DELETE") {
            self.word("FROM").then_some(())?;
            self.table()?;
            self.word("WHERE").then_some(())?;
            Some(Redo::Delete(self.conditions()?))
        } else {
            None
        }
    }
}

/// Parses one (complete) `SQL_REDO` statement.
pub fn parse_redo(sql: &str) -> DMResult<Redo> {
    let mut p = Parser { toks: tokenize(sql)?, pos: 0 };
    p.parse().ok_or_else(|| DMError::Error(format!("cannot parse LogMiner SQL_REDO: {sql}")))
}

fn known(pairs: Vec<(String, Lit)>) -> (Vec<(String, Value)>, bool) {
    let mut all = true;
    let row = pairs
        .into_iter()
        .filter_map(|(c, l)| match l {
            Lit::Value(v) => Some((c, v)),
            Lit::Unknown => {
                all = false;
                None
            }
        })
        .collect();
    (row, all)
}

/// Change events of one statement. An update builds the new row from the old values in the
/// WHERE clause (supplemental logging) and the SET values; it is complete only when every
/// column is known. A changed key becomes a delete of the old key and an upsert of the new.
pub fn redo_events(def: &DataDefinition, redo: Redo, source_time: Option<DateTime<chrono::Utc>>) -> Vec<ChangeEvent> {
    let ev = |op, row, complete| ChangeEvent { container: def.container_name.clone(), op, row, source_time, complete };
    let covers = |row: &[(String, Value)]| def.properties_info.iter().all(|p| row.iter().any(|(c, _)| *c == p.property_name));
    match redo {
        Redo::Insert(cols) => {
            let (row, all) = known(cols);
            vec![ev(ChangeOp::Upsert, row, all)]
        }
        Redo::Delete(cond) => vec![ev(ChangeOp::Delete, known(cond).0, false)],
        Redo::Update { set, condition } => {
            let (old, _) = known(condition);
            let (set, all_set) = known(set);
            let mut new = old.clone();
            for (c, v) in set {
                match new.iter_mut().find(|(n, _)| *n == c) {
                    Some(slot) => slot.1 = v,
                    None => new.push((c, v)),
                }
            }
            let ids = def.identifier_names();
            let key = |row: &[(String, Value)]| -> Option<Vec<Value>> { ids.iter().map(|k| row.iter().find(|(c, _)| c == k).map(|(_, v)| v.clone())).collect() };
            let complete = all_set && covers(&new);
            let mut out = Vec::with_capacity(2);
            if let (Some(before), Some(after)) = (key(&old), key(&new)) {
                if before != after {
                    out.push(ev(ChangeOp::Delete, old.clone(), false));
                }
            }
            out.push(ev(ChangeOp::Upsert, new, complete));
            out
        }
    }
}

/// One LogMiner session over `[start, end]`: events of transactions committed after `after`, in
/// commit order. Stops at a commit boundary after about `max` events and then returns the commit
/// SCN of the last transaction included.
/// SCN window of one mining run.
struct Window {
    start: u64,
    end: u64,
    /// commit SCN already applied
    after: u64,
    max: usize,
}

fn mine(c: &oracle::Connection, schema: &str, in_pdb: bool, tables: &HashMap<String, DataDefinition>, w: Window) -> DMResult<(Vec<ChangeEvent>, Option<u64>)> {
    let Window { start, end, after, max } = w;
    if !in_pdb {
        let mut files = Vec::new();
        for r in c.query_as::<String>(ARCHIVED_LOGS, &[&start, &end])? {
            files.push(r?);
        }
        for r in c.query_as::<String>(ONLINE_LOGS, &[&start])? {
            files.push(r?);
        }
        for (i, f) in files.iter().enumerate() {
            let option = if i == 0 { "NEW" } else { "ADDFILE" };
            c.execute(&format!("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => DBMS_LOGMNR.{option}); END;"), &[f])?;
        }
    }
    c.execute(START, &[&start, &end])?;
    let result = (|| -> DMResult<(Vec<ChangeEvent>, Option<u64>)> {
        let mut events = Vec::new();
        let mut redo = String::new();
        let mut last: Option<u64> = None;
        for row in c.query_as::<(u64, String, Option<String>, i64, Option<NaiveDateTime>)>(CONTENTS, &[&schema, &after])? {
            let (commit, table, sql, csf, time) = row?;
            if events.len() >= max && last.is_some_and(|l| l != commit) && redo.is_empty() {
                return Ok((events, last));
            }
            redo.push_str(sql.as_deref().unwrap_or(""));
            // CSF = 1: the statement continues in the next row
            if csf == 1 {
                continue;
            }
            let statement = std::mem::take(&mut redo);
            last = Some(commit);
            let Some(def) = tables.get(&table) else { continue };
            match parse_redo(&statement) {
                Ok(r) => events.extend(redo_events(def, r, time.map(|t| t.and_utc()))),
                Err(e) => warn!("[Live] {e}"),
            }
        }
        Ok((events, None))
    })();
    let _ = c.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;", &[]);
    result
}

impl Oracle {
    async fn position(&self) -> DMResult<(u64, Option<u64>)> {
        self.blocking(|c| Ok(c.query_row_as::<(u64, Option<u64>)>(POSITION, &[])?)).await
    }
}

#[async_trait]
impl ChangeSource for Oracle {
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()> {
        self.end_read_tx().await?;
        let (log_mode, min, pk, all, cdb, con) = self.blocking(|c| Ok(c.query_row_as::<(String, String, String, String, String, String)>(PREREQUISITES, &[])?)).await?;
        if log_mode != "ARCHIVELOG" {
            return Err(DMError::Error("Oracle live mode needs ARCHIVELOG mode (SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN)".into()));
        }
        if min == "NO" {
            return Err(DMError::Error("Oracle live mode needs supplemental logging: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS".into()));
        }
        if pk == "NO" && all == "NO" {
            warn!("[Live] no database-wide PRIMARY KEY / ALL supplemental logging: updates need table-level supplemental logging of the key columns");
        }
        let schema = self.schema.clone();
        let tables = defs.iter().filter(|d| d.container_name.split_once('.').map(|(s, _)| s) == Some(schema.as_str())).map(|d| (short_name(&d.container_name).to_string(), d.clone())).collect();
        let (start_scn, last_commit) = match resume {
            Some(Checkpoint::Oracle { scn, start_scn }) => (*start_scn, *scn),
            _ => {
                let (current, oldest) = self.position().await?;
                (oldest.unwrap_or(current).min(current), current)
            }
        };
        self.cdc = Some(OracleCdcState { tables, start_scn, last_commit, pending: None, in_pdb: cdb == "YES" && con != "CDB$ROOT" });
        Ok(())
    }

    async fn poll_changes(&mut self, max: usize, wait: Duration, shutdown: &watch::Receiver<bool>) -> DMResult<Vec<ChangeEvent>> {
        // the snapshot (LIVE-4) ran in a read-only transaction; LogMiner calls cannot
        self.end_read_tx().await?;
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if *shutdown.borrow() {
                return Ok(Vec::new());
            }
            let (current, oldest) = self.position().await?;
            let state = self.cdc.as_ref().ok_or_else(|| DMError::Error("capture not started".into()))?;
            let (start, after, in_pdb, tables) = (state.start_scn, state.last_commit, state.in_pdb, state.tables.clone());
            let schema = self.schema.clone();
            let (events, cut) = self.blocking(move |c| mine(c, &schema, in_pdb, &tables, Window { start, end: current, after, max: max.max(1) })).await?;
            let next = match cut {
                // stopped early: the same window again, after the last included commit
                Some(last) => (start, last),
                None => (oldest.unwrap_or(current).min(current), current),
            };
            let state = self.cdc.as_mut().expect("checked above");
            if !events.is_empty() {
                state.pending = Some(next);
                return Ok(events);
            }
            // nothing to apply in this window: safe to move on at once
            (state.start_scn, state.last_commit) = next;
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(Vec::new());
            }
            tokio::time::sleep((deadline - now).min(Duration::from_millis(500))).await;
        }
    }

    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>> {
        let Some(state) = self.cdc.as_mut() else { return Ok(None) };
        if let Some((start, last)) = state.pending.take() {
            (state.start_scn, state.last_commit) = (start, last);
        }
        Ok(Some(Checkpoint::Oracle { scn: state.last_commit, start_scn: state.start_scn }))
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Oracle::reconnect(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{data_definitions::DataPropertyInfo, value::LogicalType};

    fn def() -> DataDefinition {
        let p = |n: &str, id: bool| DataPropertyInfo {
            property_name: n.into(),
            data_type: LogicalType::String,
            source_type: None,
            is_nullable: !id,
            is_identifier: id,
            reference_container_name: None,
            reference_property_name: None,
            shape: None,
        };
        DataDefinition::new("DM.CUSTOMERS".into(), vec![p("ID", true), p("NAME", false), p("NOTE", false)])
    }

    fn s(v: &str) -> Lit {
        Lit::Value(Value::String(v.into()))
    }

    #[test]
    fn live19_parses_insert_with_oracle_literals() {
        let r = parse_redo(
            "insert into \"DM\".\"T\"(\"ID\",\"NAME\",\"D\",\"TS\",\"TZ\",\"IMG\",\"U\",\"F\",\"N\",\"DOC\") values \
             ('1','Bob O''Brien',TO_DATE('2024-02-29 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('2024-03-07 18:45:30.500000000'),\
             TO_TIMESTAMP_TZ('2024-02-29 23:59:59.123000000 +02:00'),HEXTORAW('89504e47'),UNISTR('\\017b\\00f3\\0142\\0107 \\d83d\\de80'),1.5E+000,NULL,EMPTY_CLOB());",
        )
        .unwrap();
        let Redo::Insert(cols) = r else { panic!() };
        let v: Vec<Lit> = cols.iter().map(|(_, l)| l.clone()).collect();
        assert_eq!(cols[1].0, "NAME");
        assert_eq!(v[0], s("1"));
        assert_eq!(v[1], s("Bob O'Brien"));
        assert_eq!(v[2], Lit::Value(Value::DateTime("2024-02-29T23:59:59".parse().unwrap())));
        assert_eq!(v[3], Lit::Value(Value::DateTime("2024-03-07T18:45:30.5".parse().unwrap())));
        assert_eq!(v[4], Lit::Value(Value::DateTimeTz("2024-02-29T21:59:59.123Z".parse().unwrap())));
        assert_eq!(v[5], Lit::Value(Value::Bytes(vec![0x89, 0x50, 0x4e, 0x47])));
        assert_eq!(v[6], s("Żółć 🚀"));
        assert_eq!(v[7], s("1.5E+000"));
        assert_eq!(v[8], Lit::Value(Value::Null));
        assert_eq!(v[9], Lit::Unknown);
    }

    #[test]
    fn live19_parses_update_and_delete() {
        let r = parse_redo("update \"DM\".\"CUSTOMERS\" set \"NAME\" = 'Anna K.', \"NOTE\" = NULL where \"ID\" = '1' and \"NAME\" = 'Anna' and \"NOTE\" IS NULL;").unwrap();
        assert_eq!(
            r,
            Redo::Update {
                set: vec![("NAME".into(), s("Anna K.")), ("NOTE".into(), Lit::Value(Value::Null))],
                condition: vec![("ID".into(), s("1")), ("NAME".into(), s("Anna")), ("NOTE".into(), Lit::Value(Value::Null))],
            }
        );
        let r = parse_redo("delete from \"DM\".\"CUSTOMERS\" where \"ID\" = '-7';").unwrap();
        assert_eq!(r, Redo::Delete(vec![("ID".into(), s("-7"))]));
        assert!(parse_redo("set transaction read write;").is_err());
        assert!(parse_redo("insert into \"DM\".\"T\"(\"A\") values ('unterminated").is_err());
    }

    #[test]
    fn live19_update_events() {
        let d = def();
        // ALL columns logged: complete row
        let r = parse_redo("update \"DM\".\"CUSTOMERS\" set \"NAME\" = 'B' where \"ID\" = '1' and \"NAME\" = 'A' and \"NOTE\" IS NULL;").unwrap();
        let ev = redo_events(&d, r, None);
        assert_eq!(ev.len(), 1);
        assert!(ev[0].complete && ev[0].op == ChangeOp::Upsert);
        assert_eq!(ev[0].row[1], ("NAME".to_string(), Value::String("B".into())));
        // key + changed columns only: partial
        let r = parse_redo("update \"DM\".\"CUSTOMERS\" set \"NAME\" = 'B' where \"ID\" = '1' and \"NAME\" = 'A';").unwrap();
        assert!(!redo_events(&d, r, None)[0].complete);
        // key changed: delete old key, then upsert the new row
        let r = parse_redo("update \"DM\".\"CUSTOMERS\" set \"ID\" = '60' where \"ID\" = '6' and \"NAME\" IS NULL and \"NOTE\" IS NULL;").unwrap();
        let ev = redo_events(&d, r, None);
        assert_eq!(ev.iter().map(|e| e.op).collect::<Vec<_>>(), [ChangeOp::Delete, ChangeOp::Upsert]);
        assert_eq!(ev[0].row[0].1, Value::String("6".into()));
        assert_eq!(ev[1].row[0].1, Value::String("60".into()));
        assert!(ev[1].complete);
        // an insert with a LOB placeholder is not complete
        let r = parse_redo("insert into \"DM\".\"CUSTOMERS\"(\"ID\",\"NAME\",\"NOTE\") values ('2','x',EMPTY_CLOB());").unwrap();
        let ev = redo_events(&d, r, None);
        assert!(!ev[0].complete);
        assert_eq!(ev[0].row.len(), 2);
    }
}
