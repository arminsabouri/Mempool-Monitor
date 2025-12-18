use crate::now;
use anyhow::Result;
use std::time::SystemTime;

pub(crate) trait Migration {
    fn migrate(&self, conn: &rusqlite::Connection) -> Result<()>;
    fn id(&self) -> &'static str;
}

pub(crate) struct UpdateChildTxidColName;

impl Migration for UpdateChildTxidColName {
    fn id(&self) -> &'static str {
        "update_child_txid_col_name"
    }

    fn migrate(&self, conn: &rusqlite::Connection) -> Result<()> {
        // The parent_txid column was renamed to child_txid
        conn.execute(
            "ALTER TABLE transactions RENAME COLUMN parent_txid TO child_txid",
            [],
        )?;

        let applied_at = now!().to_string();
        conn.execute(
            "INSERT INTO migrations (id, applied_at) VALUES (?1, ?2)",
            [self.id(), &applied_at],
        )?;
        Ok(())
    }
}

pub(crate) struct AddTxNotSeenInMempool;

impl Migration for AddTxNotSeenInMempool {
    fn id(&self) -> &'static str {
        "add_tx_not_seen_in_mempool"
    }

    fn migrate(&self, conn: &rusqlite::Connection) -> Result<()> {
        conn.execute(
            "ALTER TABLE transactions ADD COLUMN seen_in_mempool BOOLEAN NOT NULL DEFAULT TRUE",
            [],
        )?;

        let applied_at = now!().to_string();
        conn.execute(
            "INSERT INTO migrations (id, applied_at) VALUES (?1, ?2)",
            [self.id(), &applied_at],
        )?;
        Ok(())
    }
}

pub(crate) struct AddReplacementTxid;

impl Migration for AddReplacementTxid {
    fn id(&self) -> &'static str {
        "add_replacement_txid"
    }

    fn migrate(&self, conn: &rusqlite::Connection) -> Result<()> {
        conn.execute("ALTER TABLE rbf ADD COLUMN replaces TEXT", [])?;

        let applied_at = now!().to_string();
        conn.execute(
            "INSERT INTO migrations (id, applied_at) VALUES (?1, ?2)",
            [self.id(), &applied_at],
        )?;
        Ok(())
    }
}

pub(crate) struct AddIsCpfpParent;

impl Migration for AddIsCpfpParent {
    fn id(&self) -> &'static str {
        "parent_txid"
    }

    fn migrate(&self, conn: &rusqlite::Connection) -> Result<()> {
        conn.execute("ALTER TABLE transactions ADD COLUMN parent_txid TEXT", [])?;

        let applied_at = now!().to_string();
        conn.execute(
            "INSERT INTO migrations (id, applied_at) VALUES (?1, ?2)",
            [self.id(), &applied_at],
        )?;
        Ok(())
    }
}

fn already_applied(conn: &rusqlite::Connection, migration: &str) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM migrations WHERE id = ?")?;
    let count: i32 = stmt.query_row([migration], |row| row.get(0))?;
    Ok(count > 0)
}

pub(crate) fn run_migrations(conn: &rusqlite::Connection) -> Result<()> {
    let migrations: Vec<Box<dyn Migration>> = vec![
        Box::new(UpdateChildTxidColName),
        Box::new(AddTxNotSeenInMempool),
        Box::new(AddReplacementTxid),
        Box::new(AddIsCpfpParent),
    ];
    for migration in migrations {
        if already_applied(conn, migration.id())? {
            continue;
        }
        migration.migrate(conn)?;
    }
    Ok(())
}
