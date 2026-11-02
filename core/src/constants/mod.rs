pub mod postgresql;
pub mod sqlite;

pub const TEMP_DIRECTORY: [&str; 2] = [".", "dbcTemp"];
pub const MIGRATION_ORDER_FILE_NAME: &str = "migration_order.dmb";
pub const CHECKPOINT_FILE_NAME: &str = "checkpoint.dmb";
