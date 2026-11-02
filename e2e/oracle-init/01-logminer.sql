-- LIVE-19: LogMiner needs ARCHIVELOG mode and supplemental logging. Runs once as SYS in the
-- container root (gvenzl/oracle-free runs /container-entrypoint-initdb.d on the first start).
-- ALL columns are logged so updates arrive as complete rows.
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
