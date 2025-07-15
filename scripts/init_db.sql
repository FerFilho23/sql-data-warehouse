
-- =============================================================
-- WARNING:
-- This script will DROP and recreate the databases 'bronze', 'silver', and 'gold'.
-- All data in these databases will be permanently deleted.
-- Ensure you have backups before executing.
-- =============================================================

DROP DATABASE IF EXISTS bronze;
CREATE DATABASE bronze;

DROP DATABASE IF EXISTS silver;
CREATE DATABASE silver;

DROP DATABASE IF EXISTS gold;
CREATE DATABASE gold;