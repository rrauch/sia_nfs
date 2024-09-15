-- Delete everything from the previous schema for a fresh start
DROP TRIGGER IF EXISTS capture_deleted_fs_entry_id;
DROP TABLE IF EXISTS deleted_fs_entries;
DROP TRIGGER IF EXISTS prevent_root_update;
DROP TRIGGER IF EXISTS prevent_root_deletion;
DROP TRIGGER IF EXISTS prevent_file_as_parent_update;
DROP TRIGGER IF EXISTS prevent_file_as_parent_insert;
DROP INDEX IF EXISTS idx_fs_entries_parent;
DROP TABLE IF EXISTS fs_entries;