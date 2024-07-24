CREATE TABLE fs_entries
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL CHECK (id >= 1),
    entry_type    TEXT CHECK (entry_type in ('D', 'F')) NOT NULL,
    name          TEXT                                  NOT NULL CHECK (LENGTH(name) > 0 AND LENGTH(name) <= 255),
    parent        INTEGER                               NOT NULL CHECK (parent >= 1),
    FOREIGN KEY (parent) REFERENCES fs_entries (id) ON DELETE CASCADE,
    UNIQUE (parent, name)
);

CREATE INDEX IF NOT EXISTS idx_fs_entries_parent ON fs_entries (parent);

CREATE TRIGGER prevent_file_as_parent_insert
    BEFORE INSERT
    ON fs_entries
    FOR EACH ROW
    WHEN NEW.parent != 1
BEGIN
    SELECT RAISE(ABORT, 'Parent must be a directory or root')
    FROM fs_entries
    WHERE id = NEW.parent
      AND entry_type != 'D';
END;

CREATE TRIGGER prevent_file_as_parent_update
    BEFORE UPDATE
    ON fs_entries
    FOR EACH ROW
    WHEN NEW.parent != 1
BEGIN
    SELECT RAISE(ABORT, 'Parent must be a directory or root')
    FROM fs_entries
    WHERE id = NEW.parent
      AND entry_type != 'D';
END;

CREATE TRIGGER prevent_root_deletion
    BEFORE DELETE
    ON fs_entries
    FOR EACH ROW
    WHEN OLD.id = 1
BEGIN
    SELECT RAISE(ABORT, 'Cannot delete root directory');
END;

CREATE TRIGGER prevent_root_update
    BEFORE UPDATE
    ON fs_entries
    FOR EACH ROW
    WHEN OLD.id = 1
BEGIN
    SELECT RAISE(ABORT, 'Cannot update root directory');
END;

/* create the root entry */
INSERT INTO fs_entries (id, entry_type, name, parent)
VALUES (1, 'D', '_ROOT_INTERNAL_DO_NOT_USE_', 1);

/* insert and delete a temporary dummy file to increase the id */
INSERT INTO fs_entries (id, entry_type, name, parent)
VALUES (10000, 'F', 'Foo', 1);
DELETE FROM fs_entries WHERE id = 10000;
