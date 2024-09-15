CREATE TABLE buckets
(
  id    INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL CHECK (id >= 100 AND id < 10000),
  name  TEXT                                  NOT NULL CHECK (LENGTH(name) > 0
                                                          AND LENGTH(name) <= 255
                                                          AND LENGTH(TRIM(name)) = LENGTH(name)
                                                          AND name NOT LIKE '%/%'),
  last_sync TIMESTAMP DEFAULT '1970-01-01 00:00:00' NOT NULL,
  UNIQUE (name)
);

-- Ensure the bucket id sequence starts at 100
INSERT INTO sqlite_sequence (name, seq) VALUES ('buckets', 99);

CREATE TABLE objects
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT       NOT NULL CHECK (id >= 10000 OR id == 0),
    inode_type    TEXT                                    CHECK (inode_type IN ('D', 'F')) NOT NULL,
    name          TEXT                                    NOT NULL CHECK (LENGTH(name) > 0
                                                                          AND LENGTH(name) <= 255
                                                                          AND LENGTH(TRIM(name)) = LENGTH(name)
                                                                          AND name NOT LIKE '%/%'),
    size          INTEGER                                 NOT NULL CHECK (size >= 0),
    last_modified TIMESTAMP                               NOT NULL,
    parent        INTEGER                                 CHECK (parent IS NULL OR parent >= 10000),
    bucket        INTEGER                                 NOT NULL,
    path          TEXT NOT NULL DEFAULT '__INVALID__',
    etag          TEXT                                    CHECK (etag IS NULL OR LENGTH(etag) <= 1024),
    mime_type     TEXT                                    CHECK (mime_type IS NULL OR LENGTH(mime_type) <= 1024),
    last_sync     TIMESTAMP DEFAULT '1970-01-01 00:00:00' NOT NULL,
    FOREIGN KEY (parent) REFERENCES objects (id) ON DELETE CASCADE,
    FOREIGN KEY (bucket) REFERENCES buckets (id) ON DELETE CASCADE,
    UNIQUE (parent, name, bucket)
);

-- Ensure the object id sequence starts at 10000
INSERT INTO sqlite_sequence (name, seq) VALUES ('objects', 9999);

-- UNIQUE does not work as expected if there are NULL values involved
CREATE TRIGGER unique_root_objects
BEFORE INSERT ON objects
FOR EACH ROW
WHEN NEW.parent IS NULL
BEGIN
    SELECT RAISE(FAIL, 'Duplicate named object in bucket root')
    WHERE EXISTS (
        SELECT 1
        FROM objects
        WHERE parent IS NULL
        AND name = NEW.name
        AND bucket = NEW.bucket
    );
END;

CREATE TRIGGER unique_root_objects_update
BEFORE UPDATE OF name, parent, bucket ON objects
FOR EACH ROW
WHEN NEW.parent IS NULL
BEGIN
    SELECT RAISE(FAIL, 'Duplicate named object in bucket root')
    WHERE EXISTS (
        SELECT 1
        FROM objects
        WHERE parent IS NULL
        AND name = NEW.name
        AND bucket = NEW.bucket
        AND id != NEW.id
    );
END;

-- Prevent changing the inode_type of objects
CREATE TRIGGER prevent_inode_type_change
    BEFORE UPDATE ON objects
    FOR EACH ROW
    WHEN OLD.inode_type != NEW.inode_type
BEGIN
    SELECT RAISE(ABORT, 'Cannot change inode_type value');
END;

-- Ensure only directories can be parents (on insert)
CREATE TRIGGER ensure_directory_as_parent_on_insert
    BEFORE INSERT ON objects
    FOR EACH ROW
    WHEN NEW.parent IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'Parent must be a directory or null')
    FROM objects
    WHERE id = NEW.parent AND inode_type != 'D';
END;

-- Ensure only directories can be parents (on update)
CREATE TRIGGER ensure_directory_as_parent_on_update
    BEFORE UPDATE ON objects
    FOR EACH ROW
    WHEN NEW.parent IS NOT NULL
      AND (OLD.parent IS NULL OR NEW.parent != OLD.parent)
BEGIN
    SELECT RAISE(ABORT, 'Parent must be a directory or null')
    FROM objects
    WHERE id = NEW.parent AND inode_type != 'D';
END;

-- Prevent an object from becoming its own parent (on insert)
CREATE TRIGGER prevent_self_parent_on_insert
    BEFORE INSERT ON objects
    FOR EACH ROW
    WHEN NEW.parent = NEW.id
BEGIN
    SELECT RAISE(ABORT, 'An object cannot be its own parent');
END;

-- Prevent an object from becoming its own parent (on update)
CREATE TRIGGER prevent_self_parent_on_update
    BEFORE UPDATE ON objects
    FOR EACH ROW
    WHEN NEW.parent = NEW.id
BEGIN
    SELECT RAISE(ABORT, 'An object cannot be its own parent');
END;

-- Make sure an object cannot become its own grandparent

-- SQLite does not currently support CTEs within triggers, so this view is
-- created to recursively resolve all ancestors of an object
CREATE VIEW object_ancestors AS
WITH RECURSIVE ancestor_path(id, ancestor) AS (
  SELECT id, parent FROM objects WHERE parent IS NOT NULL
  UNION ALL
  SELECT o.id, a.ancestor
  FROM objects o
  JOIN ancestor_path a ON o.parent = a.id
)
SELECT id, ancestor FROM ancestor_path;

-- Prevent loops in the parent-child relationships
CREATE TRIGGER prevent_loops_on_update
    BEFORE UPDATE ON objects
    FOR EACH ROW
    WHEN NEW.parent IS NOT NULL
      AND (OLD.parent IS NULL OR NEW.parent != OLD.parent)
      AND NEW.parent != NEW.id
BEGIN
    SELECT RAISE(ABORT, 'Loop detected in hierarchy - don''t be your own grandparent!')
    WHERE EXISTS (
        SELECT 1
        FROM object_ancestors
        WHERE ancestor = NEW.id AND id = NEW.parent
    );
END;

-- An object's path is always updated automatically.

-- Mark the object path for recalculation on insert
CREATE TRIGGER update_object_path_on_insert
    AFTER INSERT ON objects
    FOR EACH ROW
BEGIN
    UPDATE objects
    SET path = '__RECALCULATE__'
    WHERE id = NEW.id;
END;

-- Mark the object path for recalculation if name or parent changes
CREATE TRIGGER update_object_path_on_update
    AFTER UPDATE OF name, parent ON objects
    FOR EACH ROW
    WHEN OLD.name != NEW.name
      OR OLD.parent IS NULL AND NEW.parent IS NOT NULL
      OR OLD.parent IS NOT NULL AND NEW.parent IS NULL
      OR OLD.parent != NEW.parent
BEGIN
    UPDATE objects
    SET path = '__RECALCULATE__'
    WHERE id = NEW.id;
END;

-- Recursively update the paths of marked objects
CREATE TRIGGER update_object_path_on_update_recursive
    AFTER UPDATE OF path ON objects
    FOR EACH ROW
    WHEN NEW.path = '__RECALCULATE__'
BEGIN
    UPDATE objects
    SET path = (
        SELECT CASE
            WHEN NEW.parent IS NULL THEN '/' ||
                CASE
                    WHEN NEW.inode_type = 'D' THEN NEW.name || '/'
                    ELSE NEW.name
                END
            ELSE (SELECT path FROM objects WHERE id = NEW.parent) ||
                CASE
                    WHEN NEW.inode_type = 'D' THEN NEW.name || '/'
                    ELSE NEW.name
                END
        END
    )
    WHERE id = NEW.id;

    UPDATE objects
    SET path = '__RECALCULATE__'
    WHERE parent = NEW.id AND id != NEW.id;
END;

-- A temp table to capture affected object ids
CREATE TABLE affected_objects (
    id INTEGER NOT NULL
);

CREATE TRIGGER capture_deleted_object_ids
AFTER DELETE ON objects
BEGIN
    INSERT INTO affected_objects (id) VALUES (OLD.id);
END;

CREATE TRIGGER capture_updated_object_ids
AFTER UPDATE ON objects
BEGIN
    INSERT INTO affected_objects (id) VALUES (OLD.id);
END;