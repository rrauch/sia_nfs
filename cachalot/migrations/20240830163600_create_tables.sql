CREATE TABLE files
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL,
    bucket        TEXT                                  NOT NULL CHECK (LENGTH(bucket) > 0),
    path          TEXT                                  NOT NULL CHECK (LENGTH(path) > 0),
    version       INTEGER                               NOT NULL,
    num_pages     INTEGER                     DEFAULT 0 NOT NULL,
    UNIQUE (bucket, path, version)
);

CREATE INDEX idx_files_bucket_path_version ON files (bucket, path, version);

CREATE TRIGGER prevent_files_update
BEFORE UPDATE ON files
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to id, bucket, path, and version columns are not allowed.')
    WHERE OLD.id <> NEW.id
       OR OLD.bucket <> NEW.bucket
       OR OLD.path <> NEW.path
       OR OLD.version <> NEW.version;
END;

CREATE TRIGGER delete_file_with_no_pages
AFTER UPDATE OF num_pages ON files
FOR EACH ROW
WHEN NEW.num_pages <= 0
BEGIN
    DELETE FROM files WHERE id = NEW.id;
END;

CREATE TABLE pages
(
    file_id       INTEGER                               NOT NULL,
    page_nr       INTEGER                               NOT NULL CHECK (page_nr >= 0),
    content_hash  BLOB                                  NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE,
    FOREIGN KEY (content_hash) REFERENCES content (content_hash) ON DELETE CASCADE,
    PRIMARY KEY (file_id, page_nr)
);

CREATE INDEX idx_pages_file_id ON pages (file_id);
CREATE INDEX idx_pages_content_hash ON pages (content_hash);

CREATE TRIGGER prevent_pages_update
BEFORE UPDATE ON pages
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to file_id and page_nr columns are not allowed.')
    WHERE OLD.file_id <> NEW.file_id
       OR OLD.page_nr <> NEW.page_nr;
END;

CREATE TRIGGER increment_file_num_pages
AFTER INSERT ON pages
BEGIN
    UPDATE files
    SET num_pages = num_pages + 1
    WHERE id = NEW.file_id;
END;

CREATE TRIGGER decrement_file_num_pages
AFTER DELETE ON pages
BEGIN
    UPDATE files
    SET num_pages = num_pages - 1
    WHERE id = OLD.file_id;
END;

CREATE TABLE content
(
    content_hash    BLOB PRIMARY KEY NOT NULL CHECK (TYPEOF(content_hash) == 'blob' AND LENGTH(content_hash) == 32),
    content_length  INTEGER NOT NULL DEFAULT 0 CHECK (content_length >= 0),
    created         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_referenced TIMESTAMP DEFAULT CURRENT_TIMESTAMP  NOT NULL,
    num_pages       INTEGER                    DEFAULT 0 NOT NULL,
    content         BLOB NOT NULL CHECK (TYPEOF(content) == 'blob')
);

CREATE TRIGGER prevent_content_update
BEFORE UPDATE ON content
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to content_hash and content columns are not allowed.')
    WHERE OLD.content_hash <> NEW.content_hash
       OR OLD.content <> NEW.content;
END;

CREATE TRIGGER set_content_length_after_insert
AFTER INSERT ON content
FOR EACH ROW
BEGIN
    UPDATE content
    SET content_length = LENGTH(NEW.content)
    WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER delete_content_with_no_pages
AFTER UPDATE OF num_pages ON content
FOR EACH ROW
WHEN NEW.num_pages <= 0
BEGIN
    DELETE FROM content WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER increment_content_num_pages
AFTER INSERT ON pages
BEGIN
    UPDATE content
    SET num_pages = num_pages + 1
    WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER decrement_content_num_pages
AFTER DELETE ON pages
BEGIN
    UPDATE content
    SET num_pages = num_pages - 1
    WHERE content_hash = OLD.content_hash;
END;

CREATE TRIGGER update_content_num_pages
AFTER UPDATE OF content_hash ON pages
BEGIN
    UPDATE content
    SET num_pages = num_pages - 1
    WHERE content_hash = OLD.content_hash;

    UPDATE content
    SET num_pages = num_pages + 1
    WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER update_last_referenced_after_insert
AFTER INSERT ON pages
BEGIN
    UPDATE content
    SET last_referenced = CURRENT_TIMESTAMP
    WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER update_last_referenced_after_update
AFTER UPDATE OF content_hash ON pages
BEGIN
    UPDATE content
    SET last_referenced = CURRENT_TIMESTAMP
    WHERE content_hash = NEW.content_hash;
END;

CREATE TABLE content_stats
(
    total_size INTEGER DEFAULT 0 NOT NULL CHECK (total_size >= 0)
);

INSERT INTO content_stats (total_size) VALUES (0);

CREATE TRIGGER update_content_stats_insert
AFTER INSERT ON content
BEGIN
    UPDATE content_stats
    SET total_size = total_size + LENGTH(NEW.content);
END;

CREATE TRIGGER update_content_stats_delete
AFTER DELETE ON content
BEGIN
    UPDATE content_stats
    SET total_size = total_size - LENGTH(OLD.content);
END;