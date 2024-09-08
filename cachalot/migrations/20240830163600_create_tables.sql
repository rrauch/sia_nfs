CREATE TABLE config
(
    chunk_size INTEGER NOT NULL CHECK (chunk_size >= 4096 AND chunk_size <= 65536)
);

CREATE TRIGGER prevent_delete_config
BEFORE DELETE ON config
BEGIN
    SELECT RAISE(ABORT, 'Deletion not allowed');
END;

INSERT INTO config (chunk_size) VALUES (4096);

CREATE TRIGGER prevent_insert_config
BEFORE INSERT ON config
BEGIN
    SELECT RAISE(ABORT, 'Insert not allowed');
END;

CREATE TABLE files
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL,
    bucket        TEXT                                  NOT NULL CHECK (LENGTH(bucket) > 0),
    path          TEXT                                  NOT NULL CHECK (LENGTH(path) > 0),
    version       INTEGER                               NOT NULL,
    num_chunks    INTEGER                     DEFAULT 0 NOT NULL,
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

CREATE TRIGGER delete_file_with_no_chunks
AFTER UPDATE OF num_chunks ON files
FOR EACH ROW
WHEN NEW.num_chunks <= 0
BEGIN
    DELETE FROM files WHERE id = NEW.id;
END;

CREATE TABLE chunks
(
    file_id       INTEGER                               NOT NULL,
    chunk_nr      INTEGER                               NOT NULL CHECK (chunk_nr >= 0),
    content_hash  BLOB                                  NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE,
    FOREIGN KEY (content_hash) REFERENCES content (content_hash) ON DELETE CASCADE,
    PRIMARY KEY (file_id, chunk_nr)
);

CREATE INDEX idx_chunks_file_id ON chunks (file_id);
CREATE INDEX idx_chunks_content_hash ON chunks (content_hash);

CREATE TRIGGER prevent_chunks_update
BEFORE UPDATE ON chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to file_id and chunk_nr columns are not allowed.')
    WHERE OLD.file_id <> NEW.file_id
       OR OLD.chunk_nr <> NEW.chunk_nr;
END;

CREATE TRIGGER increment_file_num_chunks
AFTER INSERT ON chunks
BEGIN
    UPDATE files
    SET num_chunks = num_chunks + 1
    WHERE id = NEW.file_id;
END;

CREATE TRIGGER decrement_file_num_chunks
AFTER DELETE ON chunks
BEGIN
    UPDATE files
    SET num_chunks = num_chunks - 1
    WHERE id = OLD.file_id;
END;

CREATE TABLE content
(
    content_hash    BLOB PRIMARY KEY NOT NULL CHECK (TYPEOF(content_hash) == 'blob' AND LENGTH(content_hash) == 32),
    content_length  INTEGER NOT NULL DEFAULT 0 CHECK (content_length >= 0),
    created         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_referenced TIMESTAMP DEFAULT CURRENT_TIMESTAMP  NOT NULL,
    num_chunks      INTEGER                    DEFAULT 0 NOT NULL,
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

CREATE TRIGGER delete_content_with_no_chunks
AFTER UPDATE OF num_chunks ON content
FOR EACH ROW
WHEN NEW.num_chunks <= 0
BEGIN
    DELETE FROM content WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER increment_content_num_chunks
AFTER INSERT ON chunks
BEGIN
    UPDATE content
    SET num_chunks = num_chunks + 1
    WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER decrement_content_num_chunks
AFTER DELETE ON chunks
BEGIN
    UPDATE content
    SET num_chunks = num_chunks - 1
    WHERE content_hash = OLD.content_hash;
END;

CREATE TRIGGER update_content_num_chunks
AFTER UPDATE OF content_hash ON chunks
BEGIN
    UPDATE content
    SET num_chunks = num_chunks - 1
    WHERE content_hash = OLD.content_hash;

    UPDATE content
    SET num_chunks = num_chunks + 1
    WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER update_last_referenced_after_insert
AFTER INSERT ON chunks
BEGIN
    UPDATE content
    SET last_referenced = CURRENT_TIMESTAMP
    WHERE content_hash = NEW.content_hash;
END;

CREATE TRIGGER update_last_referenced_after_update
AFTER UPDATE OF content_hash ON chunks
BEGIN
    UPDATE content
    SET last_referenced = CURRENT_TIMESTAMP
    WHERE content_hash = NEW.content_hash;
END;