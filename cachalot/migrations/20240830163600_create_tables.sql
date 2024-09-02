CREATE TABLE files
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL,
    bucket        TEXT                                  NOT NULL CHECK (LENGTH(bucket) > 0),
    path          TEXT                                  NOT NULL CHECK (LENGTH(path) > 0),
    version       BLOB                                  NOT NULL,
    created       TIMESTAMP DEFAULT CURRENT_TIMESTAMP   NOT NULL,
    UNIQUE (bucket, path, version)
);

CREATE INDEX idx_files_bucket_path_version ON files (bucket, path, version);

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

CREATE TABLE content
(
    content_hash  BLOB PRIMARY KEY NOT NULL CHECK (TYPEOF(content_hash) == 'blob' AND LENGTH(content_hash) == 32),
    created       TIMESTAMP DEFAULT CURRENT_TIMESTAMP  NOT NULL,
    content       BLOB NOT NULL
);
