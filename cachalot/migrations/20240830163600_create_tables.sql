CREATE TABLE files
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL CHECK (id >= 0),
    bucket        TEXT                                  NOT NULL CHECK (LENGTH(bucket) > 0),
    path          TEXT                                  NOT NULL CHECK (LENGTH(path) > 0),
    version       BLOB                                  NOT NULL,
    created       TIMESTAMP DEFAULT CURRENT_TIMESTAMP   NOT NULL,
    UNIQUE (bucket, path, version)
);

CREATE TABLE pages
(
    file_id       INTEGER                               NOT NULL CHECK (file_id >= 0),
    page_nr       INTEGER                               NOT NULL CHECK (page_nr >= 0),
    created       TIMESTAMP DEFAULT CURRENT_TIMESTAMP   NOT NULL,
    content       BLOB                                  NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE CASCADE,
    PRIMARY KEY (file_id, page_nr)
);
