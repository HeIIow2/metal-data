create table metal.band
(
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    genre TEXT,
    country TEXT,
    year_creation DATE,
    band_notes TEXT,
    status TEXT,
    themes TEXT,
    location TEXT,
    label TEXT,
    notes TEXT,
    PRIMARY KEY (id)
);

