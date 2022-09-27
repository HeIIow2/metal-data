create or replace table metal.band_adjacency
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    weight INT NOT NULL,
    band_id_from BIGINT NOT NULL,
    band_id_to BIGINT NOT NULL,
    PRIMARY KEY (id)
);

create or replace table metal.theme
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    label TEXT NOT NULL,
    PRIMARY KEY (id)
);

create or replace table metal.band_theme
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    band_id BIGINT NOT NULL,
    theme_id BIGINT NOT NULL,
    PRIMARY KEY (id)
);

create or replace table metal.band
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

