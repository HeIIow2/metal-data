create table metal.band
(
    id INT NOT NULL AUTO_INCREMENT,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    country TEXT,
    year_creation DATE,
    band_notes TEXT,
    status TEXT,
    themes TEXT,
    location TEXT,
    label TEXT,
    akronyms TEXT,
    notes TEXT,
    PRIMARY KEY (id)
);

