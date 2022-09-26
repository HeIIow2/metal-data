# sudo systemctl start mariadb.service

import json
import datetime
import mysql.connector
import requests
from bs4 import BeautifulSoup
import enum

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
    'Connection': 'keep-alive',
    'Keep-Alive': 'timeout=5, max=100'
}
METAL_API_URL = 'https://www.metal-archives.com//search/ajax-advanced/searching/bands/?' \
                'bandName={bandName}&' \
                'genre={genre}&' \
                'country{country}=&' \
                'yearCreationFrom={yearCreationFrom}&' \
                'yearCreationTo={yearCreationTo}&' \
                'bandNotes={bandNotes}&' \
                'status={status}&' \
                'themes={themes}&' \
                'location={location}&' \
                'bandLabelName={bandLabelName}&' \
                'sEcho=1&iColumns=10&sColumns=0&iDisplayStart={start}&iDisplayLength={length}'

class Band(enum.Enum):
    id = 'id'
    name = 'name'
    genre = 'genre'
    ma_url = 'ma_url'
    country = 'country'
    year_creation = 'year_creation'
    band_notes = 'band_notes'
    status = 'status'
    themes = 'themes'
    location = 'location'
    band_label_name = 'band_label_name'
    notes = 'notes'


def download_band_overview(db,
                           band_name="*",
                           genre="*",
                           country="*",
                           year_creation_from="*",
                           year_creation_to="*",
                           band_notes="*",
                           status="*",
                           themes=None,
                           location="*",
                           band_label_name="*",
                           length=200,
                           batch_download=True,
                           session=requests.Session()
                           ):
    db_cursor = db.cursor()

    bands = []
    fetched_count = length

    if themes is None:
        themes = []
    themes_string = " ".join(themes) if len(themes) > 0 else "*"

    cursor = 0
    while fetched_count == length:
        url = METAL_API_URL.format(bandName=band_name, genre=genre, country=country,
                                   yearCreationFrom=year_creation_from, yearCreationTo=year_creation_to,
                                   bandNotes=band_notes, status=status, themes=themes_string, location=location,
                                   bandLabelName=band_label_name, start=cursor, length=length)
        r = session.get(url)
        if int(r.status_code / 100) != 2:
            raise Exception('Error getting metal data: {}'.format(r.status_code))

        data = r.json()['aaData']
        fetched_count = len(data)

        if fetched_count == 0:
            print(cursor)
            print(url)
            print(r.json())

        for band in data:
            # band, genre, country, location, themes, year_creation, label, Notes

            # parse the html for the band name and link on metal-archives
            soup = BeautifulSoup(band[0], 'html.parser')
            anchor = soup.find('a')
            band_name_ = anchor.text
            ma_url_ = anchor.get('href')
            id_ = int(ma_url_.split("/")[-1])

            anchor.decompose()
            strong = soup.find('strong')
            if strong is not None:
                strong.decompose()
                akronyms_ = soup.text[2:-2].split(', ')


            # genre
            genre_ = parse_genre(band[1])

            # country
            country_ = band[2]

            # location
            location_ = band[3]

            # themes
            themes_ = band[4]

            # year_creation
            year_creation_ = None
            if band[5] != 'N/A':
                year_creation_ = datetime.date(year=int(band[5]), month=1, day=1)

            # label
            label_ = band[6]

            # notes
            notes_ = band[7]

            db_cursor.execute("DELETE FROM band WHERE id=%s;", (id_,))
            db.commit()

            sql = f"""
            INSERT INTO band 
            (id, name, url, genre, country, year_creation, band_notes, location, label) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = (id_, band_name_, ma_url_, genre_, country_, year_creation_, notes_, location_, label_)
            db_cursor.execute(sql, val)
            db.commit()

            bands.append({
                Band.id: id_,
                Band.name: band_name_,
                Band.ma_url: ma_url_,
                Band.genre: genre_,
                Band.country: country_,
                Band.location: location_,
                Band.themes: themes_,
                Band.year_creation: year_creation_,
                Band.band_notes: notes_,
                Band.band_label_name: label_,
            })
            # print(band)
        cursor += length
        print(f"{cursor}/{r.json()['iTotalRecords']}")
        if not batch_download:
            break

    return bands


def parse_distinct_genre(raw_genre: str):
    """
    top_level_genres = [
        "Metal",
        "Crossover",
        "Grindcore",
        "Rock"
    ]
    """
    """
    "-" ist das gleiche wie " ": Groove/Nu-Metal
    wenn 2 unterschiedliche top level genres vorkommen, dann wird der erste nach dem gekürzten genre genutzt: Heavy/Stoner Metal/Hard Rock
    """
    return raw_genre


def parse_genre(raw_genre: str):
    # parse genre changes over time
    """
    first nesting level: a elem for each different genres the band played
    second nesting level: a elem for each genre the band mixed
    """
    """
    genres = [

    ]

    time_periods = raw_genre.split("; ")
    for time_period in time_periods:
        time_period = re.sub(r' \(.*?\)', '', time_period)
        for genre in time_period.split(", "):
            genres.append(parse_distinct_genre(genre))
    """
    return raw_genre


def get_adjacency(db):
    db_cursor = db.cursor()
    sql = "SELECT * FROM band"
    db_cursor.execute(sql)
    bands = db_cursor.fetchall()
    for x in bands:
        print(x)


if __name__ == "__main__":
    session = requests.Session()
    session.headers = headers

    with open("src/db_credentials.json", "r", encoding="utf-8") as f:
        data = json.load(f)
        db_username = data["username"]
        db_password = data["password"]
        db_database = data["db"]

    db = mysql.connector.connect(
        host="localhost",
        user=db_username,
        password=db_password,
        database=db_database
    )

    data = download_band_overview(db, batch_download=True, session=session)
    get_adjacency(db)
