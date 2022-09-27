# sudo systemctl start mariadb.service

"""
SELECT id, name as label, url, genre, country, location, year_creation, band_notes, label as record_label FROM band
SELECT band_id_from as source, band_id_to as target, weight FROM band_adjacency

get all Metal Themes ordered by popularity
SELECT theme.label, COUNT(band_theme.id) FROM metal.theme, metal.band_theme WHERE band_theme.theme_id=theme.id GROUP BY band_theme.theme_id ORDER BY COUNT(band_theme.id);

3540373143
"""

import json
import datetime
import mysql.connector
import requests
from bs4 import BeautifulSoup
import enum

VSCODE = True




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
                           get_all=False,
                           session=requests.Session(),
                           skip_present=True
                           ):

    if themes is None:
        themes = []
    themes_string = " ".join(themes) if len(themes) > 0 else "*"

    if get_all:
        band_name=""
        genre=""
        country=""
        year_creation_from=""
        year_creation_to=""
        band_notes=""
        status=""
        location=""
        band_label_name=""
        themes_string=""

    metal_api_url = 'https://www.metal-archives.com//search/ajax-advanced/searching/bands/?' \
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

    db_cursor = db.cursor()

    bands = []
    fetched_count = length

    faulty_ids = []

    cursor = 0
    while fetched_count == length:
        url = metal_api_url.format(bandName=band_name, genre=genre, country=country,
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

            country_ = None
            location_ = None
            themes_ = None
            year_creation_ = None
            label_ = None
            notes_ = None

            if not get_all:
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

            else:
                genre_ = band[1]
                country_ = band[2]

            if skip_present:
                db_cursor.execute("SELECT id FROM band WHERE id=%s;", (id_,))
                if len(db_cursor.fetchall()) > 0:
                    continue
            else:
                db_cursor.execute("DELETE FROM band WHERE id=%s;", (id_,))
                db.commit()

            sql = f"""
            INSERT INTO band 
            (id, name, url, genre, country, year_creation, band_notes, location, label, themes) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            val = (id_, band_name_, ma_url_, genre_, country_, year_creation_, notes_, location_, label_, themes_)
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


def get_adjacency(db, session=requests.Session()):
    endpoint_preset = "https://www.metal-archives.com/band/ajax-recommendations/id/{}/showMoreSimilar/1"

    db_cursor = db.cursor()
    sql = "SELECT band.id, band.name FROM band WHERE band.id NOT IN (SELECT band_id_from FROM band_adjacency)"
    db_cursor.execute(sql)
    bands = db_cursor.fetchall()
    unconnected_bands = 0

    # db_cursor.execute("SELECT * FROM band_adjacency")
    # print(db_cursor.fetchall())

    for band in bands:
        id_ = band[0]
        name = band[1]
        endpoint = endpoint_preset.format(id_)
        r = session.get(endpoint)
        if r.status_code != 200:
            raise Exception(f"{r.status_code}: {r.content}")
        soup = BeautifulSoup(r.text, 'html.parser')
        if len(soup.find_all(id='no_artists')) > 0:
            unconnected_bands += 1
            print("unconnected")
            continue

        values = []

        rows = soup.findAll('tr')[1:-1]
        for row in rows:
            td = row.findAll('td')
            foreign_id = int(td[0].find("a").get("href").split("/")[-1])
            weight = int(td[-1].text)
            values.append((weight, id_, foreign_id))
        print(id_, name, endpoint, len(values))

        sql = f"""
        INSERT INTO band_adjacency 
        (weight, band_id_from, band_id_to) 
        VALUES (%s, %s, %s)
        """
        try:
            db_cursor.executemany(sql, values)
            db.commit()
        except Exception as e:
            print(e)

def add_theme_to_db(theme: str, cursor):
    sql = "SELECT id, label FROM theme WHERE label=%s"
    cursor.execute(sql, (theme,))
    res = cursor.fetchall()
    if len(res) > 0:
        return res[0][0]
    
    sql = f"""
    INSERT INTO theme 
    (label) 
    VALUES (%s)
    """
    cursor.execute(sql, (theme, ))
    db.commit()
    return add_theme_to_db(theme, cursor)



def fill_themes(db: mysql.connector):
    db_cursor = db.cursor()
    sql = "SELECT id, themes, name FROM band WHERE themes IS NOT NULL"
    db_cursor.execute(sql)
    bands = db_cursor.fetchall()
    for id_, raw_themes, name in bands:
        themes = raw_themes.split(", ")
        values = []
        for theme in themes:
            theme_id = add_theme_to_db(theme, db_cursor)
            values.append((id_, theme_id))
        sql = f"""
        INSERT INTO band_theme
        (band_id, theme_id) 
        VALUES (%s, %s)
        """
        db_cursor.executemany(sql, values)
        db.commit()


def fill_database(db: mysql.connector, session=requests.Session()):
    # download_band_overview(db, batch_download=True, session=session, skip_present=False)
    # get_adjacency(db, session=session)
    fill_themes(db=db)


if __name__ == "__main__":
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'Connection': 'keep-alive',
        'Keep-Alive': 'timeout=5, max=100'
    }
    
    session = requests.Session()
    session.headers = headers

    path = "db_credentials.json"
    if VSCODE:
        path = "src/db_credentials.json"

    with open(path, "r", encoding="utf-8") as f:
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

    # data = download_band_overview(db, batch_download=True, session=session, get_all=True)
    # get_adjacency(db, session=session)
    fill_database(db=db, session=session)
