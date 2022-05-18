import requests
from bs4 import BeautifulSoup
import enum

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
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
    akronyms = 'akronyms'
    notes = 'notes'


def get_metal_data(band_name="*",
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
                   ):
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
        r = requests.get(url, headers=headers)
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
            anchor.decompose()
            strong = soup.find('strong')
            akronyms_ = None
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
            themes_ = band[4].split(',')

            # year_creation
            year_creation_ = None
            if band[5] != 'N/A':
                year_creation_ = int(band[5])

            # label
            label_ = band[6]

            # notes
            notes_ = band[7]

            bands.append({
                Band.name: band_name_,
                Band.akronyms: akronyms_,
                Band.ma_url: ma_url_,
                Band.genre: genre_,
                Band.country: country_,
                Band.location: location_,
                Band.themes: themes_,
                Band.year_creation: year_creation_,
                Band.band_notes: notes_,
                Band.band_label_name: label_,
            })
        cursor += length
        print(f"{cursor}/{r.json()['iTotalRecords']}")
        if not batch_download:
            break

    return bands


def parse_genre(genre: str):
    print(genre)
    return genre

if __name__ == "__main__":
    data = get_metal_data(batch_download=False)
    # for band in data:
    #     print("")
    #     print(band)

    print(len(data))
