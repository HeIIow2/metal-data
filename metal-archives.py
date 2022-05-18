import requests
from bs4 import BeautifulSoup
import enum

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
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
                'sEcho=1&iColumns=10&sColumns=0&iDisplayStart={start}&iDisplayLength=200'

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
                   band_label_name="*"):
    bands = []

    if themes is None:
        themes = []
    themes_string = " ".join(themes) if len(themes) > 0 else "*"

    url = METAL_API_URL.format(bandName=band_name, genre=genre, country=country, yearCreationFrom=year_creation_from, yearCreationTo=year_creation_to, bandNotes=band_notes, status=status, themes=themes_string, location=location, bandLabelName=band_label_name, start=0)
    r = requests.get(url, headers=headers)
    if int(r.status_code/100) != 2:
        raise Exception('Error getting metal data: {}'.format(r.status_code))

    data = r.json()['aaData']
    for band in data:
        # band, genre, country, location, themes, year_creation, label, Notes

        print(band)
        # parse the html for the band name and link on metal-archives
        soup = BeautifulSoup(band[0], 'html.parser')
        anchor = soup.find('a')
        band_name = anchor.text
        ma_url = anchor.get('href')
        anchor.decompose()
        soup.find('strong').decompose()
        akronyms = soup.text[2:-2].split(', ')

        # genre
        genre = band[1]

        # country
        country = band[2]

        # location
        location = band[3]

        # themes
        themes = band[4].split(',')

        # year_creation
        year_creation = int(band[5])

        # label
        label = band[6]

        # notes
        notes = band[7]

        bands.append({
            Band.name: band_name,
            Band.akronyms: akronyms,
            Band.ma_url: ma_url,
            Band.genre: genre,
            Band.country: country,
            Band.location: location,
            Band.themes: themes,
            Band.year_creation: year_creation,
            Band.band_notes: notes,
            Band.band_label_name: label,

        })

    return bands

if __name__ == "__main__":
    print(get_metal_data())
