import requests


url = 'https://api.bigbookapi.com/search-books'
API_KEY = 'f2602c017a2e4af1b2cb973d26328b3d'

try:
    api_response = requests.get(url, timeout=15)
    api_response.raise_for_status()
    standings_data = api_response.json()
    print(standings_data)


except Exception as http_err:
    print(http_err)
