import requests

API_KEY = '685e4414890949f3ad12b31afefe3c4b'
url_endpoint = 'https://newsapi.org/v2/top-headlines?country={country_code}&apiKey={api_key}'

def extract(api_key, country_code):
        endpoint = url_endpoint.format(api_key=api_key, country_code=country_code)
        response = requests.get(endpoint)
    
        return response.json()

if __name__ == '__main__':
    data = extract(API_KEY, 'sg')
    print(data['articles'])



