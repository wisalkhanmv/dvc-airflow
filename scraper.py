import requests
from bs4 import BeautifulSoup


def extract_dawn_articles():
    url = "https://www.dawn.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    articles = []
    for item in soup.find_all('article'):
        title = item.find('h2').get_text(strip=True) if item.find('h2') else ''
        description = item.find('p').get_text(
            strip=True) if item.find('p') else ''
        articles.append({'title': title, 'description': description})
    return articles


def extract_bbc_articles():
    url = "https://www.bbc.com/news"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    articles = []
    for item in soup.find_all('div', {'class': 'gs-c-promo'}):
        title = item.find('h3').get_text(strip=True) if item.find('h3') else ''
        description = item.find('p').get_text(
            strip=True) if item.find('p') else ''
        articles.append({'title': title, 'description': description})
    return articles
