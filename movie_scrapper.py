import requests
import time
import csv
import random
import concurrent.futures


from bs4 import BeautifulSoup

# global headers to be used for requests
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

MAX_THREADS = 10


def extract_movie_details(movie_link):
    #time.sleep(random.uniform(0, 0.2))
    
    # Get movie informations page
    _, movie_soup = get_url_soup(movie_link)

    if movie_soup is not None:
        title = movie_soup.find('span', attrs={'class':'sc-afe43def-1'}).get_text()
        plot = movie_soup.find('span', attrs={'class':'sc-6a7933c5-2'}).get_text()
        rating = movie_soup.find('span', attrs={'class':'sc-bde20123-1'}).get_text()
        
        with open('movies.csv', mode='a') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if all([title, rating, plot]):
                print('Writing: ' + title)
                movie_writer.writerow([title, rating, plot])


def extract_movies(scraped_movies):    
    # Generate movie links
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in scraped_movies]

    # Start threads
    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)


def get_url_soup(url):    
    # Try get the URL requested
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    return response, soup


def get_movies_list():
    response, soup = get_url_soup('https://www.imdb.com/chart/moviemeter/')
    
    # Parse the movies list
    scraped_movies = soup.find_all('td', class_='titleColumn')
    
    if (response.status_code != 200) or len(scraped_movies) == 0:
        # Something went wrong
        print('Something went wrong. Will try again in 5 seconds : Status code=' + str(response.status_code) + ' : Scraped movies list size=' + str(len(scraped_movies)))
        time.sleep(5)
        
        return []
    
    return scraped_movies # HTTP request successful
  
    
def main():
    # Main function to extract the 100 movies from IMDB Most Popular Movies
    start_time = time.time() 
    scraped_movies = get_movies_list()
    
    while (not scraped_movies):
        start_time = time.time() # If the request failed, restart the start time
        
        scraped_movies = get_movies_list()
    
    # Will extract movies URLs
    extract_movies(scraped_movies)

    end_time = time.time()
    
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()