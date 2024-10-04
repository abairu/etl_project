from datetime import datetime, timedelta
from airflow import DAG
import requests
from collections import defaultdict
import pandas as pd
from bs4 import BeautifulSoup
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id='math_books',
    start_date=datetime(2024, 9, 30),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    default_args={'owner':'airflow',
                  'retries':2,
                  'retry_delay': timedelta(minutes=5)
    }  
) as dag:
    

    @task()
    def get_books(num_books):
        
        # Need the headers to make sure our request to amazon.com goes through
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9"
        } 
        # Our base url for when we make the request
        base_url = f'https://www.amazon.com/s?k=mathematics'
        # Keep track of the books we have
        books = [] 
        # Start on the first page. We may have to search multiple pages depending on the amount of books ('num_books')
        page = 1 

        while len(books) < num_books:
            url = f"{base_url}&page={page}"
            # Send the request
            response = requests.get(url, headers=headers)
            
            # If the response is successful
            if response.status_code == 200:
                # Use bs4 (Beautiful Soup) to parse the content of our request
                soup = BeautifulSoup(response.content, "html.parser") # Use 
                
                # Retrieve all the books found on the page
                book_containers = soup.find_all("div", {"class": "s-result-item"})
                
                # Loop through the books we found and extract the data we want
                for book in book_containers:
                    title = book.find("span", {"class": "a-text-normal"})
                    author = book.find("a", {"class": "a-size-base"})
                    price = book.find("span", {"class": "a-price-whole"})
                    rating = book.find("span", {"class": "a-icon-alt"})
                    
                    if title and author and price and rating:
                        book_title = title.text.strip()                    
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })
                # We've extracted all the data we need from the current page, so we increment by 1.
                page += 1
            # If the request isn't a success, break from the loop.
            else:
                print("Failed to retrieve the page")
                break
        # Return the number of books we want 
        return books[:num_books]
    
    @task()
    def transform_books(books):
        # Get rid of duplicates by looping through books, keeping track of seen titles, and only keeping uniquely titled books.
        titles = set()
        unique_books = []

        for book in books:
            if book['Title'] in titles:
                continue
            else:
                unique_books.append(book)
                titles.add(book['Title'])
        
        # Convert the list of dictionaries to a dataframe with column name
        columns = ['Title', 'Author', 'Price', 'Rating']
        
        df = pd.DataFrame(unique_books, columns=columns)
        
        # Return transformed books data
        return df

    
    @task()
    def load_books(books):
        # Pull the connection 
        postgres_hook = PostgresHook(postgres_conn_id='math_books_conn')
        postgres_conn = postgres_hook.get_sqlalchemy_engine()
        
        # Load the table into PostgreSQL
        books.to_sql(
            name = 'math_books',
            con = postgres_conn,
            if_exists='append',
            index = False

        )
        
        
    # Dependencies 
    extracted = get_books(50)
    transformed = transform_books(extracted)
    load_books(transformed)

    






