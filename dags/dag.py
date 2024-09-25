"""
Tasks: E (web scrape api call)  T(clean data) L (create and insert data into table in postgres) 
Operators: Pythons and Postgres
Hooks: Postgres
Dependencies: get data, clean, load

Functions:
    get_amazon_data_books -          Note: ti.xcom_push(key='book_data'
    insert_book_data_into_postgres - Note: ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
DAG:                                                                                 |
    fetch_and_store_amazon_books                                                     |
Tasks:                                                                               V
    fetch_book_data_task ---> runs function ---> get_amazon_data_books    task_id='fetch_book_data'
    create_table_task ---> runs CREATE TABLE query
    insert_book_data_task ---> runs function ---> insert_book_data_into_postgres

"""

from airflow import DAG
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



#FUNCTIONS TO BE USED IN TASKS OUTLINED IN DAG
headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}
# Api call to amazon for books 
def get_amazon_data_books(num_books, ti):
    #amazon url for data engineer book search
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []
    seen_titles = set()  

    page = 1 #will be incremented at end of each loop

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        
        # Send a request to the URL
        response = requests.get(url, headers=headers)
        
        
        if response.status_code == 200:
            # initiate a Beautiful soup instance that will parse html content
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Look through book containers <div> for classes that are "s-result-item" (make sure it's infact the structure in your HTML)
            book_containers = soup.find_all("div", {"class": "s-result-item"})
            
            # Loop through the book containers and extract data
            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})
                
                if title and author and price and rating:
                    book_title = title.text.strip()
                    
                    # Check if title has been seen before
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })
            
            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # Limit to the requested number of books
    books = books[:num_books]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(books)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    
    # Push the DataFrame to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))


#create table ( if not exist), insert df into table and store in postgres
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data') # task id taken from fetch_book_data_task
    if not book_data:
        raise ValueError("No data found")
    
    #start a postgres hook instance using connection id from airflow admin connections
    postgres_hook = PostgresHook(postgres_conn_id='books_connection')  
    #define query for when inserting
    insert_query="""
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s,%s,%s,%s)
    """
    #for each row in df run postgres run instance using insert query with column schema as defined by parameters
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'],book['Price'],book['Rating'] ))

    
#THE DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books', # Naming the whole dag for this project
    default_args=default_args,
    description='Scrape books from amazon and store in postgres',
    schedule_interval=timedelta(days=1),
)

#DEFINE TASKS IN DAG    And...UNIQUE TASK ID
fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data', # used in insert_book_data_into_postgres function
    python_callable=get_amazon_data_books, #calling our previously made function
    op_args=[50], #number of books to fetch
    dag=dag #define to dag it belongs to
)

create_table_task = PostgresOperator(
    task_id='create_table', # unique task id
    postgres_conn_id='books_connection', # using connection id from airflow admin connections
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag, #define to dag it belongs to
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data', # unique task id
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)
fetch_book_data_task >> create_table_task >> insert_book_data_task