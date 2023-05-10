# InsiderX
Automated ETL pipeline that extracts insider trading data from the Finnhub API, transforms it with Pandas, and loads it into a PostgreSQL database. 


## 1. Objective
The objective of this project is to extract insider trading data using the Finnhub API, transform the data into a Pandas dataframe, and load the data into a PostgreSQL database. The script will be run by a cron job every 24 hours to ensure that the database is up-to-date with the latest insider trading information. 

## 2. Architecture

![etl_architecture](https://github.com/kojoh/InsiderX/blob/main/images/etl_architecture.png)





## 3. Explanation
The ETL pipeline has three main stages: Extract, Transform, and Load. Each stage performs a specific task to prepare and store the data in a usable format.

### 3.1 Extract
The Extract stage pulls data from the Finnhub API using the requests library. The API requires a stock symbol and a Finnhub API key, which are specified as parameters in the params dictionary. The data is returned as a JSON object, which is then parsed using the json library.

### 3.2 Transform
The Transform stage converts the JSON data into a Pandas DataFrame using pd.json_normalize(). This function creates a flattened DataFrame from the JSON data, making it easier to manipulate and analyze. The resulting DataFrame is then filtered to remove unnecessary columns and sorted in descending order by transaction date.

### 3.3 Load
The Load stage stores the transformed data in a PostgreSQL database. The script connects to the database using the psycopg2 library and creates a table to store the data if it does not already exist. Each row of data is then inserted into the table if it does not already exist, using a primary key constraint to prevent duplicates.
