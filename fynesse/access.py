from .config import *

import pymysql
import urllib.request
from progressbar import ProgressBar
import zipfile
import requests
import pandas as pd

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """



def create_connection(user, password, host, database, port=3306):
    """ 
    Create a database connection to the MariaDB database specified by the host url and database name.

    Argument:
        user : (string) - username
        password : (string) - password
        host : (string) - host url
        database : (string) - database name
        port : (int) - port number
    Output:
        conn : (Connection object)/None - connection object
    """
    conn = None
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database
                               )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn

def load(conn, table, file, field_term = "\",\"", lines_start = "\"", lines_term = "\"\n"):
    """ 
    Load data from file to table

    Argument:
        conn : (Connection object) - connection to database
        table : (string) - table name
        file : (string) - csv file name
        field_term : (string) - end of each field
        lines_start : (string) - start of each csv line
        lines_term : (string) - end of each csv line
    Output:
        N/A
    """
    cur = conn.cursor()
    cur.execute(f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE {table} FIELDS TERMINATED BY '{field_term}' LINES STARTING BY '{lines_start}' TERMINATED BY '{lines_term}';")
    conn.commit()
    
def load_transactions(conn, table, start_year, end_year):
    """
    Load all transaction data from files downloaded from uk.gov to table

    Argument:
        conn : (Connection object) - connection to database
        table : (string) - table name
        start_year : (int) - download transactions from year
        end_year : (int) - download transaction to year (excluding the year)
    Output:
        N/A
    """
    pbar = ProgressBar()

    for i in pbar(range(start_year, end_year)):
        for j in range(1,3):
            urllib.request.urlretrieve('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-%d-part%d.csv' % (i,j), 'data.csv')
            load(conn, table, 'data.csv', "\",\"", "\"", "\"\n")

def load_postcodes(conn, headers):
    """
    Load all postcode data to the table

    Argument:
        conn : (Connection object) - connection to database
        headers : (dicctionary) - necessary headers to download data
    Output:
        N/A
    """
    resp1 = requests.get('https://www.getthedata.com/downloads/open_postcode_geo.csv.zip',headers=headers)

    with open('open_postcode_geo.csv.zip', 'wb') as handle:
        for block in resp1.iter_content(1024):
            if not block:
                break
            handle.write(block)

    with zipfile.ZipFile('open_postcode_geo.csv.zip', 'r') as zip_ref:
        zip_ref.extractall('open_postcode_geo.csv')    
        
    load(conn, 'postcode_data', 'open_postcode_geo.csv/open_postcode_geo.csv', ',', '', '\n')
    
def joinAndStorePriceAndLocationData(conn, longitudeMin, longitudeMax, lattitudeMin, lattitudeMax, dateMin, dateMax):
    """
    Function that joins location and price data and stores it into the third table.

    Arguments:
      conn : Connection Object - connection to database
      longitudeMin : double - minimum limit for box of interest of longitude
      longitudeMax : double - maximum limit for box of interest of longitude
      lattitudeMin : double - minimum limit for box of interest of lattitude
      lattitudeMax : double - maximum limit for box of interest of lattitude
      dateMin : string - minimum limit for time of interest of date e.g. '2018-01-01'
      dateMax : string - maximum limit for time of interest of date e.g. '2018-12-31'
    Outputs:
      N/A 
    """
    cur = conn.cursor()
    # Execute Query
    cur.execute(f"INSERT INTO prices_coordinates_data \n" + 
                f"SELECT pp.price, pp.date_of_transfer, pp.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, pc.country, pc.lattitude, pc.longitude, pp.db_id \n" +
                f"FROM \n" +
                f"(SELECT db_id, price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county FROM pp_data WHERE date_of_transfer >= '{dateMin}' AND date_of_transfer <= '{dateMax}') pp \n" +
                f"INNER JOIN \n" +
                f"(SELECT postcode, country, lattitude, longitude FROM postcode_data WHERE (longitude BETWEEN {str(longitudeMin)} AND {str(longitudeMax)}) AND (lattitude BETWEEN {str(lattitudeMin)} AND {str(lattitudeMax)})) pc \n" +
                f"ON \n" +
                f"pp.postcode = pc.postcode;")
    # Commit Results
    conn.commit()
    
def fetch_data(conn, table_name, columns):
    """
    Extracts data from database required to do assess and address part.

    Arguments:
      conn : connection object - Connection to the database we want to extract data from
      table_name : string - name of the table
      columns : list(string) - columns that table has
    Output:
      rows : tuple - data extracted as tuple 
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")

    rows = cur.fetchall()
    return pd.DataFrame(rows, columns = columns)

def joinPriceAndLocationData(conn, longitudeMin, longitudeMax, lattitudeMin, lattitudeMax, dateMin, dateMax):
    """
    Function that joins price and location data and returns dataframe.

    Arguments:
      conn : connection object - Connection to the database we want to extract data from
      longitudeMin : double - minimum limit for box of interest of longitude
      longitudeMax : double - maximum limit for box of interest of longitude
      lattitudeMin : double - minimum limit for box of interest of lattitude
      lattitudeMax : double - maximum limit for box of interest of lattitude
      dateMin : string - minimum limit for time of interest of date e.g. '2018-01-01'
      dateMax : string - maximum limit for time of interest of date e.g. '2018-12-31'
    Outputs:
      data : DataFrame - joined data for exploration
    """
    cur = conn.cursor()
    # Execute Query
    cur.execute(f"SELECT pp.price, pp.date_of_transfer, pp.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, pc.country, pc.lattitude, pc.longitude, pp.db_id \n" +
                f"FROM \n" +
                f"(SELECT postcode, country, lattitude, longitude FROM postcode_data) pc \n" +
                f"INNER JOIN \n" +
                f"(SELECT db_id, price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county FROM pp_data WHERE date_of_transfer >= '{dateMin}' AND date_of_transfer <= '{dateMax}') pp \n" +
                f"ON \n" +
                f"pp.postcode = pc.postcode\n" +
                f"WHERE (longitude BETWEEN {str(longitudeMin)} AND {str(longitudeMax)}) AND (lattitude BETWEEN {str(lattitudeMin)} AND {str(lattitudeMax)});")
    # Collect Data
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns = ['price',	'date_of_transfer',	'postcode',	'property_type',	'new_build_flag',	'tenure_type',	'locality',	'town_city',	'district',	'county',	'country',	'lattitude',	'longitude',	'db_id'])

