from .config import *

import pymysql
import urllib.request
from progressbar import ProgressBar

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

def load(table, file, field_term = "\",\"", lines_start = "\"", lines_term = "\"\n"):
    """ 
    Load data from file to table

    Argument:
        table : (string) - table name
        file : (string) - csv file name
        field_term : (string) - end of each field
        lines_start : (string) - start of each csv line
        lines_term : (string) - end of each csv line
    Output:
        N/A
    """
    conn = create_connection(user=credentials["username"], 
                         password=credentials["password"], 
                         host=database_details["url"],
                         database="property_prices")
    cur = conn.cursor()
    cur.execute(f"LOAD DATA LOCAL INFILE '{file}' INTO TABLE {table} FIELDS TERMINATED BY '{field_term}' LINES STARTING BY '{lines_start}' TERMINATED BY '{lines_term}';")
    conn.commit()
    conn.close()
    
def load_transactions(table, start_year, end_year):
    """
    Load all transaction data from files downloaded from uk.gov to table

    Argument:
        table : (string) - table name
        start_year : (int) - download transactions from year
        end_year : (int) - download transaction to year (excluding the year)
        field_term : (string) - end of each field
        lines_start : (string) - start of each csv line
        lines_term : (string) - end of each csv line
    Output:
        N/A
    """
    pbar = ProgressBar()

    for i in pbar(range(start_year, end_year)):
        for j in range(1,3):
            urllib.request.urlretrieve('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-%d-part%d.csv' % (i,j), 'data.csv')
            load(table, 'data.csv', "\",\"", "\"", "\"\n")
