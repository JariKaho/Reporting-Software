import psycopg2
import os
from psycopg2 import sql
from config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from tabulate import tabulate


# <--- REPORTING SECTION STARTS --->

# Assign helper date for which we want to create hour report
DATE = '2024-05-02'

# Create a connection url to the Azure PostgreSQL server using the configuration in database.ini
def get_url():
    data = config()
    database_url = f"postgresql://{data['user']}:{data['password']}@{data['host']}:{data['port']}/{data['database']}"
    return database_url

# Establish a connection and create a cursor object
def get_connection(database_url):
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()
    return cur

# Define the query to fetch working hour data from tables and summarize by consultant and by customer
def define_queries(table_str):
    query = f"""SELECT 
                {table_str}s.id AS {table_str}_id,
                {table_str}s.name AS {table_str}_name,
                ROUND(
                    SUM(
                        CASE 
                            WHEN lunchBreak = TRUE THEN EXTRACT(EPOCH FROM (endTime - startTime)) / 3600.0 - 0.5
                            ELSE EXTRACT(EPOCH FROM (endTime - startTime)) / 3600.0
                        END
                    ), 
                    2
                ) AS total_working_hours, 
                DATE(startTime) AS working_date
                FROM 
                    WorkingHours
                JOIN 
                    {table_str}s ON {table_str}s.id = WorkingHours.{table_str}ID
                WHERE 
                    DATE(startTime) = %s  -- Replace with the desired date
                GROUP BY 
                    {table_str}s.id, {table_str}s.name, DATE(startTime);"""
    
    return query

# Execute the queries
def execute_queries(query, cursor_object):
    rows = cursor_object.execute(query, (DATE,))
    fetch = cursor_object.fetchall()
    return fetch

# Format the table to have a prettier output
def define_table(table_str, fetch_result): 
    headers = ["ID", f"{table_str} Name", "Total Working Hours", "Date"]
    master_list = []
    master_list.append(headers)
    for row in fetch_result:
        sub_list = []
        for item in row:
            sub_list.append(item)
        master_list.append(sub_list)
    return master_list

# Write the rows to a text file
def write_to_file(master_consultant_list, master_customer_list, DATE):
    with open(f"hour_report_{DATE}.txt", "w") as file:
        file.write("Working hours grouped by consultant:\n")
        file.write('\n')
        file.write(tabulate(master_consultant_list, tablefmt="github"))
        file.write('\n\n') 

    with open(f"hour_report_{DATE}.txt", "a") as file:
        file.write("Working hours grouped by customer:\n")
        file.write('\n')
        file.write(tabulate(master_customer_list, tablefmt="github"))

# <--- REPORTING SECTION ENDS --->



# <--- AZURE BLOB STORAGE SECTION STARTS --->

# Define connection and blob variables
CONTAINER_NAME = "timemanagementblob"
BLOB_NAME = f"hour_report_{DATE}.txt"
FILE_PATH = f"hour_report_{DATE}.txt"
PASS_TOKEN = os.environ.get('azure_storage_pass')
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(PASS_TOKEN)
    
# Create the container if it doesn't already exist
def create_container():
    container_client = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)
    try:
        container_client.create_container()
    except Exception as e:
        print(f"Container already exists or failed to create: {e}")

# Create a blob client using the file path
def create_blob_and_upload():
    blob_client = BLOB_SERVICE_CLIENT.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
    with open(FILE_PATH, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"File {FILE_PATH} uploaded to blob {BLOB_NAME} in container {CONTAINER_NAME}.")

# <--- AZURE BLOB STORAGE SECTION ENDS --->

# <--- Definition of main program --->

def main():
    url_object = get_url()
    cur = get_connection(url_object)
    consultant_query = define_queries("employee")
    customer_query = define_queries("customer")
    consultant_result = execute_queries(consultant_query, cur) 
    customer_result = execute_queries(customer_query, cur)
    master_consultant_list = define_table("Employee", consultant_result)
    master_customer_list = define_table("Customer", customer_result)
    final_report = write_to_file(master_consultant_list, master_customer_list, DATE)
    create_container()
    create_blob_and_upload()

# <--- End of main program --->

# <--- Run main program --->
main()