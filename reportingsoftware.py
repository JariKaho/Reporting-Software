import psycopg2
import os
from psycopg2 import sql
from config import config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient



# <--- REPORTING SECTION STARTS --->

# Create a connection url to the Azure PostgreSQL server using the configuration in database.ini
data = config()
database_url = f"postgresql://{data['user']}:{data['password']}@{data['host']}:{data['port']}/{data['database']}"

# Establish a connection and create a cursor object
conn = psycopg2.connect(database_url)
cur = conn.cursor()

# Define the query to fetch working hour data from tables and summarize by consultant and by customer
consultant_query = """SELECT consultant_name, sum(working_hours) FROM table_name
                    GROUP BY consultant_name;
                    """

customer_query = """SELECT consultant_name, customer_name, sum(working_hours) FROM table_name
                    GROUP BY customer_name;
                    """

# Execute the queries
consultant_rows = cur.execute(consultant_query)
customer_rows = cur.execute(customer_query)

# Write the rows to a text file
with open("hour_report.txt", "a") as file:
    file.write("Working hours grouped by consultant:\n")
    for row in consultant_rows:
        file.write(row)

with open("hour_report.txt", "a") as file:
    file.write("Working hours grouped by customer:\n")
    for row in customer_rows:
        file.write(row)

# <--- REPORTING SECTION ENDS --->



# <--- AZURE BLOB STORAGE SECTION STARTS --->

# Define connection and blob variables
container_name = "hour_report_container"
blob_name = "hour_report"
file_path = "hour_report.txt"
connection_string = ""

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
# Create the container if it doesn't already exist
container_client = blob_service_client.get_container_client(container_name)
try:
    container_client.create_container()
except Exception as e:
    print(f"Container already exists or failed to create: {e}")

# Create a blob client using the file path
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Upload the file
with open(file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)
    print(f"File {file_path} uploaded to blob {blob_name} in container {container_name}.")

# <--- AZURE BLOB STORAGE SECTION ENDS --->