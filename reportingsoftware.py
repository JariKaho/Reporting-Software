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

# Assign date for which we want to create hour report
date = '2024-05-01'

# Define the query to fetch working hour data from tables and summarize by consultant and by customer
consultant_query = """SELECT 
                    employees.id AS employee_id,
                    employees.name AS employee_name,
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
                        Employees ON Employees.id = WorkingHours.employeeID
                    WHERE 
                        DATE(startTime) = %s  -- Replace with the desired date
                    GROUP BY 
                        employees.id, employees.name, DATE(startTime);"""

customer_query = """SELECT 
                    customers.id AS customer_id,
                    customers.name AS customer_name,
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
                        Customers ON Customers.id = WorkingHours.customerID
                    WHERE 
                        DATE(startTime) = %s  -- Replace with the desired date
                    GROUP BY 
                        customers.id, customers.name, DATE(startTime);"""

# Execute the queries
consultant_rows = cur.execute(consultant_query, (date,))
consultant_fetch = cur.fetchall()
customer_rows = cur.execute(customer_query, (date,))
customer_fetch = cur.fetchall()

# Write the rows to a text file
with open(f"hour_report_{date}.txt", "a") as file:
    file.write("Working hours grouped by consultant:\n")
    for row in consultant_fetch:
        file.write(f"{row[0]} {row[1]} {row[2]} {row[3]}\n")

with open(f"hour_report_{date}.txt", "a") as file:
    file.write("Working hours grouped by customer:\n")
    for row in customer_fetch:
        file.write(f"{row[0]} {row[1]} {row[2]} {row[3]}\n")

# <--- REPORTING SECTION ENDS --->



# <--- AZURE BLOB STORAGE SECTION STARTS --->

# Define connection and blob variables
container_name = "timemanagementblob"
blob_name = f"hour_report_{date}.txt"
file_path = f"hour_report_{date}.txt"
connection_string = "DefaultEndpointsProtocol=https;AccountName=timemanagementproject;AccountKey=AnWhAVQpchH6JizwOP8Z9QCvfcSO96Yh/S+qsojGJml8Va6zORAOPRkZ16KNPhs8vv3RzxJku+Mt+AStOmVADA==;EndpointSuffix=core.windows.net"

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

# <--- AZURE BLOB STORAGE SECTION ENDS --->"""