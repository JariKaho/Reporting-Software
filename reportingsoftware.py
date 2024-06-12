import psycopg2
from psycopg2 import sql
from config import config

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


# 