import mysql.connector
from datetime import datetime
import time

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="demo"
)

cursor = mydb.cursor()

while True:
    records = [
        (6, 'Alice', 100000, datetime.now(), datetime.now()),
        (7, 'Bob', 80000, datetime.now(), datetime.now()),
        (8, 'Charlie', 60000, datetime.now(), datetime.now()),
        (9, 'Dave', 70000, datetime.now(), datetime.now()),
        (10, 'Eve', 90000, datetime.now(), datetime.now())
    ]

    sql = "INSERT INTO kafka_injection (emp_id, emp_name, salary, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(sql, records)
    mydb.commit()

    # Sleep for 15 seconds before inserting more records
    time.sleep(15)
