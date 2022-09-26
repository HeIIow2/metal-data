import mysql.connector
import json


with open("src/db_credentials.json", "r") as f:
  data = json.load(f)
  user = data["username"]
  password = data["password"]
  database_name = data["db"]


cnx = mysql.connector.connect(user=user, password=password, host='localhost')
cursor = cnx.cursor()

# create database if it doesn't already exist
sql = f"CREATE DATABASE {database_name};"
try:
  cursor.execute(sql)
except mysql.connector.errors.DatabaseError:
  print("Database already exists")

cnx = mysql.connector.connect(
  user=user,
  password=password,
  host='localhost',
  database=database_name
)
cursor = cnx.cursor()

# read the table structure and write it to the database
with open("src/database.sql", "r", encoding="utf-8") as file:
  sql = file.read().rstrip("\n")
  if sql == "":
    exit()

for result in cursor.execute(sql, multi=True):
  if result.with_rows:
    print("Rows produced by statement '{}':".format(
      result.statement))
    print(result.fetchall())
  else:
    print("Number of rows affected by statement '{}': {}".format(
      result.statement, result.rowcount))

cnx.close()
