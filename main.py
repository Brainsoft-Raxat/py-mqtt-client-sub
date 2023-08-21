import os
import csv
import datetime
import psycopg2
from pytz import timezone
import io
from typing import Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

app = FastAPI()

db_url = os.getenv("DATABASE_URL")
# db_url = "postgresql://postgres:password@localhost:5432/postgres"
if db_url is None:
    raise ValueError("DATABASE_URL environment variable is not set.")

timezone_kazakhstan = timezone('Asia/Almaty')

def create_table():
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mqtt_data (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP,
                    lux FLOAT,
                    current FLOAT,
                    power FLOAT
                )
                """
            )
            conn.commit()

@app.on_event("startup")
async def startup_event():
    create_table()

@app.get("/csv")
async def get_csv_file():
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT created_at, lux, current, power FROM mqtt_data ORDER BY created_at ASC")
            rows = cur.fetchall()

    # Create a CSV file in memory
    csv_data = []
    csv_data.append(["created_at", "lux", "current", "power"])
    for row in rows:
        csv_data.append(list(row))

    # Create a response with the CSV file
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerows(csv_data)

    response = Response(content=csv_buffer.getvalue(), media_type="text/csv")
    response.headers["Content-Disposition"] = 'attachment; filename="data.csv"'

    return response

@app.post("/data")
async def add_data(data: dict):
    try:
        lux = float(data["lux"])
        current = float(data["current"])
        power = float(data["power"])
    except (KeyError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid data format")

    # Get the current time in Kazakhstan's timezone
    current_time = datetime.datetime.utcnow()
    gmt_offset = datetime.timedelta(hours=6)
    current_time += gmt_offset

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mqtt_data (created_at, lux, current, power)
                VALUES (%s, %s, %s, %s)
                """,
                (current_time, lux, current, power)
            )
            conn.commit()

    return {"message": "Data added successfully"}

@app.post("/truncate")
async def truncate_table():
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE mqtt_data")
            conn.commit()

    return {"message": "mqtt_data table truncated successfully"}

def execute_query(query: str):
    global db_url
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

# @app.post("/execute-sql")
# async def execute_sql_query(query_data: Dict[str, str]):
#     query = query_data.get("query")
#     if not query:
#         raise HTTPException(status_code=400, detail="Query parameter is required")

#     # Perform additional security checks or query validation if needed

#     execute_query(query)

#     return {"message": "Query executed successfully"}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
