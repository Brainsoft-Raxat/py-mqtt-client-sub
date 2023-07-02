import os
import csv
import datetime
import psycopg2
from pytz import timezone
import io

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
            cur.execute("SELECT created_at, lux, current, power FROM mqtt_data")
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
    response.headers["Content-Disposition"] = 'attachment; filename="mqtt_data.csv"'

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
    current_time = datetime.datetime.now(timezone_kazakhstan)

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

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
