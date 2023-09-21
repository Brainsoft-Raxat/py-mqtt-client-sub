import os
import psycopg2
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from typing import Dict
import io
import csv

app = FastAPI()

db_url = os.getenv("DATABASE_URL")
if db_url is None:
    raise ValueError("DATABASE_URL environment variable is not set.")

def create_table():
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mqtt_data (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    lux FLOAT,
                    shuntvoltage FLOAT,
                    busvoltage FLOAT,
                    current_mA FLOAT,
                    loadvoltage FLOAT,
                    power_mW FLOAT
                )
                """
            )
            conn.commit()

@app.on_event("startup")
async def startup_event():
    create_table()

class SensorData(BaseModel):
    lux: float
    shuntvoltage: float
    busvoltage: float
    current_mA: float
    loadvoltage: float
    power_mW: float

@app.post("/data")
async def add_data(data: SensorData):
    try:
        lux = data.lux
        shuntvoltage = data.shuntvoltage
        busvoltage = data.busvoltage
        current_mA = data.current_mA
        loadvoltage = data.loadvoltage
        power_mW = data.power_mW
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid data format")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mqtt_data (lux, shuntvoltage, busvoltage, current_mA, loadvoltage, power_mW)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (lux, shuntvoltage, busvoltage, current_mA, loadvoltage, power_mW)
            )
            conn.commit()

    return {"message": "Data received and processed successfully"}

from fastapi.responses import Response

@app.get("/csv")
async def get_csv_file():
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT created_at, lux, shuntvoltage, busvoltage, loadvoltage, current_mA, power_mW FROM mqtt_data ORDER BY created_at ASC")
            rows = cur.fetchall()

    # Create a CSV file in memory
    csv_data = []
    csv_data.append(["created_at", "lux", "shuntvoltage", "busvoltage", "loadvoltage", "current_mA", "power_mW"])  # Update column names
    for row in rows:
        csv_data.append(list(row))

    # Create a response with the CSV file
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerows(csv_data)

    response = Response(content=csv_buffer.getvalue(), media_type="text/csv")
    response.headers["Content-Disposition"] = 'attachment; filename="data.csv"'

    return response  # Corrected from 'respons' to 'response'


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

@app.post("/execute-sql")
async def execute_sql_query(query_data: Dict[str, str]):
    query = query_data.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter is required")

    # Perform additional security checks or query validation if needed

    execute_query(query)

    return {"message": "Query executed successfully"}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
