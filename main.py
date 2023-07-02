import csv
import os
import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pytz import timezone
import uvicorn

app = FastAPI()

csv_file_path = "data.csv"
timezone_kazakhstan = timezone('Asia/Almaty')

def create_csv_file():
    # Check if the CSV file exists
    if not os.path.isfile(csv_file_path):
        # Create the CSV file and write the header row
        with open(csv_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["created_at", "lux", "current", "power"])

@app.on_event("startup")
async def startup_event():
    create_csv_file()

@app.get("/csv")
async def get_csv_file():
    return FileResponse(csv_file_path)

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

    # Format the timestamp as a string
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")

    # Append the data to the CSV file
    with open(csv_file_path, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, lux, current, power])

    return {"message": "Data added successfully"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)