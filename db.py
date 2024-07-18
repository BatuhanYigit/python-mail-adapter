import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from models import Base, Flight, Log
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import datetime

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DATABASE_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(
    DATABASE_URI,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_tables():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def add_flight(db, flight_data):
    new_flight = Flight(**flight_data)
    db.add(new_flight)
    db.commit()
    db.refresh(new_flight)
    return new_flight


def delete_min_date_max_date(db, finish_process, flights_data, mail_id):
    df = pd.DataFrame(flights_data)
    df["Date"] = pd.to_datetime(df["Date"])

    result = df.groupby("OriginCountryCode")["Date"].agg(["min", "max"]).reset_index()
    for _, row in result.iterrows():
        print(
            f"Ülke: {row['OriginCountryCode']}, Min Tarih: {row['min']}, Max Tarih: {row['max']}"
        )

        flights_delete_data = db.query(Flight).filter(
            Flight.OriginCountryCode == row["OriginCountryCode"],
            Flight.Date >= row["min"],
            Flight.Date <= row["max"],
        )

        if flights_delete_data.count() > 0:

            try:
                flights_delete_data.delete(synchronize_session=False)
                db.commit()
                log_operation(
                    db,
                    row["OriginCountryCode"],
                    row["min"],
                    row["max"],
                    finish_process,
                    None,
                    "delete",
                    mail_id,
                )
            except Exception as e:
                print(f"Error database {e}")
                db.rollback()

        elif flights_delete_data.count() == 0:
            print(f"Data not found. inserting... ")


def check_mail_id(db, mail_id):
    return db.query(Log).filter(Log.mail_id == mail_id).first()


def add_flights_bulk(db, process_time, mail_id, flights_data, batch_size=10000):
    connection = engine.connect()
    trans = connection.begin()
    start_insert = datetime.datetime.now()

    query = Flight.__table__.insert()
    values = [
        {
            "OriginCountryCode": flight["OriginCountryCode"],
            "OriginCityCode": flight["OriginCityCode"],
            "OriginAirportCode": flight["OriginAirportCode"],
            "AirlineCode": flight["AirlineCode"],
            "DestinationCountryCode": flight["DestinationCountryCode"],
            "DestinationCityCode": flight["DestinationCityCode"],
            "DestinationAirportCode": flight["DestinationAirportCode"],
            "Seat": flight["Seat"],
            "Date": flight["Date"],
        }
        for flight in flights_data
    ]

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        connection.execute(query, batch)

    trans.commit()
    connection.close()
    finish_insert = datetime.datetime.now()
    total_insert_time = finish_insert - start_insert

    log_operation(
        db,
        values[0]["OriginCountryCode"],
        min(flight["Date"] for flight in values),
        max(flight["Date"] for flight in values),
        process_time,
        total_insert_time,
        "insert",
        mail_id,
    )


# Log
def log_operation(
    db,
    country_code,
    min_date,
    max_date,
    process_csv_duration,
    insert_duration,
    operation_type,
    mail_id,
):
    log_entry = Log(
        process_time=datetime.datetime.now(),
        country_code=country_code,
        min_date=min_date,
        max_date=max_date,
        process_csv_duration=str(process_csv_duration),
        insert_duration=str(insert_duration),
        mail_id=mail_id,
        operation_type=operation_type,
    )
    db.add(log_entry)
    db.commit()


def get_flights(db):
    return db.query(Flight).all()
