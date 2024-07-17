import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from models import Base, Flight
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

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


def delete_min_date_max_date(db, flights_data):
    df = pd.DataFrame(flights_data)
    df["Date"] = pd.to_datetime(df["Date"])

    result = df.groupby("OriginCountryCode")["Date"].agg(["min", "max"]).reset_index()
    for _, row in result.iterrows():
        db.query(Flight).filter(
            Flight.OriginCountryCode == row["OriginCountryCode"],
            Flight.Date >= row["min"],
            Flight.Date <= row["max"],
        ).delete(synchronize_session=False)
        db.commit()
        print(
            f"Ülke: {row['OriginCountryCode']}, Min Tarih: {row['min']}, Max Tarih: {row['max']}"
        )


def add_flights_bulk(flights_data, batch_size=10000):
    connection = engine.connect()
    trans = connection.begin()

    print(f"Table Name: {Flight.__tablename__}")
    print("Column Names:", Flight.__table__.columns.keys())

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


def get_flights(db):
    return db.query(Flight).all()
