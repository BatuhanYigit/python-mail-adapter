import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from models import Base, Flight
import psycopg2
from psycopg2.extras import execute_values

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


def add_flights_bulk(flights_data, batch_size=10000):
    connection = psycopg2.connect(DATABASE_URI)
    cursor = connection.cursor()

    # Print the table name and column names for debugging
    print(f"Table Name: {Flight.__tablename__}")
    print("Column Names:", Flight.__table__.columns.keys())

    query = """
    INSERT INTO "public"."flights" (OriginCountryCode, OriginCityCode, OriginAirportCode, AirlineCode, DestinationCountryCode, DestinationCityCode, DestinationAirportCode, Seat, Date)
    VALUES %s
    """
    values = [
        (
            flight["OriginCountryCode"],
            flight["OriginCityCode"],
            flight["OriginAirportCode"],
            flight["AirlineCode"],
            flight["DestinationCountryCode"],
            flight["DestinationCityCode"],
            flight["DestinationAirportCode"],
            flight["Seat"],
            flight["Date"],
        )
        for flight in flights_data
    ]

    # Print values for debugging
    print("Values to be inserted:", values[:5])  # print first 5 values for brevity

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        execute_values(cursor, query, batch)
        connection.commit()

    cursor.close()
    connection.close()


def get_flights(db):
    return db.query(Flight).all()
