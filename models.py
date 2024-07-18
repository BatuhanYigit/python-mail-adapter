from sqlalchemy import Column, Integer, String, Date, Interval, TIMESTAMP, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Flight(Base):
    __tablename__ = "flights"
    id = Column(Integer, primary_key=True, index=True)
    OriginCountryCode = Column(String(5))
    OriginCityCode = Column(String(5))
    OriginAirportCode = Column(String(5))
    AirlineCode = Column(String(5))
    DestinationCountryCode = Column(String(5))
    DestinationCityCode = Column(String(5))
    DestinationAirportCode = Column(String(5))
    Seat = Column(Integer)
    Date = Column(Date)


class Log(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True, index=True)
    process_time = Column(TIMESTAMP, nullable=False)
    country_code = Column(String(5), nullable=False)
    min_date = Column(Date)
    max_date = Column(Date)
    process_csv_duration = Column(Float)
    insert_duration = Column(Float)
    mail_id = Column(String(50))
    operation_type = Column(String(10), nullable=False)
