from sqlalchemy import Column, Integer, String, Date
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
