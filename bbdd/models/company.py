from sqlalchemy import Column, String, Float
from sqlalchemy.orm import relationship
from conf import *
from ..db import Base


class Company(Base):
    """Represents a company with basic market information."""

    __tablename__ = 'company'

    symbol = Column(
        String,
        primary_key=True,
        comment="Ticker symbol identifying the company",
    )
    company_name = Column(String, comment="Registered name of the company")
    price = Column(Float, comment="Latest market price per share")
    exchange = Column(String, comment="Exchange where the company trades")
    exchange_short_name = Column(
        String,
        comment="Abbreviated name of the stock exchange",
    )
    sector = Column(String, comment="Industry sector of the company")

    fiscal_years = relationship("FiscalYear", back_populates="company")
