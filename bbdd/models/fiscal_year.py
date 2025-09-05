from sqlalchemy import Column, Integer, Float, String, ForeignKey, PrimaryKeyConstraint, and_
from sqlalchemy.orm import relationship
from conf import *
from ..db import Base
from ..utils import divide


class FiscalYear(Base):
    """Financial ratios and price metrics for a company's fiscal year."""

    __tablename__ = 'fiscal_year'

    fiscal_year = Column(
        Integer,
        nullable=False,
        comment="Fiscal year of the record",
    )
    symbol = Column(
        String,
        ForeignKey('company.symbol'),
        nullable=False,
        comment="Ticker symbol of the company",
    )

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'fiscal_year'),
    )

    company = relationship("Company", back_populates="fiscal_years")

    cash_flow = relationship(
        "CashFlow",
        back_populates="fiscal_year_relation",
        primaryjoin="and_(FiscalYear.symbol == CashFlow.symbol, FiscalYear.fiscal_year == CashFlow.fiscal_year)",
        uselist=False,
    )

    income_statement = relationship(
        "IncomeStatement",
        back_populates="fiscal_year_relation",
        primaryjoin="and_(FiscalYear.symbol == IncomeStatement.symbol, FiscalYear.fiscal_year == IncomeStatement.fiscal_year)",
        uselist=False,
    )

    balance_sheet = relationship(
        "BalanceSheet",
        back_populates="fiscal_year_relation",
        primaryjoin="and_(FiscalYear.symbol == BalanceSheet.symbol, FiscalYear.fiscal_year == BalanceSheet.fiscal_year)",
        uselist=False,
    )

    price_first = Column(Float, comment="First trading price of the year")
    price_last = Column(Float, comment="Last trading price of the year")
    price_min = Column(Float, comment="Lowest trading price of the year")
    price_max = Column(Float, comment="Highest trading price of the year")
    price_avg = Column(Float, comment="Average trading price during the year")
    price_std = Column(Float, comment="Standard deviation of the price")
    price_var = Column(Float, comment="Variance of the price")
    price_change = Column(Float, comment="Absolute price change over the year")
    price_change_pct = Column(Float, comment="Percentage price change over the year")
    price_change_pct_1y = Column(
        Float,
        comment="Percentage price change over the last year",
    )
    price_change_pct_1m = Column(
        Float,
        comment="Percentage price change over the last month",
    )
    price_change_pct_3m = Column(
        Float,
        comment="Percentage price change over the last quarter",
    )
    price_change_pct_6m = Column(
        Float,
        comment="Percentage price change over the last six months",
    )

    @property
    def market_cap(self):
        """Return the market capitalization based on last price and shares."""
        if (
            self.price_last is None
            or not self.income_statement
            or self.income_statement.acciones_promedio is None
        ):
            return None
        return self.price_last * self.income_statement.acciones_promedio

    @property
    def per(self):
        """Price-to-earnings ratio."""
        return divide(self.market_cap, self.income_statement.ingreso_neto)

    @property
    def pfcf(self):
        """Price-to-free-cash-flow ratio."""
        return divide(self.market_cap, self.cash_flow.flujo_libre_caja)

    @property
    def pb(self):
        """Price-to-book ratio."""
        return divide(self.market_cap, self.balance_sheet.total_patrimonio_accionistas)

    @property
    def ps(self):
        """Price-to-sales ratio."""
        return divide(self.market_cap, self.income_statement.ingresos)

    @property
    def ev_ebit(self):
        """Enterprise value divided by operating income."""
        deuda_total = self.balance_sheet.total_deuda
        efectivo = self.balance_sheet.efectivo_y_equivalentes
        enterprise_value = (self.market_cap or 0) + (deuda_total or 0) - (efectivo or 0)
        return divide(enterprise_value, self.income_statement.ingreso_operativo)
