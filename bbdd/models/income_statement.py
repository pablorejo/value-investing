from sqlalchemy import Column, Integer, String, Float, ForeignKey, and_
from sqlalchemy.orm import relationship
from conf import *
from ..db import Base


class IncomeStatement(Base):
    """Income statement data for a company and fiscal year."""

    __tablename__ = 'income_statement'

    symbol = Column(String, ForeignKey('fiscal_year.symbol'), primary_key=True)
    fiscal_year = Column(Integer, ForeignKey('fiscal_year.fiscal_year'), primary_key=True)

    fiscal_year_relation = relationship(
        "FiscalYear",
        back_populates="income_statement",
        primaryjoin="and_(IncomeStatement.symbol == FiscalYear.symbol, IncomeStatement.fiscal_year == FiscalYear.fiscal_year)"
    )

    moneda_reportada = Column(String, comment="Currency used in the report")
    cik = Column(Integer, comment="SEC Central Index Key identifier")
    fecha_presentacion = Column(String, comment="Filing submission date")
    fecha_aceptacion = Column(String, comment="Filing acceptance date")
    periodo = Column(String, comment="Reporting period type")

    ingresos = Column(Float, comment="Total revenue")
    costo_ingresos = Column(Float, comment="Cost of revenue")
    coste_de_las_ventas = Column(Float, comment="Cost of goods sold")
    ganancia_bruta = Column(Float, comment="Gross profit")
    margen_ganancia_bruta = Column(Float, comment="Gross profit margin")
    gastos_investigacion_desarrollo = Column(
        Float,
        comment="Research and development expenses",
    )
    gastos_generales_administrativos = Column(
        Float,
        comment="General and administrative expenses",
    )
    gastos_ventas_marketing = Column(Float, comment="Sales and marketing expenses")
    gastos_operativos = Column(Float, comment="Total operating expenses")
    otros_gastos = Column(Float, comment="Other expenses")
    coste_y_gastos_totales = Column(Float, comment="Total costs and expenses")

    ingresos_por_intereses = Column(Float, comment="Interest income")
    gastos_por_intereses = Column(Float, comment="Interest expenses")
    depreciaciones_amortizaciones = Column(
        Float,
        comment="Depreciation and amortization",
    )
    ebitda = Column(
        Float,
        comment="Earnings before interest, taxes, depreciation and amortization",
    )
    margen_ebitda = Column(Float, comment="EBITDA margin")

    ingreso_operativo = Column(Float, comment="Operating income")
    margen_ingreso_operativo = Column(Float, comment="Operating income margin")
    otros_ingresos_gastos_netos = Column(
        Float,
        comment="Other net income or expenses",
    )
    ingreso_antes_impuestos = Column(Float, comment="Income before taxes")
    margen_ingreso_antes_impuestos = Column(
        Float,
        comment="Income before taxes margin",
    )
    impuestos = Column(Float, comment="Income tax expense")
    ingreso_neto = Column(Float, comment="Net income")
    margen_ingreso_neto = Column(Float, comment="Net profit margin")

    eps = Column(Float, comment="Earnings per share")
    eps_diluido = Column(Float, comment="Diluted earnings per share")

    acciones_promedio = Column(
        Integer,
        comment="Weighted average shares outstanding",
    )
    acciones_promedio_diluidas = Column(
        Integer,
        comment="Weighted average diluted shares outstanding",
    )

    enlace = Column(String, comment="Original filing link")
    enlace_final = Column(String, comment="Final filing link")

    @property
    def beneficio_bruto(self):
        """Gross profit calculated as revenue minus cost of goods sold."""
        return self.ingresos - self.coste_de_las_ventas

    @property
    def resultado_operativo(self):
        """Operating income before depreciation and amortization."""
        return self.beneficio_bruto - self.gastos_operativos

    @property
    def resultado_explotacion(self):
        """EBIT: operating income minus depreciation and amortization."""
        return self.resultado_operativo - self.depreciaciones_amortizaciones

    @property
    def beneficio_antes_impuestos(self):
        """Earnings before taxes."""
        return self.resultado_explotacion - self.gastos_por_intereses

    @property
    def beneficio_neto(self):
        """Net income after taxes."""
        return self.beneficio_antes_impuestos - self.impuestos
