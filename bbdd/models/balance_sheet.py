from sqlalchemy import Column, Integer, String, Float, ForeignKey, and_
from sqlalchemy.orm import relationship
from conf import *
from ..db import Base


class BalanceSheet(Base):
    """Balance sheet data for a company and fiscal year."""

    __tablename__ = 'balance_sheet'

    symbol = Column(String, ForeignKey('fiscal_year.symbol'), primary_key=True)
    fiscal_year = Column(Integer, ForeignKey('fiscal_year.fiscal_year'), primary_key=True)

    fiscal_year_relation = relationship(
        "FiscalYear",
        back_populates="balance_sheet",
        primaryjoin="and_(BalanceSheet.symbol == FiscalYear.symbol, BalanceSheet.fiscal_year == FiscalYear.fiscal_year)"
    )

    moneda_reportada = Column(String, comment="Currency used in the report")
    cik = Column(String, comment="SEC Central Index Key identifier")
    fecha_presentacion = Column(String, comment="Filing submission date")
    fecha_aceptacion = Column(String, comment="Filing acceptance date")
    periodo = Column(String, comment="Reporting period type")

    efectivo_y_equivalentes = Column(Float, comment="Cash and cash equivalents")
    inversiones_corto_plazo = Column(Float, comment="Short-term investments")
    efectivo_y_inversiones_corto_plazo = Column(
        Float,
        comment="Total cash and short-term investments",
    )
    cuentas_por_cobrar = Column(Float, comment="Accounts receivable")
    inventario = Column(Float, comment="Inventory")
    otros_activos_corrientes = Column(Float, comment="Other current assets")
    total_activos_corrientes = Column(Float, comment="Total current assets")
    propiedad_planta_y_equipo = Column(
        Float,
        comment="Property, plant and equipment",
    )
    plusvalia = Column(Float, comment="Goodwill")
    activos_intangibles = Column(Float, comment="Intangible assets")
    plusvalia_y_intangibles = Column(
        Float,
        comment="Goodwill and intangible assets",
    )
    inversiones_largo_plazo = Column(Float, comment="Long-term investments")
    activos_por_impuestos = Column(Float, comment="Deferred tax assets")
    otros_activos_no_corrientes = Column(Float, comment="Other non-current assets")
    total_activos_no_corrientes = Column(Float, comment="Total non-current assets")
    otros_activos = Column(Float, comment="Other assets")
    total_activos = Column(Float, comment="Total assets")

    cuentas_por_pagar = Column(Float, comment="Accounts payable")
    deuda_corto_plazo = Column(Float, comment="Short-term debt")
    impuestos_por_pagar = Column(Float, comment="Taxes payable")
    ingresos_diferidos = Column(Float, comment="Deferred revenue")
    otros_pasivos_corrientes = Column(Float, comment="Other current liabilities")
    total_pasivos_corrientes = Column(Float, comment="Total current liabilities")
    deuda_largo_plazo = Column(Float, comment="Long-term debt")
    ingresos_diferidos_no_corrientes = Column(
        Float,
        comment="Non-current deferred revenue",
    )
    impuestos_diferidos_no_corrientes = Column(
        Float,
        comment="Deferred tax liabilities",
    )
    otros_pasivos_no_corrientes = Column(Float, comment="Other non-current liabilities")
    total_pasivos_no_corrientes = Column(
        Float,
        comment="Total non-current liabilities",
    )
    otros_pasivos = Column(Float, comment="Other liabilities")
    obligaciones_arrendamiento_capital = Column(
        Float,
        comment="Capital lease obligations",
    )
    total_pasivos = Column(Float, comment="Total liabilities")

    acciones_preferentes = Column(Float, comment="Preferred stock")
    acciones_comunes = Column(Float, comment="Common stock")
    ganancias_retenidas = Column(Float, comment="Retained earnings")
    ingresos_comprensivos_acumulados = Column(
        Float,
        comment="Accumulated other comprehensive income",
    )
    otro_total_patrimonio_accionistas = Column(
        Float,
        comment="Other stockholders' equity",
    )
    total_patrimonio_accionistas = Column(Float, comment="Total stockholders' equity")
    total_patrimonio = Column(Float, comment="Total equity")
    intereses_minoritarios = Column(Float, comment="Minority interest")
    total_pasivos_y_patrimonio_accionistas = Column(
        Float,
        comment="Total liabilities and stockholders' equity",
    )
    total_pasivos_y_patrimonio = Column(
        Float,
        comment="Total liabilities and equity",
    )

    total_inversiones = Column(Float, comment="Total investments")
    total_deuda = Column(Float, comment="Total debt")
    deuda_neta = Column(Float, comment="Net debt")

    enlace = Column(String, comment="Original filing link")
    enlace_final = Column(String, comment="Final filing link")
