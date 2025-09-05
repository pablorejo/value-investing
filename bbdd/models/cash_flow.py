from sqlalchemy import Column, Integer, String, Float, ForeignKey, and_
from sqlalchemy.orm import relationship
from conf import *
from ..db import Base


class CashFlow(Base):
    """Cash flow statement data for a company and fiscal year."""

    __tablename__ = 'cash_flow'

    symbol = Column(String, ForeignKey('fiscal_year.symbol'), primary_key=True)
    fiscal_year = Column(Integer, ForeignKey('fiscal_year.fiscal_year'), primary_key=True)

    fiscal_year_relation = relationship(
        "FiscalYear",
        back_populates="cash_flow",
        primaryjoin="and_(CashFlow.symbol == FiscalYear.symbol, CashFlow.fiscal_year == FiscalYear.fiscal_year)"
    )

    moneda_reportada = Column(String, comment="Currency used in the report")
    cik = Column(String, comment="SEC Central Index Key identifier")
    fecha_presentacion = Column(String, comment="Filing submission date")
    fecha_aceptacion = Column(String, comment="Filing acceptance date")
    periodo = Column(String, comment="Reporting period type")

    beneficio_neto = Column(Float, comment="Net income")
    depreciacion_y_amortizacion = Column(
        Float,
        comment="Depreciation and amortization",
    )
    impuestos_diferidos = Column(Float, comment="Deferred income taxes")
    compensacion_acciones = Column(Float, comment="Stock-based compensation")
    cambio_capital_trabajo = Column(Float, comment="Change in working capital")
    cuentas_por_cobrar = Column(Float, comment="Accounts receivable")
    inventario = Column(Float, comment="Inventory")
    cuentas_por_pagar = Column(Float, comment="Accounts payable")
    otro_capital_trabajo = Column(Float, comment="Other working capital")
    otros_items_no_efectivo = Column(Float, comment="Other non-cash items")
    flujo_operativo_neto = Column(
        Float,
        comment="Net cash from operating activities",
    )

    inversiones_propiedad_planta_y_equipo = Column(
        Float,
        comment="Capital expenditures for property, plant and equipment",
    )
    adquisiciones_netas = Column(Float, comment="Net acquisitions")
    compras_inversiones = Column(Float, comment="Purchases of investments")
    ventas_vencimientos_inversiones = Column(
        Float,
        comment="Sales and maturities of investments",
    )
    otras_actividades_inversion = Column(Float, comment="Other investing activities")
    flujo_inversion_neto = Column(Float, comment="Net cash from investing activities")

    reembolso_deuda = Column(Float, comment="Debt repayment")
    emision_acciones_comunes = Column(Float, comment="Issuance of common stock")
    recompra_acciones_comunes = Column(Float, comment="Repurchase of common stock")
    dividendos_pagados = Column(Float, comment="Dividends paid")
    otras_actividades_financieras = Column(Float, comment="Other financing activities")
    flujo_financiacion_neto = Column(Float, comment="Net cash from financing activities")

    efecto_cambios_divisas = Column(Float, comment="Effect of exchange rate changes")
    variacion_neta_flujo_caja = Column(Float, comment="Net change in cash")
    saldo_efectivo_inicio = Column(Float, comment="Cash at beginning of period")
    saldo_efectivo_cierre = Column(Float, comment="Cash at end of period")
    flujo_libre_caja = Column(Float, comment="Free cash flow")

    enlace = Column(String, comment="Original filing link")
    enlace_final = Column(String, comment="Final filing link")
