"""CRUD utilities for persisting and querying financial data."""

from sqlalchemy.orm import Session
from sqlalchemy import and_
from conf import *
from typing import Any, Dict, Optional
import pandas as pd
from .models import (
    CashFlow,
    BalanceSheet,
    IncomeStatement,
    Company,
    FiscalYear,
)
from .utils import capture_db_errors


@capture_db_errors
def save_cash_flow(session: Session, report: Dict[str, Any]) -> None:
    """Persist a cash flow report.

    Parameters
    ----------
    session:
        Active SQLAlchemy session used for persistence.
    report:
        Raw cash flow data as returned by the API.
    """
    nuevo_cash_flow = CashFlow(
        symbol=report.get('symbol', None),
        fiscal_year=report.get('calendarYear', None),
        moneda_reportada=report.get('reportedCurrency', None),
        cik=report.get('cik', None),
        fecha_presentacion=report.get('fillingDate', None),
        fecha_aceptacion=report.get('acceptedDate', None),
        periodo=report.get('period', None),
        beneficio_neto=report.get("netIncome", None),
        depreciacion_y_amortizacion=report.get('depreciationAndAmortization', None),
        impuestos_diferidos=report.get('deferredIncomeTax', None),
        compensacion_acciones=report.get('stockBasedCompensation', None),
        cambio_capital_trabajo=report.get('changeInWorkingCapital', None),
        cuentas_por_cobrar=report.get('accountsReceivables', None),
        inventario=report.get('inventory', None),
        cuentas_por_pagar=report.get('accountsPayables', None),
        otro_capital_trabajo=report.get('otherWorkingCapital', None),
        otros_items_no_efectivo=report.get('otherNonCashItems', None),
        flujo_operativo_neto=report.get('netCashProvidedByOperatingActivities', None),
        inversiones_propiedad_planta_y_equipo=report.get('investmentsInPropertyPlantAndEquipment', None),
        adquisiciones_netas=report.get('acquisitionsNet', None),
        compras_inversiones=report.get('purchasesOfInvestments', None),
        ventas_vencimientos_inversiones=report.get('salesMaturitiesOfInvestments', None),
        otras_actividades_inversion=report.get('otherInvestingActivites', None),
        flujo_inversion_neto=report.get('netCashUsedForInvestingActivites', None),
        reembolso_deuda=report.get('debtRepayment', None),
        emision_acciones_comunes=report.get('commonStockIssued', None),
        recompra_acciones_comunes=report.get('commonStockRepurchased', None),
        dividendos_pagados=report.get('dividendsPaid', None),
        otras_actividades_financieras=report.get('otherFinancingActivites', None),
        flujo_financiacion_neto=report.get('netCashUsedProvidedByFinancingActivities', None),
        efecto_cambios_divisas=report.get('effectOfForexChangesOnCash', None),
        variacion_neta_flujo_caja=report.get('netChangeInCash', None),
        saldo_efectivo_inicio=report.get('cashAtBeginningOfPeriod', None),
        saldo_efectivo_cierre=report.get('cashAtEndOfPeriod', None),
        flujo_libre_caja=report.get('freeCashFlow', None),
        enlace=report.get('link', None),
        enlace_final=report.get('finalLink', None),
    )
    session.add(nuevo_cash_flow)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error saving cash flow: {e}")


@capture_db_errors
def save_balance_sheet(session: Session, report: Dict[str, Any]) -> None:
    """Persist a balance sheet report.

    Parameters
    ----------
    session:
        Active SQLAlchemy session used for persistence.
    report:
        Raw balance sheet data as returned by the API.
    """
    nuevo_balance = BalanceSheet(
        symbol=report.get('symbol', None),
        fiscal_year=report.get('calendarYear', None),
        moneda_reportada=report.get('reportedCurrency', None),
        cik=report.get('cik', None),
        fecha_presentacion=report.get('fillingDate', None),
        fecha_aceptacion=report.get('acceptedDate', None),
        periodo=report.get('period', None),
        efectivo_y_equivalentes=report.get('cashAndCashEquivalents', None),
        inversiones_corto_plazo=report.get('shortTermInvestments', None),
        efectivo_y_inversiones_corto_plazo=report.get('cashAndShortTermInvestments', None),
        cuentas_por_cobrar=report.get('netReceivables', None),
        inventario=report.get('inventory', None),
        otros_activos_corrientes=report.get('otherCurrentAssets', None),
        total_activos_corrientes=report.get('totalCurrentAssets', None),
        propiedad_planta_y_equipo=report.get('propertyPlantEquipmentNet', None),
        plusvalia=report.get('goodwill', None),
        activos_intangibles=report.get('intangibleAssets', None),
        plusvalia_y_intangibles=report.get('goodwillAndIntangibleAssets', None),
        inversiones_largo_plazo=report.get('longTermInvestments', None),
        activos_por_impuestos=report.get('taxAssets', None),
        otros_activos_no_corrientes=report.get('otherNonCurrentAssets', None),
        total_activos_no_corrientes=report.get('totalNonCurrentAssets', None),
        otros_activos=report.get('otherAssets', None),
        total_activos=report.get('totalAssets', None),
        cuentas_por_pagar=report.get('accountPayables', None),
        deuda_corto_plazo=report.get('shortTermDebt', None),
        impuestos_por_pagar=report.get('taxPayables', None),
        ingresos_diferidos=report.get('deferredRevenue', None),
        otros_pasivos_corrientes=report.get('otherCurrentLiabilities', None),
        total_pasivos_corrientes=report.get('totalCurrentLiabilities', None),
        deuda_largo_plazo=report.get('longTermDebt', None),
        ingresos_diferidos_no_corrientes=report.get('deferredRevenueNonCurrent', None),
        impuestos_diferidos_no_corrientes=report.get('deferredTaxLiabilitiesNonCurrent', None),
        otros_pasivos_no_corrientes=report.get('otherNonCurrentLiabilities', None),
        total_pasivos_no_corrientes=report.get('totalNonCurrentLiabilities', None),
        otros_pasivos=report.get('otherLiabilities', None),
        obligaciones_arrendamiento_capital=report.get('capitalLeaseObligations', None),
        total_pasivos=report.get('totalLiabilities', None),
        acciones_preferentes=report.get('preferredStock', None),
        acciones_comunes=report.get('commonStock', None),
        ganancias_retenidas=report.get('retainedEarnings', None),
        ingresos_comprensivos_acumulados=report.get('accumulatedOtherComprehensiveIncomeLoss', None),
        otro_total_patrimonio_accionistas=report.get('othertotalStockholdersEquity', None),
        total_patrimonio_accionistas=report.get('totalStockholdersEquity', None),
        total_patrimonio=report.get('totalEquity', None),
        intereses_minoritarios=report.get('minorityInterest', None),
        total_pasivos_y_patrimonio_accionistas=report.get('totalLiabilitiesAndStockholdersEquity', None),
        total_pasivos_y_patrimonio=report.get('totalLiabilitiesAndTotalEquity', None),
        total_inversiones=report.get('totalInvestments', None),
        total_deuda=report.get('totalDebt', None),
        deuda_neta=report.get('netDebt', None),
        enlace=report.get('link', None),
        enlace_final=report.get('finalLink', None),
    )
    session.add(nuevo_balance)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error saving balance sheet: {e}")


@capture_db_errors
def save_income_statement(session: Session, report: Dict[str, Any]) -> IncomeStatement:
    """Persist an income statement report.

    Parameters
    ----------
    session:
        Active SQLAlchemy session used for persistence.
    report:
        Raw income statement data from the API.

    Returns
    -------
    IncomeStatement
        The newly created ORM instance.
    """
    nueva_cuenta_resultados = IncomeStatement(
        symbol=report.get('symbol', None),
        fiscal_year=report.get('calendarYear', None),
        moneda_reportada=report.get('reportedCurrency', None),
        cik=report.get('cik', None),
        fecha_presentacion=report.get('fillingDate', None),
        fecha_aceptacion=report.get('acceptedDate', None),
        periodo=report.get('period', None),
        ingresos=report.get('revenue', None),
        costo_ingresos=report.get('costOfRevenue', None),
        ganancia_bruta=report.get('grossProfit', None),
        margen_ganancia_bruta=report.get('grossProfitRatio', None),
        gastos_investigacion_desarrollo=report.get('researchAndDevelopmentExpenses', None),
        gastos_generales_administrativos=report.get('generalAndAdministrativeExpenses', None),
        gastos_ventas_marketing=report.get('sellingAndMarketingExpenses', None),
        gastos_operativos=report.get('operatingExpenses', None),
        otros_gastos=report.get('otherExpenses', None),
        coste_y_gastos_totales=report.get('costAndExpenses', None),
        ingresos_por_intereses=report.get('interestIncome', None),
        gastos_por_intereses=report.get('interestExpense', None),
        depreciaciones_amortizaciones=report.get('depreciationAndAmortization', None),
        ebitda=report.get('ebitda', None),
        margen_ebitda=report.get('ebitdaratio', None),
        ingreso_operativo=report.get('operatingIncome', None),
        margen_ingreso_operativo=report.get('operatingIncomeRatio', None),
        otros_ingresos_gastos_netos=report.get('totalOtherIncomeExpensesNet', None),
        ingreso_antes_impuestos=report.get('incomeBeforeTax', None),
        margen_ingreso_antes_impuestos=report.get('incomeBeforeTaxRatio', None),
        impuestos=report.get('incomeTaxExpense', None),
        ingreso_neto=report.get('netIncome', None),
        margen_ingreso_neto=report.get('netIncomeRatio', None),
        eps=report.get('eps', None),
        eps_diluido=report.get('epsdiluted', None),
        acciones_promedio=report.get('weightedAverageShsOut', None),
        acciones_promedio_diluidas=report.get('weightedAverageShsOutDil', None),
        enlace=report.get('link', None),
        enlace_final=report.get('finalLink', None),
    )
    session.add(nueva_cuenta_resultados)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error saving income statement: {e}")


@capture_db_errors
def save_company(session: Session, company: Dict[str, Any]) -> Company:
    """Persist basic company information.

    Parameters
    ----------
    session:
        Active SQLAlchemy session used for persistence.
    company:
        Company profile data returned by the API.

    Returns
    -------
    Company
        The newly created company instance.
    """
    nueva_empresa = Company(
        symbol=company.get('symbol'),
        company_name=company.get('companyName'),
        price=company.get('price'),
        exchange=company.get('exchange'),
        exchange_short_name=company.get('exchangeShortName'),
        sector=company.get('sector'),
    )
    session.add(nueva_empresa)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error saving company: {e}")
    return nueva_empresa


@capture_db_errors
def save_fiscal_year(session: Session, symbol: str, year: int, prices) -> Optional[FiscalYear]:
    """Store yearly price metrics for a company.

    Parameters
    ----------
    session:
        Active SQLAlchemy session used for persistence.
    symbol:
        Ticker symbol of the company.
    year:
        Fiscal year of the data.
    prices:
        Dictionary with price statistics.

    Returns
    -------
    Optional[FiscalYear]
        The existing or newly created fiscal year instance.
    """
    existing = session.query(FiscalYear).filter_by(symbol=symbol, fiscal_year=year).first()
    if existing:
        return existing

    new_fiscal_year = FiscalYear(
        symbol=symbol,
        fiscal_year=year,
        price_first=prices.get('open', None),
        price_last=prices.get('close', None),
        price_min=prices.get('low', None),
        price_max=prices.get('high', None),
        price_avg=prices.get('close_mean', None),
        price_std=prices.get('close_std', None),
        price_var=prices.get('close_var', None),
        price_change=prices.get('price_change', None),
        price_change_pct=prices.get('price_change_pct', None),
        price_change_pct_1y=prices.get('price_change_pct_1y', None),
        price_change_pct_1m=prices.get('price_change_pct_1m', None),
        price_change_pct_3m=prices.get('price_change_pct_3m', None),
        price_change_pct_6m=prices.get('price_change_pct_6m', None),
    )
    session.add(new_fiscal_year)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error saving fiscal year: {e}")
    return new_fiscal_year


def extract_all_data(session: Session) -> pd.DataFrame:
    """Return a DataFrame joining all financial tables for analysis.

    Parameters
    ----------
    session:
        Active SQLAlchemy session used for querying.

    Returns
    -------
    pandas.DataFrame
        Data combining company information with cash flow, balance
        sheet and income statement metrics.
    """
    query = session.query(
        Company.symbol,
        FiscalYear.fiscal_year,
        CashFlow.moneda_reportada,
        CashFlow.cik,
        CashFlow.fecha_presentacion,
        CashFlow.fecha_aceptacion,
        CashFlow.periodo,
        CashFlow.beneficio_neto,
        CashFlow.depreciacion_y_amortizacion,
        CashFlow.impuestos_diferidos,
        CashFlow.compensacion_acciones,
        CashFlow.cambio_capital_trabajo,
        CashFlow.cuentas_por_cobrar,
        CashFlow.inventario,
        CashFlow.cuentas_por_pagar,
        CashFlow.otro_capital_trabajo,
        CashFlow.otros_items_no_efectivo,
        CashFlow.flujo_operativo_neto,
        CashFlow.inversiones_propiedad_planta_y_equipo,
        CashFlow.adquisiciones_netas,
        CashFlow.compras_inversiones,
        CashFlow.ventas_vencimientos_inversiones,
        CashFlow.otras_actividades_inversion,
        CashFlow.flujo_inversion_neto,
        CashFlow.reembolso_deuda,
        CashFlow.emision_acciones_comunes,
        CashFlow.recompra_acciones_comunes,
        CashFlow.dividendos_pagados,
        CashFlow.otras_actividades_financieras,
        CashFlow.flujo_financiacion_neto,
        CashFlow.efecto_cambios_divisas,
        CashFlow.variacion_neta_flujo_caja,
        CashFlow.saldo_efectivo_inicio,
        CashFlow.saldo_efectivo_cierre,
        CashFlow.flujo_libre_caja,
        IncomeStatement.ingresos,
        IncomeStatement.costo_ingresos,
        IncomeStatement.coste_de_las_ventas,
        IncomeStatement.ganancia_bruta,
        IncomeStatement.margen_ganancia_bruta,
        IncomeStatement.gastos_investigacion_desarrollo,
        IncomeStatement.gastos_generales_administrativos,
        IncomeStatement.gastos_ventas_marketing,
        IncomeStatement.gastos_operativos,
        IncomeStatement.otros_gastos,
        IncomeStatement.coste_y_gastos_totales,
        IncomeStatement.ingresos_por_intereses,
        IncomeStatement.gastos_por_intereses,
        IncomeStatement.depreciaciones_amortizaciones,
        IncomeStatement.ebitda,
        IncomeStatement.margen_ebitda,
        IncomeStatement.ingreso_operativo,
        IncomeStatement.margen_ingreso_operativo,
        IncomeStatement.otros_ingresos_gastos_netos,
        IncomeStatement.ingreso_antes_impuestos,
        IncomeStatement.margen_ingreso_antes_impuestos,
        IncomeStatement.impuestos,
        IncomeStatement.ingreso_neto,
        IncomeStatement.margen_ingreso_neto,
        IncomeStatement.eps,
        IncomeStatement.eps_diluido,
        IncomeStatement.acciones_promedio,
        IncomeStatement.acciones_promedio_diluidas,
        BalanceSheet.efectivo_y_equivalentes,
        BalanceSheet.inversiones_corto_plazo,
        BalanceSheet.efectivo_y_inversiones_corto_plazo,
        BalanceSheet.cuentas_por_cobrar,
        BalanceSheet.inventario,
        BalanceSheet.otros_activos_corrientes,
        BalanceSheet.total_activos_corrientes,
        BalanceSheet.propiedad_planta_y_equipo,
        BalanceSheet.plusvalia,
        BalanceSheet.activos_intangibles,
        BalanceSheet.plusvalia_y_intangibles,
        BalanceSheet.inversiones_largo_plazo,
        BalanceSheet.activos_por_impuestos,
        BalanceSheet.otros_activos_no_corrientes,
        BalanceSheet.total_activos_no_corrientes,
        BalanceSheet.otros_activos,
        BalanceSheet.total_activos,
        BalanceSheet.cuentas_por_pagar,
        BalanceSheet.deuda_corto_plazo,
        BalanceSheet.impuestos_por_pagar,
        BalanceSheet.ingresos_diferidos,
        BalanceSheet.otros_pasivos_corrientes,
        BalanceSheet.total_pasivos_corrientes,
        BalanceSheet.deuda_largo_plazo,
        BalanceSheet.ingresos_diferidos_no_corrientes,
        BalanceSheet.impuestos_diferidos_no_corrientes,
        BalanceSheet.otros_pasivos_no_corrientes,
        BalanceSheet.total_pasivos_no_corrientes,
        BalanceSheet.otros_pasivos,
        BalanceSheet.obligaciones_arrendamiento_capital,
        BalanceSheet.total_pasivos,
        BalanceSheet.acciones_preferentes,
        BalanceSheet.acciones_comunes,
        BalanceSheet.ganancias_retenidas,
        BalanceSheet.ingresos_comprensivos_acumulados,
        BalanceSheet.otro_total_patrimonio_accionistas,
        BalanceSheet.total_patrimonio_accionistas,
        BalanceSheet.total_patrimonio,
        BalanceSheet.intereses_minoritarios,
        BalanceSheet.total_pasivos_y_patrimonio_accionistas,
        BalanceSheet.total_pasivos_y_patrimonio,
        BalanceSheet.total_inversiones,
        BalanceSheet.total_deuda,
        BalanceSheet.deuda_neta,
    ).join(FiscalYear, Company.symbol == FiscalYear.symbol)\
     .join(CashFlow, and_(FiscalYear.symbol == CashFlow.symbol, FiscalYear.fiscal_year == CashFlow.fiscal_year))\
     .join(IncomeStatement, and_(FiscalYear.symbol == IncomeStatement.symbol, FiscalYear.fiscal_year == IncomeStatement.fiscal_year))\
     .join(BalanceSheet, and_(FiscalYear.symbol == BalanceSheet.symbol, FiscalYear.fiscal_year == BalanceSheet.fiscal_year))

    df = pd.read_sql(query.statement, session.bind)
    df = df.loc[:, ~df.columns.duplicated()]
    return df
