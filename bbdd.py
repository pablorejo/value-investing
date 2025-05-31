from sqlalchemy import create_engine, MetaData, Column, Integer, String, Float, ForeignKey, PrimaryKeyConstraint, and_
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base

from conf import *
from typing import Any, Dict, Optional

# Configuración del logger raíz
logging.basicConfig(level=logging.WARNING)

# Configuración específica para SQLAlchemy
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)  # Nivel global de SQLAlchemy
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)  # Logs de consultas SQL
logging.getLogger('sqlalchemy.pool').setLevel(logging.ERROR)  # Logs del pool de conexiones

# Limpiar handlers no deseados de los loggers
logging.getLogger('sqlalchemy').handlers = []
logging.getLogger('sqlalchemy.engine').handlers = []
logging.getLogger('sqlalchemy.pool').handlers = []

file_handler = logging.FileHandler('sqlalchemy.log')
file_handler.setLevel(logging.WARNING)
logging.getLogger('sqlalchemy').addHandler(file_handler)

# Crear el motor para la base de datos SQLite
engine = create_engine(f'sqlite:///{DATA_BASE}', echo=True)
Base = declarative_base()

def dividir(a: float, b: float) -> Optional[float]:
    return a / b if b != 0 else None

def capturar_errores_bbdd(func):
    def error(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.warning(f"Se produjo una excepción: {e}")
            logging.warning(traceback.format_exc())  # Cambiado a format_exc para capturar el traceback como texto
            return None  # Devuelve None o un valor predeterminado si ocurre un error
    return error


class CashFlow(Base):
    __tablename__ = 'cash_flow'

    # Llaves primarias y relaciones
    symbol = Column(String, ForeignKey('anio_fiscal.symbol'), primary_key=True)
    anio_fiscal = Column(Integer, ForeignKey('anio_fiscal.anio_fiscal'), primary_key=True)

    # Relación con la tabla Año Fiscal
    anio_fiscal_relacion = relationship("AnioFiscal", back_populates="cash_flow",
                                        primaryjoin="and_(CashFlow.symbol == AnioFiscal.symbol, CashFlow.anio_fiscal == AnioFiscal.anio_fiscal)")

    # Atributos del flujo de caja (nuevos campos añadidos)
    moneda_reportada = Column(String)  # Moneda reportada
    cik = Column(String)  # Código CIK
    fecha_presentacion = Column(String)  # Fecha de presentación
    fecha_aceptacion = Column(String)  # Fecha de aceptación
    periodo = Column(String)  # Periodo reportado

    # Atributos del flujo de caja
    beneficio_neto = Column(Float)  # netIncome
    depreciacion_y_amortizacion = Column(Float)  # depreciationAndAmortization
    impuestos_diferidos = Column(Float)  # deferredIncomeTax
    compensacion_acciones = Column(Float)  # stockBasedCompensation
    cambio_capital_trabajo = Column(Float)  # changeInWorkingCapital
    cuentas_por_cobrar = Column(Float)  # accountsReceivables
    inventario = Column(Float)  # inventory
    cuentas_por_pagar = Column(Float)  # accountsPayables
    otro_capital_trabajo = Column(Float)  # otherWorkingCapital
    otros_items_no_efectivo = Column(Float)  # otherNonCashItems
    flujo_operativo_neto = Column(Float)  # netCashProvidedByOperatingActivities

    # Flujo de caja de inversión
    inversiones_propiedad_planta_y_equipo = Column(Float)  # investmentsInPropertyPlantAndEquipment
    adquisiciones_netas = Column(Float)  # acquisitionsNet
    compras_inversiones = Column(Float)  # purchasesOfInvestments
    ventas_vencimientos_inversiones = Column(Float)  # salesMaturitiesOfInvestments
    otras_actividades_inversion = Column(Float)  # otherInvestingActivites
    flujo_inversion_neto = Column(Float)  # netCashUsedForInvestingActivites

    # Flujo de caja de financiación
    reembolso_deuda = Column(Float)  # debtRepayment
    emision_acciones_comunes = Column(Float)  # commonStockIssued
    recompra_acciones_comunes = Column(Float)  # commonStockRepurchased
    dividendos_pagados = Column(Float)  # dividendsPaid
    otras_actividades_financieras = Column(Float)  # otherFinancingActivites
    flujo_financiacion_neto = Column(Float)  # netCashUsedProvidedByFinancingActivities

    # Otros valores de flujo de caja
    efecto_cambios_divisas = Column(Float)  # effectOfForexChangesOnCash
    variacion_neta_flujo_caja = Column(Float)  # netChangeInCash
    saldo_efectivo_inicio = Column(Float)  # cashAtBeginningOfPeriod
    saldo_efectivo_cierre = Column(Float)  # cashAtEndOfPeriod
    flujo_libre_caja = Column(Float)  # freeCashFlow

    # Enlaces al reporte
    enlace = Column(String)  # link
    enlace_final = Column(String)  # finalLink

@capturar_errores_bbdd
def guardar_cash_flow(session: Session, report: Dict[str, Any]) -> None:
    # Crear una instancia de CashFlow
    nuevo_cash_flow = CashFlow(
        symbol=report.get('symbol', None),  # Símbolo de la empresa (ticker)
        anio_fiscal=report.get('calendarYear', None),  # Año fiscal


        # fecha=report.get('date', None),  # Fecha del reporte
        moneda_reportada=report.get('reportedCurrency', None),  # Moneda utilizada en el reporte
        cik=report.get('cik', None),  # Código CIK
        fecha_presentacion=report.get('fillingDate', None),  # Fecha de presentación
        fecha_aceptacion=report.get('acceptedDate', None),  # Fecha de aceptación
        periodo=report.get('period', None),  # Periodo reportado (por ejemplo, FY)

        # Flujo de caja operativo
        beneficio_neto=report.get("netIncome", None),  # Beneficio neto
        depreciacion_y_amortizacion=report.get('depreciationAndAmortization', None),  # Depreciaciones y amortizaciones
        impuestos_diferidos=report.get('deferredIncomeTax', None),  # Impuestos diferidos
        compensacion_acciones=report.get('stockBasedCompensation', None),  # Compensación basada en acciones
        cambio_capital_trabajo=report.get('changeInWorkingCapital', None),  # Cambio en el capital de trabajo
        cuentas_por_cobrar=report.get('accountsReceivables', None),  # Cuentas por cobrar
        inventario=report.get('inventory', None),  # Inventario
        cuentas_por_pagar=report.get('accountsPayables', None),  # Cuentas por pagar
        otro_capital_trabajo=report.get('otherWorkingCapital', None),  # Otros cambios en capital de trabajo
        otros_items_no_efectivo=report.get('otherNonCashItems', None),  # Otros ítems no en efectivo
        flujo_operativo_neto=report.get('netCashProvidedByOperatingActivities', None),  # Flujo de caja operativo neto

        # Flujo de caja de inversión
        inversiones_propiedad_planta_y_equipo=report.get('investmentsInPropertyPlantAndEquipment', None),  # Inversiones en propiedad, planta y equipo
        adquisiciones_netas=report.get('acquisitionsNet', None),  # Adquisiciones netas
        compras_inversiones=report.get('purchasesOfInvestments', None),  # Compras de inversiones
        ventas_vencimientos_inversiones=report.get('salesMaturitiesOfInvestments', None),  # Ventas y vencimientos de inversiones
        otras_actividades_inversion=report.get('otherInvestingActivites', None),  # Otras actividades de inversión
        flujo_inversion_neto=report.get('netCashUsedForInvestingActivites', None),  # Flujo neto de actividades de inversión

        # Flujo de caja de financiación
        reembolso_deuda=report.get('debtRepayment', None),  # Reembolso de deuda
        emision_acciones_comunes=report.get('commonStockIssued', None),  # Emisión de acciones comunes
        recompra_acciones_comunes=report.get('commonStockRepurchased', None),  # Recompra de acciones comunes
        dividendos_pagados=report.get('dividendsPaid', None),  # Dividendos pagados
        otras_actividades_financieras=report.get('otherFinancingActivites', None),  # Otras actividades financieras
        flujo_financiacion_neto=report.get('netCashUsedProvidedByFinancingActivities', None),  # Flujo neto de actividades de financiación

        # Otros valores de flujo de caja
        efecto_cambios_divisas=report.get('effectOfForexChangesOnCash', None),  # Efecto de cambios en el tipo de cambio sobre el efectivo
        variacion_neta_flujo_caja=report.get('netChangeInCash', None),  # Variación neta en el flujo de caja
        saldo_efectivo_inicio=report.get('cashAtBeginningOfPeriod', None),  # Saldo efectivo al inicio del período
        saldo_efectivo_cierre=report.get('cashAtEndOfPeriod', None),  # Saldo efectivo al cierre del período
        flujo_libre_caja=report.get('freeCashFlow', None),  # Flujo libre de caja

        # Enlaces al reporte
        enlace=report.get('link', None),  # Enlace al reporte general
        enlace_final=report.get('finalLink', None)  # Enlace directo al reporte
    )

    # Añadir a la sesión
    session.add(nuevo_cash_flow)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error al guardar cash flow: {e}")

# Definir la tabla Balance General
class BalanceGeneral(Base):
    __tablename__ = 'balance_general'

    # Llaves primarias y relaciones
    symbol = Column(String, ForeignKey('anio_fiscal.symbol'), primary_key=True)
    anio_fiscal = Column(Integer, ForeignKey('anio_fiscal.anio_fiscal'), primary_key=True)

    # Relación con la tabla Año Fiscal
    anio_fiscal_relacion = relationship("AnioFiscal", back_populates="balance_general",
                                        primaryjoin="and_(BalanceGeneral.symbol == AnioFiscal.symbol, BalanceGeneral.anio_fiscal == AnioFiscal.anio_fiscal)")

    # Atributos del balance general (nuevos campos añadidos)
    moneda_reportada = Column(String)  # Moneda reportada
    cik = Column(String)  # Código CIK
    fecha_presentacion = Column(String)  # Fecha de presentación
    fecha_aceptacion = Column(String)  # Fecha de aceptación
    periodo = Column(String)  # Periodo reportado

    # Activos
    efectivo_y_equivalentes = Column(Float)  # cashAndCashEquivalents
    inversiones_corto_plazo = Column(Float)  # shortTermInvestments
    efectivo_y_inversiones_corto_plazo = Column(Float)  # cashAndShortTermInvestments
    cuentas_por_cobrar = Column(Float)  # netReceivables
    inventario = Column(Float)  # inventory
    otros_activos_corrientes = Column(Float)  # otherCurrentAssets
    total_activos_corrientes = Column(Float)  # totalCurrentAssets
    propiedad_planta_y_equipo = Column(Float)  # propertyPlantEquipmentNet
    plusvalia = Column(Float)  # goodwill
    activos_intangibles = Column(Float)  # intangibleAssets
    plusvalia_y_intangibles = Column(Float)  # goodwillAndIntangibleAssets
    inversiones_largo_plazo = Column(Float)  # longTermInvestments
    activos_por_impuestos = Column(Float)  # taxAssets
    otros_activos_no_corrientes = Column(Float)  # otherNonCurrentAssets
    total_activos_no_corrientes = Column(Float)  # totalNonCurrentAssets
    otros_activos = Column(Float)  # otherAssets
    total_activos = Column(Float)  # totalAssets

    # Pasivos
    cuentas_por_pagar = Column(Float)  # accountPayables
    deuda_corto_plazo = Column(Float)  # shortTermDebt
    impuestos_por_pagar = Column(Float)  # taxPayables
    ingresos_diferidos = Column(Float)  # deferredRevenue
    otros_pasivos_corrientes = Column(Float)  # otherCurrentLiabilities
    total_pasivos_corrientes = Column(Float)  # totalCurrentLiabilities
    deuda_largo_plazo = Column(Float)  # longTermDebt
    ingresos_diferidos_no_corrientes = Column(Float)  # deferredRevenueNonCurrent
    impuestos_diferidos_no_corrientes = Column(Float)  # deferredTaxLiabilitiesNonCurrent
    otros_pasivos_no_corrientes = Column(Float)  # otherNonCurrentLiabilities
    total_pasivos_no_corrientes = Column(Float)  # totalNonCurrentLiabilities
    otros_pasivos = Column(Float)  # otherLiabilities
    obligaciones_arrendamiento_capital = Column(Float)  # capitalLeaseObligations
    total_pasivos = Column(Float)  # totalLiabilities

    # Patrimonio
    acciones_preferentes = Column(Float)  # preferredStock
    acciones_comunes = Column(Float)  # commonStock
    ganancias_retenidas = Column(Float)  # retainedEarnings
    ingresos_comprensivos_acumulados = Column(Float)  # accumulatedOtherComprehensiveIncomeLoss
    otro_total_patrimonio_accionistas = Column(Float)  # othertotalStockholdersEquity
    total_patrimonio_accionistas = Column(Float)  # totalStockholdersEquity
    total_patrimonio = Column(Float)  # totalEquity
    intereses_minoritarios = Column(Float)  # minorityInterest
    total_pasivos_y_patrimonio_accionistas = Column(Float)  # totalLiabilitiesAndStockholdersEquity
    total_pasivos_y_patrimonio = Column(Float)  # totalLiabilitiesAndTotalEquity

    # Otros valores
    total_inversiones = Column(Float)  # totalInvestments
    total_deuda = Column(Float)  # totalDebt
    deuda_neta = Column(Float)  # netDebt

    # Enlaces
    enlace = Column(String)  # link
    enlace_final = Column(String)  # finalLink

@capturar_errores_bbdd
def guardar_balance_general(session: Session, report: Dict[str, Any]) -> None:
    # Crear una instancia de BalanceGeneral
    nuevo_balance = BalanceGeneral(
        symbol=report.get('symbol', None),  # Símbolo de la empresa (ticker)
        anio_fiscal=report.get('calendarYear', None),  # Año fiscal

        # fecha=report.get('date', None),  # Fecha del balance
        moneda_reportada=report.get('reportedCurrency', None),  # Moneda utilizada en el reporte
        cik=report.get('cik', None),  # Código CIK
        fecha_presentacion=report.get('fillingDate', None),  # Fecha de presentación
        fecha_aceptacion=report.get('acceptedDate', None),  # Fecha de aceptación
        periodo=report.get('period', None),  # Periodo reportado (por ejemplo, FY)

        # Activos
        efectivo_y_equivalentes=report.get('cashAndCashEquivalents', None),  # Efectivo y equivalentes
        inversiones_corto_plazo=report.get('shortTermInvestments', None),  # Inversiones a corto plazo
        efectivo_y_inversiones_corto_plazo=report.get('cashAndShortTermInvestments', None),  # Efectivo e inversiones a corto plazo
        cuentas_por_cobrar=report.get('netReceivables', None),  # Cuentas por cobrar
        inventario=report.get('inventory', None),  # Inventario
        otros_activos_corrientes=report.get('otherCurrentAssets', None),  # Otros activos corrientes
        total_activos_corrientes=report.get('totalCurrentAssets', None),  # Total de activos corrientes
        propiedad_planta_y_equipo=report.get('propertyPlantEquipmentNet', None),  # Propiedad, planta y equipo neto
        plusvalia=report.get('goodwill', None),  # Plusvalía
        activos_intangibles=report.get('intangibleAssets', None),  # Activos intangibles
        plusvalia_y_intangibles=report.get('goodwillAndIntangibleAssets', None),  # Plusvalía y activos intangibles
        inversiones_largo_plazo=report.get('longTermInvestments', None),  # Inversiones a largo plazo
        activos_por_impuestos=report.get('taxAssets', None),  # Activos por impuestos
        otros_activos_no_corrientes=report.get('otherNonCurrentAssets', None),  # Otros activos no corrientes
        total_activos_no_corrientes=report.get('totalNonCurrentAssets', None),  # Total de activos no corrientes
        otros_activos=report.get('otherAssets', None),  # Otros activos
        total_activos=report.get('totalAssets', None),  # Total de activos

        # Pasivos
        cuentas_por_pagar=report.get('accountPayables', None),  # Cuentas por pagar
        deuda_corto_plazo=report.get('shortTermDebt', None),  # Deuda a corto plazo
        impuestos_por_pagar=report.get('taxPayables', None),  # Impuestos por pagar
        ingresos_diferidos=report.get('deferredRevenue', None),  # Ingresos diferidos
        otros_pasivos_corrientes=report.get('otherCurrentLiabilities', None),  # Otros pasivos corrientes
        total_pasivos_corrientes=report.get('totalCurrentLiabilities', None),  # Total de pasivos corrientes
        deuda_largo_plazo=report.get('longTermDebt', None),  # Deuda a largo plazo
        ingresos_diferidos_no_corrientes=report.get('deferredRevenueNonCurrent', None),  # Ingresos diferidos no corrientes
        impuestos_diferidos_no_corrientes=report.get('deferredTaxLiabilitiesNonCurrent', None),  # Impuestos diferidos no corrientes
        otros_pasivos_no_corrientes=report.get('otherNonCurrentLiabilities', None),  # Otros pasivos no corrientes
        total_pasivos_no_corrientes=report.get('totalNonCurrentLiabilities', None),  # Total de pasivos no corrientes
        otros_pasivos=report.get('otherLiabilities', None),  # Otros pasivos
        obligaciones_arrendamiento_capital=report.get('capitalLeaseObligations', None),  # Obligaciones de arrendamiento capital
        total_pasivos=report.get('totalLiabilities', None),  # Total de pasivos

        # Patrimonio
        acciones_preferentes=report.get('preferredStock', None),  # Acciones preferentes
        acciones_comunes=report.get('commonStock', None),  # Acciones comunes
        ganancias_retenidas=report.get('retainedEarnings', None),  # Ganancias retenidas
        ingresos_comprensivos_acumulados=report.get('accumulatedOtherComprehensiveIncomeLoss', None),  # Ingresos comprensivos acumulados
        otro_total_patrimonio_accionistas=report.get('othertotalStockholdersEquity', None),  # Otro total del patrimonio de los accionistas
        total_patrimonio_accionistas=report.get('totalStockholdersEquity', None),  # Total de patrimonio de los accionistas
        total_patrimonio=report.get('totalEquity', None),  # Total de patrimonio
        intereses_minoritarios=report.get('minorityInterest', None),  # Intereses minoritarios
        total_pasivos_y_patrimonio_accionistas=report.get('totalLiabilitiesAndStockholdersEquity', None),  # Total de pasivos y patrimonio de los accionistas
        total_pasivos_y_patrimonio=report.get('totalLiabilitiesAndTotalEquity', None),  # Total de pasivos y patrimonio

        # Otros valores
        total_inversiones=report.get('totalInvestments', None),  # Total de inversiones
        total_deuda=report.get('totalDebt', None),  # Total de deuda
        deuda_neta=report.get('netDebt', None),  # Deuda neta

        # Enlaces
        enlace=report.get('link', None),  # Enlace al reporte general
        enlace_final=report.get('finalLink', None)  # Enlace directo al reporte
    )

    # Añadir a la sesión
    session.add(nuevo_balance)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error al guardar balance general: {e}")

class CuentaDeResultados(Base):
    __tablename__ = 'cuenta_de_resultados'

    # Llaves primarias y relaciones
    symbol = Column(String, ForeignKey('anio_fiscal.symbol'), primary_key=True)
    anio_fiscal = Column(Integer, ForeignKey('anio_fiscal.anio_fiscal'), primary_key=True)

    # Relación con la tabla Año Fiscal (añadido foreign_keys)
    anio_fiscal_relacion = relationship("AnioFiscal", back_populates="cuenta_de_resultados",
                                        primaryjoin="and_(CuentaDeResultados.symbol == AnioFiscal.symbol, CuentaDeResultados.anio_fiscal == AnioFiscal.anio_fiscal)")

    # Otros atributos
    moneda_reportada = Column(String)
    cik = Column(Integer)
    fecha_presentacion = Column(String)  # Fecha de presentación
    fecha_aceptacion = Column(String)  # Fecha de aceptación
    periodo = Column(String)  # Periodo reportado

    # Atributos - Nuevos campos añadidos
    ingresos = Column(Float)  # Revenue
    costo_ingresos = Column(Float) 
    coste_de_las_ventas = Column(Float)  # Cost of revenue
    ganancia_bruta = Column(Float)  # Gross profit
    margen_ganancia_bruta = Column(Float)  # Gross profit ratio
    gastos_investigacion_desarrollo = Column(Float)  # R&D expenses
    gastos_generales_administrativos = Column(Float)  # General & admin expenses
    gastos_ventas_marketing = Column(Float)  # Selling & marketing expenses
    gastos_operativos = Column(Float)  # Operating expenses
    otros_gastos = Column(Float)  # Other expenses
    coste_y_gastos_totales = Column(Float)  # Total costs and expenses

    ingresos_por_intereses = Column(Float)  # Interest income
    gastos_por_intereses = Column(Float)  # Interest expense igual a gastos financieros
    depreciaciones_amortizaciones = Column(Float)  # Depreciation and amortization
    ebitda = Column(Float)  # EBITDA
    margen_ebitda = Column(Float)  # EBITDA margin

    ingreso_operativo = Column(Float)  # Operating income
    margen_ingreso_operativo = Column(Float)  # Operating income ratio
    otros_ingresos_gastos_netos = Column(Float)  # Total other income/expenses, net
    ingreso_antes_impuestos = Column(Float)  # Income before tax
    margen_ingreso_antes_impuestos = Column(Float)  # Income before tax ratio
    impuestos = Column(Float)  # Income tax expense
    ingreso_neto = Column(Float)  # Net income
    margen_ingreso_neto = Column(Float)  # Net income ratio

    eps = Column(Float)  # Earnings per share (EPS)
    eps_diluido = Column(Float)  # Diluted earnings per share (EPS diluted)

    acciones_promedio = Column(Integer)  # Weighted average shares outstanding
    acciones_promedio_diluidas = Column(Integer)  # Weighted average shares outstanding (diluted)

    enlace = Column(String)
    enlace_final = Column(String)
    # Campos calculados en tiempo de ejecución
    @property
    def beneficio_bruto(self):
        return self.ingresos - self.coste_de_las_ventas

    @property
    def resultado_operativo(self):
        return self.beneficio_bruto - self.gastos_operativos

    @property
    def resultado_explotacion(self):
        return self.resultado_operativo - self.depreciaciones_amortizaciones

    @property
    def beneficio_antes_impuestos(self):
        return self.resultado_explotacion - self.gastos_por_intereses

    @property
    def beneficio_neto(self):
        return self.beneficio_antes_impuestos - self.impuestos


@capturar_errores_bbdd
def guardar_cuenta_resultados(session: Session, report: Dict[str, Any]) -> CuentaDeResultados:
    # Crear una instancia de CuentaDeResultados
    nueva_cuenta_resultados = CuentaDeResultados(
        symbol=report.get('symbol', None),  # Símbolo de la empresa (ticker)
        anio_fiscal=report.get('calendarYear', None),  # Año fiscal

        # fecha=report.get('date', None),  # Fecha del informe
        moneda_reportada=report.get('reportedCurrency', None),  # Moneda utilizada en el reporte
        cik=report.get('cik', None),  # Código CIK

        # Ingresos y gastos
        ingresos=report.get('revenue', None),  # Ingresos totales
        costo_ingresos=report.get('costOfRevenue', None),  # Costo de ingresos
        ganancia_bruta=report.get('grossProfit', None),  # Ganancia bruta
        margen_ganancia_bruta=report.get('grossProfitRatio', None),  # Margen de ganancia bruta
        gastos_investigacion_desarrollo=report.get('researchAndDevelopmentExpenses', None),  # Gastos en investigación y desarrollo
        gastos_generales_administrativos=report.get('generalAndAdministrativeExpenses', None),  # Gastos generales y administrativos
        gastos_ventas_marketing=report.get('sellingAndMarketingExpenses', None),  # Gastos en ventas y marketing
        gastos_operativos=report.get('operatingExpenses', None),  # Gastos operativos
        otros_gastos=report.get('otherExpenses', None),  # Otros gastos
        coste_y_gastos_totales=report.get('costAndExpenses', None),  # Costos y gastos totales

        # Ingresos y gastos financieros
        ingresos_por_intereses=report.get('interestIncome', None),  # Ingresos por intereses
        gastos_por_intereses=report.get('interestExpense', None),  # Gastos por intereses

        # Depreciación y EBITDA
        depreciaciones_amortizaciones=report.get('depreciationAndAmortization', None),  # Depreciación y amortización
        ebitda=report.get('ebitda', None),  # EBITDA
        margen_ebitda=report.get('ebitdaratio', None),  # Margen EBITDA

        # Ingresos operativos
        ingreso_operativo=report.get('operatingIncome', None),  # Ingresos operativos
        margen_ingreso_operativo=report.get('operatingIncomeRatio', None),  # Margen de ingresos operativos

        # Otros ingresos/gastos
        otros_ingresos_gastos_netos=report.get('totalOtherIncomeExpensesNet', None),  # Otros ingresos/gastos netos

        # Ingresos antes de impuestos
        ingreso_antes_impuestos=report.get('incomeBeforeTax', None),  # Ingresos antes de impuestos
        margen_ingreso_antes_impuestos=report.get('incomeBeforeTaxRatio', None),  # Margen de ingresos antes de impuestos

        # Impuestos
        impuestos=report.get('incomeTaxExpense', None),  # Gastos por impuestos

        # Ingresos netos
        ingreso_neto=report.get('netIncome', None),  # Ingreso neto
        margen_ingreso_neto=report.get('netIncomeRatio', None),  # Margen de ingreso neto

        # Ganancias por acción (EPS)
        eps=report.get('eps', None),  # EPS (ganancias por acción)
        eps_diluido=report.get('epsdiluted', None),  # EPS diluido

        # Acciones promedio
        acciones_promedio=report.get('weightedAverageShsOut', None),  # Acciones promedio en circulación
        acciones_promedio_diluidas=report.get('weightedAverageShsOutDil', None),  # Acciones promedio diluidas

        # Enlaces
        enlace=report.get('link', None),  # Enlace al reporte general
        enlace_final=report.get('finalLink', None)  # Enlace directo al reporte
    )

    # Añadir a la sesión
    session.add(nueva_cuenta_resultados)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error al guardar cuenta de resultados: {e}")

# Definir la tabla Empresa
class Empresa(Base):
    __tablename__ = 'empresa'

    symbol = Column(String, primary_key=True)

    nombre = Column(String)
    precio = Column(Float)
    exchange = Column(String)
    exchange_short_name = Column(String)
    sector = Column(String)

    # Relación con la tabla Año Fiscal (uno a muchos)
    anios_fiscales = relationship("AnioFiscal", back_populates="empresa")
@capturar_errores_bbdd
def guardar_empresa(session: Session, empresa: Dict[str, Any]) -> Empresa:
    nueva_empresa = Empresa(
        symbol=empresa.get('symbol'),
        nombre=empresa.get('companyName'),
        precio = empresa.get('price'),
        exchange = empresa.get('exchange'),
        exchange_short_name = empresa.get('exchangeShortName'),
        sector = empresa.get('sector'))

    session.add(nueva_empresa)
    try:
        session.commit()  # Asegurarse de hacer commit para obtener el ID
    except Exception as e:
        session.rollback()
        logging.error(f"Error al guardar empresa: {e}")

    logging.debug(f'Empresa {nueva_empresa.nombre} guardada.')
    return nueva_empresa




# Definir la tabla Año Fiscal
class AnioFiscal(Base):
    __tablename__ = 'anio_fiscal'

    anio_fiscal = Column(Integer, nullable=False)
    symbol = Column(String, ForeignKey('empresa.symbol'), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'anio_fiscal'),  # Clave primaria compuesta
    )


    # Relación con la tabla Empresa
    empresa = relationship("Empresa", back_populates="anios_fiscales")

    # Relaciones con las otras tablas (añadido foreign_keys)
    # Relaciones con las otras tablas (añadido primaryjoin)
    cash_flow = relationship("CashFlow", back_populates="anio_fiscal_relacion", 
                             primaryjoin="and_(AnioFiscal.symbol == CashFlow.symbol, AnioFiscal.anio_fiscal == CashFlow.anio_fiscal)")

    cuenta_de_resultados = relationship("CuentaDeResultados", back_populates="anio_fiscal_relacion", 
                             primaryjoin="and_(AnioFiscal.symbol == CuentaDeResultados.symbol, AnioFiscal.anio_fiscal == CuentaDeResultados.anio_fiscal)")

    balance_general = relationship("BalanceGeneral", back_populates="anio_fiscal_relacion", 
                             primaryjoin="and_(AnioFiscal.symbol == BalanceGeneral.symbol, AnioFiscal.anio_fiscal == BalanceGeneral.anio_fiscal)")


    # Propiedades
    price_frist = Column(Float)  # Marketcap
    price_last = Column(Float)  # Marketcap
    price_min = Column(Float)  # Marketcap
    price_max = Column(Float)  # Marketcap
    price_avg = Column(Float)  # Marketcap
    price_std = Column(Float)  # Marketcap
    price_var = Column(Float)  # Marketcap
    price_change = Column(Float)  # Marketcap
    price_change_pct = Column(Float)  # Marketcap
    price_change_pct_1y = Column(Float)  # Marketcap
    price_change_pct_1m = Column(Float)  # Marketcap
    price_change_pct_3m = Column(Float)  # Marketcap
    price_change_pct_6m = Column(Float)  # Marketcap

    @property
    def market_cap(self):
        return self.precio_por_accion * self.cuenta_de_resultados.numero_de_acciones

    # Ratios financieros calculados en tiempo de ejecución
    @property
    def per(self):
        return dividir(self.market_cap, self.cuenta_de_resultados.beneficio_neto)

    @property
    def pfcf(self):
        return dividir(self.market_cap, self.cash_flow.flujo_libre_caja)

    @property
    def pb(self):
        return dividir(self.market_cap, self.balance_general.patrimonio_neto_total)

    @property
    def ps(self):
        return dividir(self.market_cap, self.cuenta_de_resultados.ingresos)

    @property
    def ev_ebit(self):
        deuda_neta = (self.balance_general.pasivo_total - self.balance_general.efectivo_inversiones_cp)
        enterprise_value = self.market_cap + deuda_neta
        return dividir(enterprise_value, self.cuenta_de_resultados.resultado_operativo)


@capturar_errores_bbdd
def guardar_anio_fiscal(session: Session, symbol: str, anio: int, precios) -> Optional[AnioFiscal]:

    # Comprobar si el año fiscal ya existe
    anio_fiscal_existente = session.query(AnioFiscal).filter_by(symbol=symbol, anio_fiscal=anio).first()
    if anio_fiscal_existente:
        logging.debug(f'Año fiscal {anio} para la empresa {symbol} ya existe.')
        return anio_fiscal_existente

    # Crear una nueva instancia de AnioFiscal si no existe
    nuevo_anio_fiscal = AnioFiscal(
        symbol=symbol,
        anio_fiscal=anio,

        # Precios de la acción
        price_frist=precios.get('open', None),
        price_last=precios.get('close', None),
        price_min=precios.get('low', None),
        price_max=precios.get('high', None),
        price_avg=precios.get('close_mean', None),
        price_std=precios.get('close_std', None),
        price_var=precios.get('close_var', None),
        price_change=precios.get('price_change', None),
        price_change_pct=precios.get('price_change_pct', None),
        price_change_pct_1y=precios.get('price_change_pct_1y', None),
        price_change_pct_1m=precios.get('price_change_pct_1m', None),
        price_change_pct_3m=precios.get('price_change_pct_3m', None),
        price_change_pct_6m=precios.get('price_change_pct_6m', None)
    )

    session.add(nuevo_anio_fiscal)

    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error al guardar año fiscal: {e}")

    logging.debug(f'Año fiscal {anio} para la empresa {symbol} guardado.')
    return nuevo_anio_fiscal

def crear_tablas(eliminar_bbdd: bool = False) -> None:
    """
    Crea las tablas definidas en la base de datos. Si eliminar_bbdd es True, elimina todas las tablas antes de crearlas.
    """
    try:
        if eliminar_bbdd:
            # Eliminar todas las tablas en la base de datos
            Base.metadata.drop_all(engine)
            logging.info("Tablas eliminadas exitosamente en la base de datos.")

        # Crear las tablas en la base de datos
        Base.metadata.create_all(engine)
        logging.info("Tablas creadas exitosamente en la base de datos.")
    except Exception as e:
        logging.error(f"Error al crear las tablas: {e}")
        logging.error(traceback.format_exc())


def extraer_todos_datos(session: Session) -> pd.DataFrame:
    query = session.query(
        Empresa.symbol,
        AnioFiscal.anio_fiscal,
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
        CuentaDeResultados.ingresos,
        CuentaDeResultados.costo_ingresos,
        CuentaDeResultados.coste_de_las_ventas,
        CuentaDeResultados.ganancia_bruta,
        CuentaDeResultados.margen_ganancia_bruta,
        CuentaDeResultados.gastos_investigacion_desarrollo,
        CuentaDeResultados.gastos_generales_administrativos,
        CuentaDeResultados.gastos_ventas_marketing,
        CuentaDeResultados.gastos_operativos,
        CuentaDeResultados.otros_gastos,
        CuentaDeResultados.coste_y_gastos_totales,
        CuentaDeResultados.ingresos_por_intereses,
        CuentaDeResultados.gastos_por_intereses,
        CuentaDeResultados.depreciaciones_amortizaciones,
        CuentaDeResultados.ebitda,
        CuentaDeResultados.margen_ebitda,
        CuentaDeResultados.ingreso_operativo,
        CuentaDeResultados.margen_ingreso_operativo,
        CuentaDeResultados.otros_ingresos_gastos_netos,
        CuentaDeResultados.ingreso_antes_impuestos,
        CuentaDeResultados.margen_ingreso_antes_impuestos,
        CuentaDeResultados.impuestos,
        CuentaDeResultados.ingreso_neto,
        CuentaDeResultados.margen_ingreso_neto,
        CuentaDeResultados.eps,
        CuentaDeResultados.eps_diluido,
        CuentaDeResultados.acciones_promedio,
        CuentaDeResultados.acciones_promedio_diluidas,
        BalanceGeneral.efectivo_y_equivalentes,
        BalanceGeneral.inversiones_corto_plazo,
        BalanceGeneral.efectivo_y_inversiones_corto_plazo,
        BalanceGeneral.cuentas_por_cobrar,
        BalanceGeneral.inventario,
        BalanceGeneral.otros_activos_corrientes,
        BalanceGeneral.total_activos_corrientes,
        BalanceGeneral.propiedad_planta_y_equipo,
        BalanceGeneral.plusvalia,
        BalanceGeneral.activos_intangibles,
        BalanceGeneral.plusvalia_y_intangibles,
        BalanceGeneral.inversiones_largo_plazo,
        BalanceGeneral.activos_por_impuestos,
        BalanceGeneral.otros_activos_no_corrientes,
        BalanceGeneral.total_activos_no_corrientes,
        BalanceGeneral.otros_activos,
        BalanceGeneral.total_activos,
        BalanceGeneral.cuentas_por_pagar,
        BalanceGeneral.deuda_corto_plazo,
        BalanceGeneral.impuestos_por_pagar,
        BalanceGeneral.ingresos_diferidos,
        BalanceGeneral.otros_pasivos_corrientes,
        BalanceGeneral.total_pasivos_corrientes,
        BalanceGeneral.deuda_largo_plazo,
        BalanceGeneral.ingresos_diferidos_no_corrientes,
        BalanceGeneral.impuestos_diferidos_no_corrientes,
        BalanceGeneral.otros_pasivos_no_corrientes,
        BalanceGeneral.total_pasivos_no_corrientes,
        BalanceGeneral.otros_pasivos,
        BalanceGeneral.obligaciones_arrendamiento_capital,
        BalanceGeneral.total_pasivos,
        BalanceGeneral.acciones_preferentes,
        BalanceGeneral.acciones_comunes,
        BalanceGeneral.ganancias_retenidas,
        BalanceGeneral.ingresos_comprensivos_acumulados,
        BalanceGeneral.otro_total_patrimonio_accionistas,
        BalanceGeneral.total_patrimonio_accionistas,
        BalanceGeneral.total_patrimonio,
        BalanceGeneral.intereses_minoritarios,
        BalanceGeneral.total_pasivos_y_patrimonio_accionistas,
        BalanceGeneral.total_pasivos_y_patrimonio,
        BalanceGeneral.total_inversiones,
        BalanceGeneral.total_deuda,
        BalanceGeneral.deuda_neta
    ).join(AnioFiscal, Empresa.symbol == AnioFiscal.symbol) \
     .join(CashFlow, and_(AnioFiscal.symbol == CashFlow.symbol, AnioFiscal.anio_fiscal == CashFlow.anio_fiscal)) \
     .join(CuentaDeResultados, and_(AnioFiscal.symbol == CuentaDeResultados.symbol, AnioFiscal.anio_fiscal == CuentaDeResultados.anio_fiscal)) \
     .join(BalanceGeneral, and_(AnioFiscal.symbol == BalanceGeneral.symbol, AnioFiscal.anio_fiscal == BalanceGeneral.anio_fiscal))
    # Convertir a DataFrame
    df = pd.read_sql(query.statement, session.bind)

    # Eliminar columnas repetidas
    df = df.loc[:, ~df.columns.duplicated()]

    return df
