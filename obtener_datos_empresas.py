import requests
import pandas as pd
from sqlalchemy.orm import sessionmaker, Session
from typing import Any, Dict, Optional, List, Union
import bbdd
from conf import *  # Ensure that API_KEY is defined in conf.py

# Constantes para las URLs de la API
API = 'https://financialmodelingprep.com/api/v3/'
URL_LISTA_EMPRESAS = API + 'stock/list?apikey={api_key}'
URL_PRECIOS_HISTORICOS = API + 'historical-price-full/{symbol}?apikey={api_key}'
URL_CASH_FLOW = API + 'cash-flow-statement/{symbol}?period=annual&apikey={api_key}'
URL_BALANCE_GENERAL = API + 'balance-sheet-statement/{symbol}?apikey={api_key}'
URL_CUENTA_RESULTADOS = API + 'income-statement/{symbol}?apikey={api_key}'
URL_PERFIL_EMPRESA = API + 'profile/{symbol}?apikey={api_key}'

def realizar_peticion(url: str) -> Optional[Dict[str, Any]]:
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Error al realizar la solicitud a la API: {response.status_code}")
        logging.debug(response.text)

        if response.status_code == 429:
            logging.error("Se ha alcanzado el límite de solicitudes a la API.")
            exit(1)
        return None
    data = response.json()
    if not data:
        logging.debug(response)
        logging.debug(url)
    return data

def error_no_data(data: Any, tipo: str) -> None:
    logging.warning(f'No se encontraron datos de {tipo}')
    return None

def obtener_lista_empresas(api_key: str, solo_eeuu: bool = False) -> List[Dict[str, Any]]:
    url = URL_LISTA_EMPRESAS.format(api_key=api_key)
    empresas = realizar_peticion(url)
    if not empresas:
        return []
    if solo_eeuu:
        empresas = [empresa for empresa in empresas if empresa.get('exchangeShortName') in ['NYSE', 'NASDAQ']]
    return empresas

def obtener_precios_historicos(session: Session, api_key: str, symbol: str) -> Optional[pd.DataFrame]:
    url = URL_PRECIOS_HISTORICOS.format(symbol=symbol, api_key=api_key)
    data = realizar_peticion(url)

    if data and 'historical' in data:
        # Convertir los datos históricos en un DataFrame
        precios_df = pd.DataFrame(data['historical'])
        precios_df['date'] = pd.to_datetime(precios_df['date'])
        precios_df.set_index('date', inplace=True)

        # Asegurarse de que las columnas necesarias existan
        required_columns = {'open', 'close', 'low', 'high'}
        if not required_columns.issubset(precios_df.columns):
            raise ValueError(f"Faltan columnas necesarias en los datos: {required_columns - set(precios_df.columns)}")

        # Resumir datos anuales
        precios_anuales = precios_df.resample('Y').agg({
            'open': 'first',
            'low': 'min',
            'high': 'max',
            'close': 'last'
        })

        # Calcular métricas adicionales
        precios_anuales['close_mean'] = precios_df['close'].resample('Y').mean()
        precios_anuales['close_std'] = precios_df['close'].resample('Y').std()
        precios_anuales['close_var'] = precios_df['close'].resample('Y').var()
        precios_anuales['price_change'] = precios_anuales['close'] - precios_anuales['open']
        precios_anuales['price_change_pct'] = precios_anuales['price_change'] / precios_anuales['open']

        # Calcular cambios porcentuales mensuales, trimestrales y semestrales
        mean_close_monthly = precios_df['close'].resample('M').mean()
        mean_close_quarterly = precios_df['close'].resample('Q').mean()
        mean_close_semiannually = precios_df['close'].resample('6M').mean()

        precios_anuales['price_change_pct_1y'] = precios_anuales['close'].pct_change(periods=1)
        precios_anuales['price_change_pct_1m'] = mean_close_monthly.pct_change(periods=1).resample('Y').last()
        precios_anuales['price_change_pct_3m'] = mean_close_quarterly.pct_change(periods=1).resample('Y').last()
        precios_anuales['price_change_pct_6m'] = mean_close_semiannually.pct_change(periods=1).resample('Y').last()

        # Guardar en la base de datos
        for anio, precios in precios_anuales.iterrows():
            bbdd.guardar_anio_fiscal(session=session, symbol=symbol, anio=anio.year, precios=precios.to_dict())

        return precios_anuales

    logging.debug(f"No se encontraron datos históricos para {symbol}")
    return None

def obtener_precio_por_fecha(precios_df: pd.DataFrame, fecha: str) -> Optional[float]:
    fecha = pd.to_datetime(fecha)
    if precios_df.empty:
        logging.debug("El DataFrame está vacío.")
        return None
    precios_df = precios_df.sort_index()
    pos = precios_df.index.searchsorted(fecha)
    if pos == 0:
        fecha_mas_cercana = precios_df.index[0]
    elif pos >= len(precios_df.index):
        fecha_mas_cercana = precios_df.index[-1]
    else:
        fecha_anterior = precios_df.index[pos - 1]
        fecha_siguiente = precios_df.index[pos]
        fecha_mas_cercana = fecha_anterior if (fecha - fecha_anterior) <= (fecha_siguiente - fecha) else fecha_siguiente
    precio_cierre = precios_df.loc[fecha_mas_cercana, 'close']
    logging.info(f"El precio de cierre más cercano a {fecha.date()} es el de {fecha_mas_cercana.date()}: {precio_cierre}")
    return precio_cierre

def obtener_cash_flow_fmp(session: Session, api_key: str, symbol: str, guardar_base_de_datos: bool = True) -> Optional[List[Dict[str, Any]]]:
    url = URL_CASH_FLOW.format(symbol=symbol, api_key=api_key)
    data = realizar_peticion(url)
    if not data:
        return error_no_data(data, 'flujo de caja')
    if guardar_base_de_datos:
        for report in data:
            with session:
                bbdd.guardar_cash_flow(session=session, report=report)
    return data

def obtener_balance_general_fmp(session: Session, api_key: str, symbol: str, guardar_base_de_datos: bool = True) -> Optional[List[Dict[str, Any]]]:
    url = URL_BALANCE_GENERAL.format(symbol=symbol, api_key=api_key)
    data = realizar_peticion(url)
    if not data:
        return error_no_data(data, 'balance general')
    if guardar_base_de_datos:
        for report in data:
            with session:
                bbdd.guardar_balance_general(session, report)
    return data

def obtener_cuenta_resultados_fmp(session: Session, api_key: str, symbol: str, guardar_base_de_datos: bool = True) -> Optional[List[Dict[str, Any]]]:
    url = URL_CUENTA_RESULTADOS.format(symbol=symbol, api_key=api_key)
    data = realizar_peticion(url)
    if not data:
        return error_no_data(data, 'cuenta de resultados')
    if guardar_base_de_datos:
        for report in data:
            with session:
                bbdd.guardar_cuenta_resultados(session=session, report=report)
    return data

def obtener_informacion_empresa(session: Session, api_key: str, symbol: str, guardar_base_de_datos: bool = True) -> Union[None, Dict[str, Any]]:
    url = URL_PERFIL_EMPRESA.format(symbol=symbol, api_key=api_key)
    data = realizar_peticion(url)
    if not data:
        return None
    empresa = data[0]
    if guardar_base_de_datos:
        bbdd.guardar_empresa(session, empresa)
    return empresa

def procesar_empresa(session: Session, symbol: str) -> bool:
    try:
        logging.info(f"Procesando empresa con símbolo: {symbol}")
        empresa = obtener_informacion_empresa(session, api_key=API_KEY, symbol=symbol)
        if not empresa:
            logging.warning(f"No se pudo obtener información para la empresa: {symbol}")
            return False
        obtener_precios_historicos(session,API_KEY, symbol)
        obtener_cash_flow_fmp(session, API_KEY, symbol)
        obtener_balance_general_fmp(session, API_KEY, symbol)
        obtener_cuenta_resultados_fmp(session, API_KEY, symbol)
        logging.info(f"Empresa procesada correctamente: {symbol}")
        return True
    except Exception as e:
        logging.error(f"Error al procesar la empresa {symbol}: {e}")
        logging.error(traceback.format_exc())
        session.rollback()
        return False

def main() -> None:
    bbdd.crear_tablas(ELIMINAR_BBDD)

    Session = sessionmaker(bind=bbdd.engine)

    try:
        if OBTENER_EMPRESAS_CON_API:
            empresas = obtener_lista_empresas(API_KEY, True)
            if not empresas:
                logging.error("No se obtuvieron empresas desde la API.")
                return
            logging.info(f"Empresas obtenidas de la API: {len(empresas)}")
            df_empresas = pd.DataFrame(empresas)
            df_empresas.to_csv(fichero_lista_empresas, index=False)
        else:
            if not os.path.exists(fichero_lista_empresas):
                logging.error(f"No se encontró el archivo: {fichero_lista_empresas}")
                return
            df_empresas = pd.read_csv(fichero_lista_empresas)
        with Session() as session:
            for _, empresa in df_empresas.iterrows():
                symbol = empresa.get('symbol')
                if not symbol:
                    logging.warning(f"Símbolo no encontrado para empresa: {empresa}")
                    continue
                procesar_empresa(session, symbol)
                session.commit()
    except KeyboardInterrupt:
        logging.info("Interrupción del usuario. Saliendo...")
    except Exception as e:
        logging.error(f"Error crítico durante el procesamiento: {e}")
        logging.error(traceback.format_exc())
    finally:
        logging.info("Ejecución finalizada.")

if __name__ == "__main__":
    main()
