"""Fetch company financial data from the FMP API and store it."""

import requests
import pandas as pd
from sqlalchemy.orm import sessionmaker, Session
from typing import Any, Dict, Optional, List, Union
import bbdd
from conf import *  # Ensure that API_KEY is defined in conf.py

# API URL constants
API = 'https://financialmodelingprep.com/api/v3/'
URL_LISTA_EMPRESAS = API + 'stock/list?apikey={api_key}'
URL_PRECIOS_HISTORICOS = API + 'historical-price-full/{symbol}?apikey={api_key}'
URL_CASH_FLOW = API + 'cash-flow-statement/{symbol}?period=annual&apikey={api_key}'
URL_BALANCE_GENERAL = API + 'balance-sheet-statement/{symbol}?apikey={api_key}'
URL_CUENTA_RESULTADOS = API + 'income-statement/{symbol}?apikey={api_key}'
URL_PERFIL_EMPRESA = API + 'profile/{symbol}?apikey={api_key}'


def make_request(url: str) -> Optional[Dict[str, Any]]:
    """Perform a GET request and return the JSON body."""
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"API request failed: {response.status_code}")
        logging.debug(response.text)

        if response.status_code == 429:
            logging.error("API rate limit reached.")
            exit(1)
        return None
    data = response.json()
    if not data:
        logging.debug(response)
        logging.debug(url)
    return data


def log_no_data(data: Any, kind: str) -> None:
    """Log a warning when no data was returned for a specific report."""
    logging.warning(f'No data found for {kind}')
    return None


def get_company_list(api_key: str, only_us: bool = False) -> List[Dict[str, Any]]:
    """Retrieve the list of companies from the API."""
    url = URL_LISTA_EMPRESAS.format(api_key=api_key)
    companies = make_request(url)
    if not companies:
        return []
    if only_us:
        companies = [c for c in companies if c.get('exchangeShortName') in ['NYSE', 'NASDAQ']]
    return companies


def get_historical_prices(session: Session, api_key: str, symbol: str) -> Optional[pd.DataFrame]:
    """Fetch and store yearly price statistics for a symbol."""
    url = URL_PRECIOS_HISTORICOS.format(symbol=symbol, api_key=api_key)
    data = make_request(url)

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

        # Store in the database
        for year, prices in precios_anuales.iterrows():
            bbdd.save_fiscal_year(session=session, symbol=symbol, year=year.year, prices=prices.to_dict())

        return precios_anuales

    logging.debug(f"No historical data found for {symbol}")
    return None


def get_price_by_date(prices_df: pd.DataFrame, date: str) -> Optional[float]:
    """Return the closing price closest to ``date``."""
    date = pd.to_datetime(date)
    if prices_df.empty:
        logging.debug("DataFrame is empty.")
        return None
    prices_df = prices_df.sort_index()
    pos = prices_df.index.searchsorted(date)
    if pos == 0:
        closest = prices_df.index[0]
    elif pos >= len(prices_df.index):
        closest = prices_df.index[-1]
    else:
        prev_date = prices_df.index[pos - 1]
        next_date = prices_df.index[pos]
        closest = prev_date if (date - prev_date) <= (next_date - date) else next_date
    close_price = prices_df.loc[closest, 'close']
    logging.info(f"Closing price near {date.date()} is {close_price} on {closest.date()}")
    return close_price


def get_cash_flow_fmp(session: Session, api_key: str, symbol: str, save_db: bool = True) -> Optional[List[Dict[str, Any]]]:
    """Retrieve cash flow statements from FMP."""
    url = URL_CASH_FLOW.format(symbol=symbol, api_key=api_key)
    data = make_request(url)
    if not data:
        return log_no_data(data, 'cash flow')
    if save_db:
        for report in data:
            with session:
                bbdd.save_cash_flow(session=session, report=report)
    return data


def get_balance_sheet_fmp(session: Session, api_key: str, symbol: str, save_db: bool = True) -> Optional[List[Dict[str, Any]]]:
    """Retrieve balance sheets from FMP."""
    url = URL_BALANCE_GENERAL.format(symbol=symbol, api_key=api_key)
    data = make_request(url)
    if not data:
        return log_no_data(data, 'balance sheet')
    if save_db:
        for report in data:
            with session:
                bbdd.save_balance_sheet(session, report)
    return data


def get_income_statement_fmp(session: Session, api_key: str, symbol: str, save_db: bool = True) -> Optional[List[Dict[str, Any]]]:
    """Retrieve income statements from FMP."""
    url = URL_CUENTA_RESULTADOS.format(symbol=symbol, api_key=api_key)
    data = make_request(url)
    if not data:
        return log_no_data(data, 'income statement')
    if save_db:
        for report in data:
            with session:
                bbdd.save_income_statement(session=session, report=report)
    return data


def get_company_info(session: Session, api_key: str, symbol: str, save_db: bool = True) -> Union[None, Dict[str, Any]]:
    """Retrieve basic company information."""
    url = URL_PERFIL_EMPRESA.format(symbol=symbol, api_key=api_key)
    data = make_request(url)
    if not data:
        return None
    company = data[0]
    if save_db:
        bbdd.save_company(session, company)
    return company


def process_company(session: Session, symbol: str) -> bool:
    """Download and store all available reports for ``symbol``."""
    try:
        logging.info(f"Processing company: {symbol}")
        company = get_company_info(session, api_key=API_KEY, symbol=symbol)
        if not company:
            logging.warning(f"No data for company: {symbol}")
            return False
        get_historical_prices(session, API_KEY, symbol)
        get_cash_flow_fmp(session, API_KEY, symbol)
        get_balance_sheet_fmp(session, API_KEY, symbol)
        get_income_statement_fmp(session, API_KEY, symbol)
        logging.info(f"Company processed: {symbol}")
        return True
    except Exception as e:
        logging.error(f"Failed to process {symbol}: {e}")
        logging.error(traceback.format_exc())
        session.rollback()
        return False


def main() -> None:
    """Entry point for fetching and storing company data."""
    bbdd.create_tables(ELIMINAR_BBDD)

    Session = sessionmaker(bind=bbdd.engine)

    try:
        if OBTENER_EMPRESAS_CON_API:
            companies = get_company_list(API_KEY, True)
            if not companies:
                logging.error("No companies returned from the API.")
                return
            logging.info(f"Companies obtained from API: {len(companies)}")
            df_empresas = pd.DataFrame(companies)
            df_empresas.to_csv(fichero_lista_empresas, index=False)
        else:
            if not os.path.exists(fichero_lista_empresas):
                logging.error(f"File not found: {fichero_lista_empresas}")
                return
            df_empresas = pd.read_csv(fichero_lista_empresas)
        with Session() as session:
            for _, empresa in df_empresas.iterrows():
                symbol = empresa.get('symbol')
                if not symbol:
                    logging.warning(f"Symbol not found for company: {empresa}")
                    continue
                process_company(session, symbol)
                session.commit()
    except KeyboardInterrupt:
        logging.info("Execution interrupted by user.")
    except Exception as e:
        logging.error(f"Critical error during processing: {e}")
        logging.error(traceback.format_exc())
    finally:
        logging.info("Execution finished.")

if __name__ == "__main__":
    main()
