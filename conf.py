import logging
import colorlog
import os
import traceback
import pandas as pd
from dotenv import load_dotenv

if not os.path.exists('data'): os.makedirs('data')
DATA_BASE = os.path.join('data','financial_data.db')


# Cargar las variables del archivo .env
load_dotenv()
API_KEY = os.getenv("API_KEY")
fichero_lista_empresas = os.path.join('data','lista_empresas.csv')

OBTENER_EMPRESAS_CON_API = False
ELIMINAR_BBDD = False

# Configuración de colorlog
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    log_colors={
        'DEBUG': 'blue',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
))

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

