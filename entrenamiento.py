import bbdd

# Ahora vamos a obtener los datos de todas las empresas y a clasificarlas segun su rendimiento en los últimos n años
from sqlalchemy.orm import sessionmaker, Session
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import pandas as pd

# Crear la sesión
Session = sessionmaker(bind=bbdd.engine)
with Session() as session:
    # Extraer los datos de la base
    df = bbdd.extraer_todos_datos(session=session)

    # Eliminar filas con valores nulos
    df = df.dropna()

    # Calcular el incremento porcentual del precio de las acciones
    df['puntuacion'] = (df['precio_final'] - df['precio_inicial']) / df['precio_inicial'] * 100

    # Escalar los datos
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df.drop(columns=["symbol", "anio_fiscal", "precio_inicial", "precio_final"]))
    df_scaled = pd.DataFrame(scaled_data, columns=df.columns.drop(["symbol", "anio_fiscal", "precio_inicial", "precio_final"]))

    # Añadir el año fiscal a los datos escalados
    df_scaled['anio_fiscal'] = df['anio_fiscal'].values

    X = df_scaled
    y = df['puntuacion']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entrenar el modelo
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Predecir y evaluar
    y_pred = model.predict(X_test)
    error = mean_squared_error(y_test, y_pred)
    print(f"Error cuadrático medio: {error}")