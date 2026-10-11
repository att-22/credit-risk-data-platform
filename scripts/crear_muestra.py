"""Crea una muestra pequeña y SINTÉTICA de los CSV de Home Credit para la CI.

Los datos reales (2,5 GB) no están en GitHub, así que la CI no puede ejecutar
dbt con ellos. Este script:

1. Elige unos cientos de clientes y copia todas sus filas de cada tabla, de modo
   que las relaciones entre tablas se mantienen (cada cliente tiene su historial
   de buró, pagos, tarjetas...).
2. Baraja por separado los valores de cada columna entre las filas de la
   muestra. Los identificadores no se tocan (las relaciones siguen intactas) y
   cada columna conserva sus valores, su tipo y sus categorías, pero ninguna fila
   corresponde ya a un cliente real. Así no se redistribuyen datos de Kaggle.

La selección y el barajado son deterministas (md5 y semilla fija): ejecutarlo
dos veces produce exactamente los mismos ficheros.

Uso (se ejecuta a mano, una vez, y el resultado se sube a Git):
    uv run python scripts/crear_muestra.py
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from credit_risk.config import KEY_BUREAU, KEY_CURR, KEY_PREV, RANDOM_STATE, RAW_DIR, ROOT, TABLES

DESTINO = ROOT / "tests" / "fixtures" / "muestra" / "raw"

# Tamaño de la muestra. Cada CSV debe quedar por debajo de 1000 KB (límite del
# hook check-added-large-files de pre-commit).
N_TRAIN_IMPAGO = 20  # 20 de 250 = 8 %, la tasa de impago real (8,07 %)
N_TRAIN_PAGO = 230
N_TEST = 50

# Columnas que la capa intermediate convierte en proporciones (*_PROP). Se
# añade al menos un cliente con cada categoría para que la muestra genere las
# mismas columnas que los datos completos (test de nº de columnas del mart).
# Debe coincidir con el argumento `categoricas` de los modelos int_*__por_cliente.
CATEGORICAS = {
    "bureau": ["CREDIT_ACTIVE", "CREDIT_TYPE"],
    "previous_application": ["NAME_CONTRACT_STATUS", "NAME_CONTRACT_TYPE", "NAME_YIELD_GROUP"],
    "pos_cash_balance": ["NAME_CONTRACT_STATUS"],
    "credit_card_balance": ["NAME_CONTRACT_STATUS"],
}

# Columnas que NO se barajan: identificadores (relaciones entre tablas) y el
# mes, que forma la clave compuesta (SK_ID_BUREAU, MONTHS_BALANCE).
FIJAS = {KEY_CURR, KEY_BUREAU, KEY_PREV, "MONTHS_BALANCE"}

# Columnas que se barajan JUNTAS porque los tests comprueban su coherencia:
# cuota <= crédito (RATIO_CUOTA_CREDITO) y años trabajados <= edad (PORC_VIDA_EMPLEADO).
BLOQUES = [
    ["AMT_CREDIT", "AMT_ANNUITY", "AMT_GOODS_PRICE"],
    ["DAYS_BIRTH", "DAYS_EMPLOYED"],
]


def barajar(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Permuta los valores de cada columna (o bloque de columnas) entre las filas."""
    df = df.copy()
    en_bloque = {c for bloque in BLOQUES for c in bloque}
    grupos = [[c for c in bloque if c in df.columns] for bloque in BLOQUES]
    grupos += [[c] for c in df.columns if c not in FIJAS and c not in en_bloque]
    for grupo in filter(None, grupos):
        df[grupo] = df[grupo].to_numpy()[rng.permutation(len(df))]
    return df


def main() -> None:
    print(f"Origen:  {RAW_DIR}")
    print(f"Destino: {DESTINO}\n")
    DESTINO.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    rng = np.random.default_rng(RANDOM_STATE)

    # all_varchar: se lee todo como texto para escribirlo tal cual estaba en el
    # CSV original (mismos formatos de números, Y/N, vacíos...).
    for nombre, fichero in TABLES.items():
        ruta = (RAW_DIR / fichero).as_posix()
        con.execute(f"CREATE VIEW {nombre} AS SELECT * FROM read_csv('{ruta}', all_varchar=true)")

    # 1) Muestra base: estratificada por TARGET en train, más algunos de test.
    con.execute(f"""
        CREATE TABLE clientes AS
        (SELECT SK_ID_CURR FROM application_train WHERE TARGET = '1'
         ORDER BY md5(SK_ID_CURR) LIMIT {N_TRAIN_IMPAGO})
        UNION
        (SELECT SK_ID_CURR FROM application_train WHERE TARGET = '0'
         ORDER BY md5(SK_ID_CURR) LIMIT {N_TRAIN_PAGO})
        UNION
        (SELECT SK_ID_CURR FROM application_test
         ORDER BY md5(SK_ID_CURR) LIMIT {N_TEST})
    """)

    # 2) Cobertura de categorías: un cliente por cada valor de cada columna.
    for tabla, columnas in CATEGORICAS.items():
        for columna in columnas:
            con.execute(f"""
                INSERT INTO clientes
                SELECT SK_ID_CURR FROM {tabla}
                WHERE {columna} IS NOT NULL
                QUALIFY row_number() OVER (PARTITION BY {columna} ORDER BY md5(SK_ID_CURR)) = 1
            """)
    con.execute("CREATE TABLE muestra AS SELECT DISTINCT SK_ID_CURR FROM clientes")
    n = con.execute("SELECT count(*) FROM muestra").fetchone()[0]
    print(f"Clientes en la muestra: {n}\n")

    # 3) Filas de esos clientes, barajadas. bureau_balance no tiene SK_ID_CURR:
    #    se filtra por los créditos de buró de los clientes elegidos.
    for nombre, fichero in TABLES.items():
        if nombre == "bureau_balance":
            filtro = """WHERE SK_ID_BUREAU IN (
                SELECT SK_ID_BUREAU FROM bureau WHERE SK_ID_CURR IN (SELECT * FROM muestra))"""
        else:
            filtro = "WHERE SK_ID_CURR IN (SELECT * FROM muestra)"
        # ORDER BY ALL: DuckDB lee en paralelo y el orden de las filas varía entre
        # ejecuciones; sin un orden fijo, el barajado no sería reproducible.
        df = barajar(con.sql(f"SELECT * FROM {nombre} {filtro} ORDER BY ALL").df(), rng)

        salida = DESTINO / fichero
        df.to_csv(salida, index=False, lineterminator="\n")
        print(f"[OK] {fichero:<28} {len(df):>7,} filas  {salida.stat().st_size / 1024:>6.0f} KB")


if __name__ == "__main__":
    main()
