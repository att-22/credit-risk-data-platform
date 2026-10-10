"""Tests de la carga de tablas.

No usan los CSV reales: crean ficheros falsos en una carpeta temporal y hacen
que el código apunte allí. Así los tests funcionan en cualquier máquina,
incluido el servidor de CI, donde no están los 2,7 GB de datos.
"""

import pandas as pd
import pytest

from credit_risk import data_loader


def test_tabla_desconocida_lanza_error():
    with pytest.raises(KeyError, match="Tabla desconocida"):
        data_loader.load_table("tabla_que_no_existe")


def test_fichero_ausente_lanza_error(tmp_path, monkeypatch):
    # tmp_path: carpeta temporal vacía que pytest crea (y borra) para este test.
    # monkeypatch: cambia RAW_DIR solo durante este test.
    monkeypatch.setattr(data_loader, "RAW_DIR", tmp_path)

    with pytest.raises(FileNotFoundError, match="DATA_DIR"):
        data_loader.load_table("bureau")


def test_carga_una_tabla_existente(tmp_path, monkeypatch):
    pd.DataFrame({"SK_ID_CURR": [1, 2], "TARGET": [0, 1]}).to_csv(
        tmp_path / "application_train.csv", index=False
    )
    monkeypatch.setattr(data_loader, "RAW_DIR", tmp_path)

    df = data_loader.load_application_train()

    assert df.shape == (2, 2)
    assert list(df["TARGET"]) == [0, 1]
