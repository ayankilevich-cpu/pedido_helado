import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def build_mapeo():
    """Factory: filas con nombre_venta, tipo, codigo_carrito, descripcion_carrito, score."""
    def _build(rows):
        return pd.DataFrame(rows)
    return _build


@pytest.fixture
def build_ventas():
    """Factory: filas con nombre_venta, venta, tipo."""
    def _build(rows):
        return pd.DataFrame(rows)
    return _build


@pytest.fixture
def build_stock():
    """Factory: filas con codigo, descripcion, grupo, stock_seg, stock_real."""
    def _build(rows):
        return pd.DataFrame(rows)
    return _build


@pytest.fixture
def build_plan():
    """Factory: filas de planificación semanal (codigo_homologado + columnas Semana_N_Mes_Año)."""
    def _build(rows):
        return pd.DataFrame(rows)
    return _build
