"""Tests de validación de uploads (PR3, item 6) y de persistencia remota
vía GitHub (PR3, item nuevo: la plantilla subida por UI debe sobrevivir a
un reinicio del contenedor de Streamlit Cloud).

Los loaders de ventas/stock leen `.xls`/`.xlsx` reales vía pandas — acá se
monkeypatchea `pd.read_excel` para probar la lógica de validación sin
depender de archivos binarios de ejemplo (que además están gitignored).
`actualizar_archivo_en_github` se prueba monkeypatcheando `requests`, sin
tocar la red.
"""

import pandas as pd
import pytest
import requests

from pedido_logic import (
    ArchivoInvalidoError,
    actualizar_archivo_en_github,
    cargar_cajas_terminadas,
    cargar_mixventas,
    cargar_stock,
)


# ── Loaders: columnas/forma inesperada → ArchivoInvalidoError ────────────────

def test_cargar_cajas_terminadas_columnas_faltantes(monkeypatch):
    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: pd.DataFrame({"otra_columna": [1, 2]}))
    with pytest.raises(ArchivoInvalidoError):
        cargar_cajas_terminadas(object())


def test_cargar_cajas_terminadas_agrupa_ok(monkeypatch):
    monkeypatch.setattr(
        pd, "read_excel",
        lambda *a, **k: pd.DataFrame({
            "artdescrip": ["LIMON", "LIMON", "FRUTILLA"],
            "ctecantidad": [1, 2, 3],
        }),
    )
    resultado = cargar_cajas_terminadas(object())
    assert set(resultado["nombre_venta"]) == {"LIMON", "FRUTILLA"}
    assert resultado.loc[resultado["nombre_venta"] == "LIMON", "venta"].iloc[0] == 3


def test_cargar_mixventas_columnas_faltantes(monkeypatch):
    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: pd.DataFrame({"artdescrip": ["x"]}))
    with pytest.raises(ArchivoInvalidoError):
        cargar_mixventas(object())


def test_cargar_stock_pocas_columnas(monkeypatch):
    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: pd.DataFrame({0: [1], 1: [2]}))
    with pytest.raises(ArchivoInvalidoError):
        cargar_stock(object())


def test_cargar_stock_sin_filas(monkeypatch):
    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: pd.DataFrame(columns=list(range(7))))
    with pytest.raises(ArchivoInvalidoError):
        cargar_stock(object())


def test_cargar_stock_ok(monkeypatch):
    monkeypatch.setattr(
        pd, "read_excel",
        lambda *a, **k: pd.DataFrame({
            0: ["H13- SABORES AL AGUA"], 1: ["4000036"], 2: ["x"], 3: ["x"],
            4: ["LIMON AL AGUA"], 5: [10], 6: [5],
        }),
    )
    resultado = cargar_stock(object())
    assert resultado.loc[0, "codigo"] == "4000036"
    assert resultado.loc[0, "stock_real"] == 5


# ── Persistencia remota vía GitHub ────────────────────────────────────────────

class _FakeResponse:
    def __init__(self, status_code, json_data=None, text=""):
        self.status_code = status_code
        self._json = json_data or {}
        self.text = text

    def json(self):
        return self._json


def test_actualizar_archivo_en_github_ok_incluye_sha_si_el_archivo_ya_existe(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(200, {"sha": "abc123"}))

    calls = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        calls["json"] = json
        return _FakeResponse(201)

    monkeypatch.setattr(requests, "put", fake_put)

    ok, err = actualizar_archivo_en_github(
        b"contenido", token="tok", repo="a/b", path_repo="x.xlsx",
    )

    assert ok is True
    assert err == ""
    assert calls["json"]["sha"] == "abc123"


def test_actualizar_archivo_en_github_sin_sha_si_el_archivo_no_existe(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(404))

    calls = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        calls["json"] = json
        return _FakeResponse(201)

    monkeypatch.setattr(requests, "put", fake_put)

    ok, _ = actualizar_archivo_en_github(b"contenido", token="tok", repo="a/b", path_repo="x.xlsx")

    assert ok is True
    assert "sha" not in calls["json"]


def test_actualizar_archivo_en_github_credenciales_invalidas(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: _FakeResponse(404))
    monkeypatch.setattr(
        requests, "put",
        lambda *a, **k: _FakeResponse(403, {"message": "Bad credentials"}),
    )

    ok, err = actualizar_archivo_en_github(
        b"contenido", token="tok-invalido", repo="a/b", path_repo="x.xlsx",
    )

    assert ok is False
    assert "403" in err


def test_actualizar_archivo_en_github_error_de_red(monkeypatch):
    def _raise(*a, **k):
        raise requests.RequestException("timeout")

    monkeypatch.setattr(requests, "get", _raise)

    ok, err = actualizar_archivo_en_github(b"contenido", token="tok", repo="a/b", path_repo="x.xlsx")

    assert ok is False
    assert err  # mensaje no vacío, no nos importa la redacción exacta
