"""Tests del histórico de pedidos (registrar_pedido_historico / cargar_historico_pedidos).

Usa `tmp_path` para no tocar data/historico_pedidos.csv real.
"""

from pedido_logic import HISTORICO_COLUMNAS, cargar_historico_pedidos, registrar_pedido_historico


def test_cargar_historico_inexistente_devuelve_df_vacio_con_columnas(tmp_path):
    historico = cargar_historico_pedidos(tmp_path / "no_existe.csv")

    assert historico.empty
    assert list(historico.columns) == HISTORICO_COLUMNAS


def test_registrar_agrega_fila_y_persiste_en_disco(tmp_path):
    path = tmp_path / "historico_pedidos.csv"
    fila = {
        "fecha": "2026-08-18 12:00",
        "total_bultos": 100,
        "cajas_granel": 20,
        "total_kilos": 150.5,
        "total_cubicaje": 3.21,
        "subtotal_sin_iva": 500000.0,
        "total_con_iva": 605000.0,
        "n_productos": 45,
        "modo_replicar_venta": False,
        "semana_plan": "Semana_1_Enero_2026",
    }

    actualizado, guardado = registrar_pedido_historico(fila, path=path)

    assert guardado is True
    assert path.exists()
    assert len(actualizado) == 1
    assert actualizado.loc[0, "total_con_iva"] == 605000.0

    releido = cargar_historico_pedidos(path)
    assert len(releido) == 1
    assert releido.loc[0, "fecha"] == "2026-08-18 12:00"


def test_registrar_acumula_varias_filas_en_orden(tmp_path):
    path = tmp_path / "historico_pedidos.csv"
    fila_base = {c: None for c in HISTORICO_COLUMNAS}

    registrar_pedido_historico({**fila_base, "fecha": "2026-08-01 10:00", "total_con_iva": 100}, path=path)
    actualizado, _ = registrar_pedido_historico(
        {**fila_base, "fecha": "2026-08-08 10:00", "total_con_iva": 200}, path=path
    )

    assert len(actualizado) == 2
    assert list(actualizado["fecha"]) == ["2026-08-01 10:00", "2026-08-08 10:00"]


def test_registrar_tolera_dict_incompleto(tmp_path):
    path = tmp_path / "historico_pedidos.csv"

    actualizado, guardado = registrar_pedido_historico({"fecha": "2026-08-18 12:00"}, path=path)

    assert guardado is True
    assert list(actualizado.columns) == HISTORICO_COLUMNAS
    assert actualizado.loc[0, "fecha"] == "2026-08-18 12:00"
    assert actualizado.loc[0, "total_bultos"] is None or str(actualizado.loc[0, "total_bultos"]) == "nan"
