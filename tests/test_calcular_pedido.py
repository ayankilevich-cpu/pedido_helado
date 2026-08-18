"""Tests de `calcular_pedido`, la función central del negocio.

Cubre: modo normal, modo replicar venta, banda de plan (up/down/zero/missing),
SKUs sin venta, y el bug de descripciones cruzadas (ver AGENTS.md §12, ago 2026).
"""

import math

import pandas as pd

from pedido_logic import (
    AJUSTE_BAJA,
    AJUSTE_NINGUNO,
    AJUSTE_PLAN_CERO,
    AJUSTE_SIN_PLAN,
    AJUSTE_SUBE,
    calcular_pedido,
)

GRANEL = "H13- SABORES AL AGUA"
EMPAQUETADO = "H17- PALITOS"


def _fila(codigo, nombre, tipo="granel", grupo=GRANEL, stock_seg=0, stock_real=0, score=100):
    """Atajo para armar una tripleta consistente (mapeo, stock) de un mismo producto."""
    mapeo = {
        "nombre_venta": nombre,
        "tipo": tipo,
        "codigo_carrito": codigo,
        "descripcion_carrito": f"DESC {codigo}",
        "score": score,
    }
    stock = {
        "codigo": codigo,
        "descripcion": f"DESC {codigo}",
        "grupo": grupo,
        "stock_seg": stock_seg,
        "stock_real": stock_real,
    }
    return mapeo, stock


def test_modo_normal_formula_basica(build_mapeo, build_ventas, build_stock):
    mapeo_row, stock_row = _fila("4000036", "LIMON AL AGUA", stock_seg=10, stock_real=5)
    mapeo = build_mapeo([mapeo_row])
    stock = build_stock([stock_row])
    ventas = build_ventas([{"nombre_venta": "LIMON AL AGUA", "venta": 20, "tipo": "granel"}])

    resultado = calcular_pedido(ventas, mapeo, stock, pct_stock_seg=100, pct_ajuste_venta=0)

    fila = resultado.set_index("codigo_carrito").loc["4000036"]
    # pedido_calc = max(0, ceil(20 + 10*1.0 - 5)) = 25
    assert fila["pedido_calc"] == 25
    assert fila["pedido"] == 25
    assert fila["ajuste_plan"] == AJUSTE_NINGUNO


def test_ajuste_venta_porcentual(build_mapeo, build_ventas, build_stock):
    mapeo_row, stock_row = _fila("4000036", "LIMON AL AGUA", stock_seg=0, stock_real=0)
    mapeo = build_mapeo([mapeo_row])
    stock = build_stock([stock_row])
    ventas = build_ventas([{"nombre_venta": "LIMON AL AGUA", "venta": 100, "tipo": "granel"}])

    resultado = calcular_pedido(ventas, mapeo, stock, pct_stock_seg=0, pct_ajuste_venta=10)

    fila = resultado.set_index("codigo_carrito").loc["4000036"]
    # venta_ajustada = 100 * (1 + 10/100); ceil() sobre el resultado tal cual lo
    # calcula la función, para no depender de redondeos exactos de punto flotante.
    assert fila["pedido_calc"] == math.ceil(100 * (1 + 10 / 100))


def test_modo_replicar_venta_ignora_stock_y_plan(build_mapeo, build_ventas, build_stock, build_plan):
    mapeo_row, stock_row = _fila("4000036", "LIMON AL AGUA", stock_seg=999, stock_real=999)
    mapeo = build_mapeo([mapeo_row])
    stock = build_stock([stock_row])
    ventas = build_ventas([{"nombre_venta": "LIMON AL AGUA", "venta": 20, "tipo": "granel"}])
    plan = build_plan([{"codigo_homologado": "4000036", "Semana_1_Enero_2026": 5}])

    resultado = calcular_pedido(
        ventas, mapeo, stock,
        plan_df=plan, plan_col="Semana_1_Enero_2026",
        modo_replicar_venta=True,
    )

    fila = resultado.set_index("codigo_carrito").loc["4000036"]
    assert fila["pedido"] == 20
    assert fila["pedido_calc"] == 20
    assert fila["ajuste_plan"] == AJUSTE_NINGUNO


def test_banda_de_plan_sube_baja_cero_y_falta(build_mapeo, build_ventas, build_stock, build_plan):
    # A: pedido_calc bajo -> se sube al mínimo (97% del plan)
    mapeo_a, stock_a = _fila("5000001", "PRODUCTO A", stock_seg=0, stock_real=0)
    # B: pedido_calc alto -> se baja al máximo (105% del plan)
    mapeo_b, stock_b = _fila("5000002", "PRODUCTO B", stock_seg=0, stock_real=0)
    # C: plan en 0 -> pedido forzado a 0
    mapeo_c, stock_c = _fila("5000003", "PRODUCTO C", stock_seg=0, stock_real=0)
    # D: sin entrada en el plan -> pedido forzado a 0
    mapeo_d, stock_d = _fila("5000004", "PRODUCTO D", stock_seg=0, stock_real=0)

    mapeo = build_mapeo([mapeo_a, mapeo_b, mapeo_c, mapeo_d])
    stock = build_stock([stock_a, stock_b, stock_c, stock_d])
    ventas = build_ventas([
        {"nombre_venta": "PRODUCTO A", "venta": 50, "tipo": "granel"},   # calc=50
        {"nombre_venta": "PRODUCTO B", "venta": 200, "tipo": "granel"},  # calc=200
        {"nombre_venta": "PRODUCTO C", "venta": 10, "tipo": "granel"},   # calc=10
        {"nombre_venta": "PRODUCTO D", "venta": 10, "tipo": "granel"},   # calc=10
    ])
    plan = build_plan([{
        "codigo_homologado": "5000001", "Semana_1_Enero_2026": 100,
    }, {
        "codigo_homologado": "5000002", "Semana_1_Enero_2026": 100,
    }, {
        "codigo_homologado": "5000003", "Semana_1_Enero_2026": 0,
    }])
    # 5000004 deliberadamente ausente del plan.

    resultado = calcular_pedido(
        ventas, mapeo, stock,
        plan_df=plan, plan_col="Semana_1_Enero_2026",
    ).set_index("codigo_carrito")

    # lim_min = ceil(100*0.97) = 97, lim_max = floor(100*1.05) = 105
    assert resultado.loc["5000001", "pedido"] == 97
    assert resultado.loc["5000001", "ajuste_plan"] == AJUSTE_SUBE

    assert resultado.loc["5000002", "pedido"] == 105
    assert resultado.loc["5000002", "ajuste_plan"] == AJUSTE_BAJA

    assert resultado.loc["5000003", "pedido"] == 0
    assert resultado.loc["5000003", "ajuste_plan"] == AJUSTE_PLAN_CERO

    assert resultado.loc["5000004", "pedido"] == 0
    assert resultado.loc["5000004", "ajuste_plan"] == AJUSTE_SIN_PLAN


def test_sku_granel_sin_venta_aparece_en_cero(build_mapeo, build_ventas, build_stock):
    # Sabor granel con stock pero sin ninguna venta en la semana (quiebre/faltante).
    _, stock_sin_venta = _fila("4000099", "SABOR FALTANTE", grupo=GRANEL, stock_seg=5, stock_real=2)
    mapeo_otro, stock_otro = _fila("4000036", "LIMON AL AGUA", stock_seg=0, stock_real=0)

    mapeo = build_mapeo([mapeo_otro])
    stock = build_stock([stock_sin_venta, stock_otro])
    ventas = build_ventas([{"nombre_venta": "LIMON AL AGUA", "venta": 10, "tipo": "granel"}])

    resultado = calcular_pedido(ventas, mapeo, stock).set_index("codigo_carrito")

    assert resultado.loc["4000099", "venta"] == 0
    assert resultado.loc["4000099", "pedido"] == 0
    assert resultado.loc["4000099", "pedido_calc"] == 0


def test_mapeo_desc_no_se_desalinea_con_codigos_duplicados(build_mapeo, build_ventas, build_stock):
    """Regresión del bug de ago 2026 (pedido_logic.py, calcular_pedido).

    mapeo_productos.csv real tiene codigo_carrito duplicado (varios nombre_venta
    apuntan al mismo producto) y arrastra sufijos ".0" de Excel en algunas filas
    (por eso existe la normalización `.str.replace(r"\\.0$", "", ...)` en varios
    puntos del código). El zip posicional viejo comparaba esa columna ya
    normalizada contra la cruda sin normalizar: una fila con sufijo ".0" no
    matcheaba en el `isin`, desalineando las dos secuencias y dejando códigos
    sin descripción (o con la de otro producto) al resolver vía mapeo.
    """
    mapeo = build_mapeo([
        # Dos nombres de venta para el mismo código, uno con el sufijo ".0" que
        # arrastra un CSV re-exportado desde Excel (caso real: Casatta).
        {"nombre_venta": "A", "tipo": "empaquetado", "codigo_carrito": "4000142",
         "descripcion_carrito": "PACK CASSATA", "score": 100},
        {"nombre_venta": "B", "tipo": "empaquetado", "codigo_carrito": "4000142.0",
         "descripcion_carrito": "PACK CASSATA", "score": 100},
        # Producto distinto, vendido, sin fila en Stock.
        {"nombre_venta": "C", "tipo": "empaquetado", "codigo_carrito": "4000200",
         "descripcion_carrito": "OTRO PRODUCTO", "score": 100},
    ])
    # Stock vacío a propósito (pero con las columnas del contrato): fuerza que
    # la descripción de "C" se resuelva por el fallback de mapeo_desc, no por
    # el merge con stock.
    stock = pd.DataFrame(columns=["codigo", "descripcion", "grupo", "stock_seg", "stock_real"])
    ventas = build_ventas([{"nombre_venta": "C", "venta": 5, "tipo": "empaquetado"}])

    resultado = calcular_pedido(ventas, mapeo, stock).set_index("codigo_carrito")

    assert resultado.loc["4000200", "descripcion"] == "OTRO PRODUCTO"
