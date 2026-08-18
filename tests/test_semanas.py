"""Tests del parseo de columnas semanales y del cálculo de rangos domingo→sábado."""

from datetime import timedelta

import pytest

from pedido_logic import (
    _parsear_semana,
    fin_semana,
    inicio_semana,
    obtener_semanas,
    semana_default,
)


@pytest.mark.parametrize("col,esperado", [
    ("Semana_18_Mayo_2026", (2026, 18, 5)),
    ("Semana_1_Enero_2026", (2026, 1, 1)),
    ("Semana_1_Setiembre_2026", (2026, 1, 9)),  # variante regional de "septiembre"
    ("Semana_1_Septiembre_2026", (2026, 1, 9)),
])
def test_parsear_semana_columnas_validas(col, esperado):
    assert _parsear_semana(col) == esperado


@pytest.mark.parametrize("col", [
    "codigo_homologado",
    "total_temporada",
    "Semana_X_Mayo_2026",
    "semana_1_enero_2026",  # sin mayúscula inicial en "Semana"
])
def test_parsear_semana_columnas_invalidas(col):
    assert _parsear_semana(col) is None


def test_inicio_semana_1_es_domingo():
    inicio = inicio_semana(2026, 1)
    assert inicio.weekday() == 6  # domingo


def test_fin_semana_es_seis_dias_despues_y_sabado():
    inicio = inicio_semana(2026, 10)
    fin = fin_semana(2026, 10)
    assert fin == inicio + (fin - inicio)
    assert (fin - inicio).days == 6
    assert fin.weekday() == 5  # sábado


def test_semanas_consecutivas_estan_separadas_siete_dias():
    s1 = inicio_semana(2026, 1)
    s2 = inicio_semana(2026, 2)
    assert (s2 - s1).days == 7


def test_obtener_semanas_ordena_e_ignora_columnas_no_semanales(build_plan):
    plan = build_plan([{
        "codigo_homologado": "4000036",
        "descripcion": "x",
        "Semana_2_Enero_2026": 10,
        "Semana_1_Enero_2026": 5,
        "total_temporada": 15,
    }])

    semanas = obtener_semanas(plan)

    assert [nombre for nombre, _ in semanas] == ["Semana_1_Enero_2026", "Semana_2_Enero_2026"]
    assert semanas[0][1] < semanas[1][1]


def test_semana_default_dentro_del_rango(build_plan):
    plan = build_plan([{
        "codigo_homologado": "4000036",
        "Semana_1_Enero_2026": 5,
        "Semana_2_Enero_2026": 8,
    }])
    inicio_s2 = inicio_semana(2026, 2)

    assert semana_default(plan, hoy=inicio_s2 + timedelta(days=3)) == "Semana_2_Enero_2026"


def test_semana_default_antes_de_la_primera_semana(build_plan):
    plan = build_plan([{
        "codigo_homologado": "4000036",
        "Semana_1_Enero_2026": 5,
        "Semana_2_Enero_2026": 8,
    }])
    inicio_s1 = inicio_semana(2026, 1)

    assert semana_default(plan, hoy=inicio_s1 - timedelta(days=30)) == "Semana_1_Enero_2026"


def test_semana_default_despues_de_la_ultima_semana(build_plan):
    plan = build_plan([{
        "codigo_homologado": "4000036",
        "Semana_1_Enero_2026": 5,
        "Semana_2_Enero_2026": 8,
    }])
    fin_s2 = fin_semana(2026, 2)

    assert semana_default(plan, hoy=fin_s2 + timedelta(days=30)) == "Semana_2_Enero_2026"


def test_semana_default_sin_columnas_semanales(build_plan):
    plan = build_plan([{"codigo_homologado": "4000036", "total_temporada": 15}])

    assert semana_default(plan) is None
