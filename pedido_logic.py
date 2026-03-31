"""
Lógica de negocio para el cálculo del pedido semanal de helado.

Flujo: ventas (cajas terminadas + mixventas) + stock → pedido → carrito Excel
"""

import math
import os
from io import BytesIO
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from rapidfuzz import fuzz, process

DATA_DIR = Path(__file__).parent / "data"
MAPEO_PATH = Path(__file__).parent / "mapeo_productos.csv"


# ---------------------------------------------------------------------------
# 1. Carga de archivos de ventas
# ---------------------------------------------------------------------------

def cargar_cajas_terminadas(file) -> pd.DataFrame:
    """Lee el .xls de cajas terminadas y agrupa por sabor (consolidado)."""
    df = pd.read_excel(file, engine="xlrd")
    agrupado = (
        df.groupby("artdescrip", as_index=False)["ctecantidad"]
        .sum()
        .rename(columns={"artdescrip": "nombre_venta", "ctecantidad": "venta"})
    )
    agrupado["tipo"] = "granel"
    return agrupado


def cargar_mixventas(file) -> pd.DataFrame:
    """Lee el .xls de mixventas; filtra productos con bultos > 0 y subgrupo == 1.

    Productos granel vendidos a mayoristas (ej. baldes 7,8 kg) se detectan
    automáticamente por patrón en el nombre y se etiquetan como "granel"
    con el nombre en MAYÚSCULAS para coincidir con cajas terminadas.
    """
    df = pd.read_excel(file, engine="xlrd")
    mask = (df["subgrupo"] == 1) & (df["bultos"].notna()) & (df["bultos"] > 0)
    resultado = (
        df.loc[mask, ["artdescrip", "bultos"]]
        .rename(columns={"artdescrip": "nombre_venta", "bultos": "venta"})
        .copy()
    )

    _GRANEL_RE = r"\(7[,.]8"
    is_granel = resultado["nombre_venta"].str.contains(
        _GRANEL_RE, case=False, na=False
    )
    resultado["tipo"] = "empaquetado"
    resultado.loc[is_granel, "tipo"] = "granel"
    resultado.loc[is_granel, "nombre_venta"] = (
        resultado.loc[is_granel, "nombre_venta"].str.upper()
    )
    return resultado


# ---------------------------------------------------------------------------
# 2. Carga del archivo de stock
# ---------------------------------------------------------------------------

def cargar_stock(file) -> pd.DataFrame:
    """
    Lee el archivo de stock (.xlsx).
    Retorna DataFrame con: codigo, descripcion, stock_seg, stock_real.
    Fila 0 del Excel es el header.
    """
    df = pd.read_excel(file, header=None, skiprows=1)
    stock = pd.DataFrame({
        "codigo": df[1].astype(str).str.replace(r"\.0$", "", regex=True).str.strip(),
        "descripcion": df[4].astype(str).str.strip(),
        "grupo": df[0].astype(str).str.strip(),
        "stock_seg": pd.to_numeric(df[5], errors="coerce").fillna(0),
        "stock_real": pd.to_numeric(df[6], errors="coerce").fillna(0),
    })
    return stock


# ---------------------------------------------------------------------------
# 3. Mapeo de nombres de ventas → código del carrito
# ---------------------------------------------------------------------------

GRUPOS_GRANEL = {
    "H13- SABORES AL AGUA", "H14- SABORES COMUNES",
    "H15- SABORES ESPECIALES", "H16- SABORES PREMIUM",
}

GRUPOS_EMPAQUETADO = {
    "H17- PALITOS", "H18- IMPULSIVOS", "H21- TORTAS Y POSTRES",
    "H22- FAMILIAR", "H23- POTE 1 LTS", "H02- FRIZZIO",
    "H12- FRUTAS BAÑADAS", "H03- SUNDAE GO",
    "H10- CONGELADOS - FUDY", "H24- CONGELADOS - EASY FRUT",
    "H20- PRODUCTOS SIN TACC",
}

# Mapeos manuales conocidos que el fuzzy matching no resuelve bien
_OVERRIDE_GRANEL = {
    "MASCARPONE CON FRUTOS DEL BOSQUE (7,8KG)": "4000058",
    "MANGO AL AGUA 7.8KG": "4000907",
    "FRUTILLA (7,8KG)": "4000194",
    "MARACUYA (7,8KG)": "4000038",
    "DULCE DE LECHE CON NUEZ (7,8KG)": "4000068",
    "CHOCOLATE CON ALMENDRAS (7,8KG)": "4000067",
}

# Descripciones para productos que están en el carrito pero no en el archivo de stock
_DESC_EXTRA = {
    "4000907": "MANGO AL AGUA 7,800 KG GRIDO",
}

_OVERRIDE_EMPAQUETADO = {
    "Familiar Nº 1": "4000163",
    "Familiar Nº 2": "4000164",
    "Familiar Nº 3": "4000165",
    "Familiar Nº 4": "4000166",
    "Tentacion 1 Lt Chocolate": "4000158",
    "Tentacion 1 Lt Chocolate Con Almendras": "4000155",
    "Tentacion 1 Lt Cookie": "4000157",
    "Tentacion 1 Lt Crema Americana": "4000162",
    "Tentacion 1 Lt Dulce de Leche": "4000159",
    "Tentacion 1 Lt Dulce de Leche Granizado": "4000153",
    "Tentacion 1 Lt Frutilla": "4000160",
    "Tentacion 1 Lt Granizado": "4000152",
    "Tentacion 1 Lt Limon": "4000156",
    "Tentacion 1 Lt Vainilla": "4000161",
    "Tentación 1l Menta Granizada": "4000670",
    "Tentacion Toddy Galletitas": "4000341",
    "Cups Black x 3": "6002071",
    "Cups Conito x 5 Un.": "6000014",
    "Cups Familiar  x 3 Cucuruchos": "6003296",
    "Cups Vasitos x 6": "6000196",
    "Bastoncito de Muzarella": "6002672",
    "Pechuguita de Pollo": "6002673",
    "Mini Frizzio": "6002674",
    "Pizza Frizzio Mozzarella": "6002671",
    "Pizza Frizzio Integral": "6002675",
    "Pizza Moz. y Jamon Frizzio": "6002679",
    "Pizza Tipo Casera": "6002680",
    "Empanada de Carne Frizzio 4u x 80g": "6003299",
    "Empanada de Jyq Frizzio 4u x 80g": "6003300",
    "Casatta en Caja x 8": "4000142",
    "Casatta x Unidad": "4000142",
    "Grido Tops Mini Rocklets": "6000999",
    "Grido Tops x 204 U": "",
    "Lunchera Oficial Selección Argentina": "",
    "Pote Reutilizable": "6002422",
    "Salsas x 500 Grs": "6000430",
    "Mermelada Frutilla Grido": "6000973",
    "Palito Bombon en Caja x 20": "4000859",
    "Palito Bombón X10 Un.": "4000859",
    "Cremoso Americana en Caja x 20": "4000854",
    "Cremoso Frutilla en Caja x 20": "4000855",
    "Palito Cremoso Americana X10 U.": "4000854",
    "Palito Cremoso Americana\xa0X10 U.": "4000854",
    "Palito Cremoso Frutilla X10 U.": "4000855",
    "Palito Frutal Frutilla en Caja x 20": "4000138",
    "Palito Frutal Limon en Caja x 20": "4000139",
    "Palito Frutal Naranja en Caja X20": "4000141",
    "Palito Frutal Frutilla en Caja x 10": "4000856",
    "Palito Frutal Naranja en Caja X10": "4000858",
    "Almendrado en Caja x 8": "4000143",
    "Almendrado x Unidad": "4000143",
    "Bombon Crocante en Caja x 8": "4000145",
    "Bombon Crocante x Unidad": "4000145",
    "Bombon Escoces en Caja x 8": "4000147",
    "Bombon Escoces x Unidad": "4000147",
    "Bombon Frutezza X8u.": "4000867",
    "Bombon Frutezza Xu.": "4000867",
    "Bombon Suizo en Caja x 8": "4000146",
    "Bombon Suizo x Unidad": "4000146",
    "Crocantino": "4000144",
    "Barra Delicia Chocolate y Mani": "4000593",
    "Postre Veg. Maní 80 Gr x Un": "6002857",
    "Alfajor Secreto Cookies And Cream x Un.": "4000823",
    "Alfajor Secreto Cookies And Cream X6": "4000823",
    "Torta Grido Rellena": "4000446",
    "Torta Cookies And Cream": "4000835",
    "Torta Frutillas Con Crema": "4000892",
    "Frambuesas Doble Chocolate X120": "6002434",
    "Frutillas Doble Chocolate X120": "6002435",
}

_ALL_OVERRIDES = {**_OVERRIDE_GRANEL, **_OVERRIDE_EMPAQUETADO}

# Also index by normalized keys (replace \xa0 with space, strip)
_OVERRIDES_NORMALIZED = {}
for k, v in _ALL_OVERRIDES.items():
    norm_key = k.replace("\xa0", " ").strip()
    _OVERRIDES_NORMALIZED[norm_key] = v


def _normalizar(texto: str) -> str:
    """Normaliza texto para mejorar el fuzzy matching."""
    import re
    t = texto.upper().strip()
    t = re.sub(r"\(7[,.]8\s*KG?\)", "", t)
    t = re.sub(r"7[,.]800?\s*KG", "", t)
    t = re.sub(r"7[.]8\s*KG", "", t)
    for word in ("GRIDO", "PACK", "CAJAS", "CAJ.", " - "):
        t = t.replace(word, " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def generar_mapeo(
    ventas: pd.DataFrame,
    stock_df: pd.DataFrame,
    mapeo_path: Path | str = MAPEO_PATH,
    score_threshold: int = 40,
) -> pd.DataFrame:
    """
    Si mapeo_productos.csv existe, lo carga y solo agrega productos nuevos.
    Si no existe, genera uno completo con overrides manuales + fuzzy matching
    restringido por grupo de productos.
    """
    mapeo_path = Path(mapeo_path)

    existente = None
    if mapeo_path.exists():
        existente = pd.read_csv(mapeo_path, dtype={"codigo_carrito": str})
        existente["codigo_carrito"] = (
            existente["codigo_carrito"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
        nombres_ya = set(existente["nombre_venta"].str.strip())
        nuevos = ventas[~ventas["nombre_venta"].str.strip().isin(nombres_ya)]
        if nuevos.empty:
            return existente
        ventas_a_mapear = nuevos
    else:
        ventas_a_mapear = ventas

    codigo_a_desc = dict(zip(stock_df["codigo"], stock_df["descripcion"]))
    codigo_a_desc.update(_DESC_EXTRA)

    granel_mask = stock_df["grupo"].isin(GRUPOS_GRANEL)
    empaq_mask = stock_df["grupo"].isin(GRUPOS_EMPAQUETADO)
    subsets = {
        "granel": stock_df[granel_mask].reset_index(drop=True),
        "empaquetado": stock_df[empaq_mask].reset_index(drop=True),
    }

    filas = []
    for _, row in ventas_a_mapear.iterrows():
        nombre = row["nombre_venta"]
        tipo = row["tipo"]

        nombre_clean = nombre.replace("\xa0", " ").strip()
        override_cod = _OVERRIDES_NORMALIZED.get(nombre_clean)
        if override_cod is not None:
            cod = override_cod
            if cod:
                filas.append({
                    "nombre_venta": nombre,
                    "tipo": tipo,
                    "codigo_carrito": cod,
                    "descripcion_carrito": codigo_a_desc.get(cod, "???"),
                    "score": 100,
                })
            else:
                filas.append({
                    "nombre_venta": nombre,
                    "tipo": tipo,
                    "codigo_carrito": "",
                    "descripcion_carrito": "NO APLICA",
                    "score": 0,
                })
            continue

        subset = subsets.get(tipo, stock_df)
        descs = subset["descripcion"].tolist()
        cods = subset["codigo"].tolist()
        descs_norm = [_normalizar(d) for d in descs]
        nombre_norm = _normalizar(nombre)

        resultado = process.extractOne(
            nombre_norm,
            descs_norm,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=score_threshold,
        )
        if resultado:
            match_text, score, idx = resultado
            filas.append({
                "nombre_venta": nombre,
                "tipo": tipo,
                "codigo_carrito": cods[idx],
                "descripcion_carrito": descs[idx],
                "score": int(score),
            })
        else:
            filas.append({
                "nombre_venta": nombre,
                "tipo": tipo,
                "codigo_carrito": "",
                "descripcion_carrito": "SIN MAPEO",
                "score": 0,
            })

    nuevos_df = pd.DataFrame(filas)

    if existente is not None:
        resultado_final = pd.concat([existente, nuevos_df], ignore_index=True)
    else:
        resultado_final = nuevos_df

    try:
        resultado_final.to_csv(mapeo_path, index=False)
    except OSError:
        pass
    return resultado_final


def cargar_mapeo(mapeo_path: Path | str = MAPEO_PATH) -> pd.DataFrame | None:
    """Carga el mapeo existente si hay uno (disco o session_state)."""
    import streamlit as _st

    if "mapeo_df" in _st.session_state:
        return _st.session_state["mapeo_df"]

    mapeo_path = Path(mapeo_path)
    if mapeo_path.exists():
        df = pd.read_csv(mapeo_path, dtype={"codigo_carrito": str})
        df["codigo_carrito"] = (
            df["codigo_carrito"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
        return df
    return None


# ---------------------------------------------------------------------------
# 4. Cálculo del pedido
# ---------------------------------------------------------------------------

def calcular_pedido(
    ventas: pd.DataFrame,
    mapeo: pd.DataFrame,
    stock_df: pd.DataFrame,
    pct_stock_seg: int = 100,
    pct_ajuste_venta: float = 0,
) -> pd.DataFrame:
    """
    Calcula el pedido de reposición por producto.

    Lógica:
    - venta_ajustada = venta * (1 + pct_ajuste_venta/100)
    - Si stock_real == 0: pedido = ceil(venta_ajustada * 1.5)
    - Si stock_real > 0:  pedido = max(0, ceil(venta_ajustada + stock_seg * pct/100 - stock_real))

    Retorna DataFrame con: codigo, descripcion, grupo, venta, stock_real, stock_seg, pedido
    (venta = venta ajustada usada en el cálculo)
    """
    mapeo_valido = mapeo[
        (mapeo["codigo_carrito"].notna())
        & (mapeo["codigo_carrito"].astype(str).str.strip() != "")
        & (mapeo["descripcion_carrito"] != "SIN MAPEO")
        & (mapeo["descripcion_carrito"] != "NO APLICA")
    ].copy()
    mapeo_valido["codigo_carrito"] = (
        mapeo_valido["codigo_carrito"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )

    ventas_con_codigo = ventas.merge(
        mapeo_valido[["nombre_venta", "codigo_carrito"]],
        on="nombre_venta",
        how="inner",
    )

    venta_por_codigo = (
        ventas_con_codigo
        .groupby("codigo_carrito", as_index=False)["venta"]
        .sum()
    )

    stock_df = stock_df.copy()
    stock_df["codigo"] = stock_df["codigo"].astype(str)

    pedido_df = venta_por_codigo.merge(
        stock_df[["codigo", "descripcion", "grupo", "stock_seg", "stock_real"]],
        left_on="codigo_carrito",
        right_on="codigo",
        how="left",
    )

    pedido_df["stock_seg"] = pedido_df["stock_seg"].fillna(0)
    pedido_df["stock_real"] = pedido_df["stock_real"].fillna(0)

    # Ajuste de venta por estacionalidad: venta * (1 + pct/100)
    pedido_df["venta"] = pedido_df["venta"] * (1 + pct_ajuste_venta / 100)

    if pedido_df["descripcion"].isna().any():
        mapeo_desc = dict(
            zip(
                mapeo_valido["codigo_carrito"],
                mapeo.loc[mapeo["codigo_carrito"].isin(mapeo_valido["codigo_carrito"]), "descripcion_carrito"],
            )
        )
        mask = pedido_df["descripcion"].isna()
        pedido_df.loc[mask, "descripcion"] = (
            pedido_df.loc[mask, "codigo_carrito"].map(mapeo_desc)
        )
        pedido_df["grupo"] = pedido_df["grupo"].fillna("")

    factor = pct_stock_seg / 100

    def _calc_pedido(row):
        if row["stock_real"] == 0:
            return math.ceil(row["venta"] * 1.5)
        return max(0, math.ceil(row["venta"] + row["stock_seg"] * factor - row["stock_real"]))

    pedido_df["pedido"] = pedido_df.apply(_calc_pedido, axis=1)

    cols = [
        "codigo_carrito", "descripcion", "grupo",
        "venta", "stock_real", "stock_seg", "pedido",
    ]
    return pedido_df[cols].sort_values("grupo").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 5. Escritura del carrito Excel
# ---------------------------------------------------------------------------

def _guardar_plantilla(file, dest: Path | str = DATA_DIR / "carrito_template.xlsx") -> bytes | None:
    """Guarda una copia de la plantilla del carrito. Retorna los bytes para session_state."""
    dest = Path(dest)
    if hasattr(file, "read"):
        contenido = file.read()
        file.seek(0)
    else:
        contenido = Path(file).read_bytes()

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(contenido)
    except OSError:
        pass
    return contenido


def obtener_plantilla() -> Path | None:
    """Retorna la ruta de la última plantilla guardada, si existe."""
    p = DATA_DIR / "carrito_template.xlsx"
    return p if p.exists() else None


def cargar_datos_plantilla(
    template_path: str | Path,
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """
    Extrae cubicaje (col F), peso (col G) y precio (col I) de la plantilla.
    Retorna ({codigo: cubicaje}, {codigo: precio}, {codigo: peso_kg}).
    """
    wb = load_workbook(template_path, read_only=True, data_only=True)
    ws = wb.active
    cubicaje: dict[str, float] = {}
    precios: dict[str, float] = {}
    pesos: dict[str, float] = {}
    for row_idx in range(2, ws.max_row + 1):
        codigo_cell = ws.cell(row=row_idx, column=2).value
        if codigo_cell is None:
            continue
        codigo = str(codigo_cell).strip()
        cub = ws.cell(row=row_idx, column=6).value
        peso = ws.cell(row=row_idx, column=7).value
        pre = ws.cell(row=row_idx, column=9).value
        try:
            cubicaje[codigo] = float(cub)
        except (TypeError, ValueError):
            cubicaje[codigo] = 0.0
        try:
            precios[codigo] = float(pre)
        except (TypeError, ValueError):
            precios[codigo] = 0.0
        try:
            pesos[codigo] = float(peso)
        except (TypeError, ValueError):
            pesos[codigo] = 0.0
    wb.close()
    return cubicaje, precios, pesos


def cargar_cubicaje(template_path: str | Path) -> dict[str, float]:
    """Extrae el cubicaje (columna F) de la plantilla del carrito. Retorna {codigo: cubicaje}."""
    cubicaje, _, _ = cargar_datos_plantilla(template_path)
    return cubicaje


def escribir_carrito(
    template_path: str | Path,
    pedido_df: pd.DataFrame,
) -> BytesIO:
    """
    Abre la plantilla del carrito con openpyxl, escribe las cantidades
    en la columna C (índice 3 en openpyxl, fila 2+ porque fila 1 es header),
    y retorna un BytesIO con el archivo listo para descargar.
    """
    wb = load_workbook(template_path)
    ws = wb.active

    pedido_dict = dict(
        zip(
            pedido_df["codigo_carrito"].astype(str),
            pedido_df["pedido"],
        )
    )

    for row_idx in range(2, ws.max_row + 1):
        codigo_cell = ws.cell(row=row_idx, column=2).value
        if codigo_cell is None:
            continue
        codigo = str(codigo_cell).strip()
        if codigo in pedido_dict:
            ws.cell(row=row_idx, column=3).value = pedido_dict[codigo]

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer
