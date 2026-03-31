"""
App Streamlit para calcular el pedido semanal de helado.

Uso local:
    streamlit run app_pedido.py

Deploy en Streamlit Cloud:
    Subir repo a GitHub y conectar desde share.streamlit.io
"""

import streamlit as st
import pandas as pd
from io import BytesIO
from pathlib import Path

from pedido_logic import (
    cargar_cajas_terminadas,
    cargar_mixventas,
    cargar_stock,
    generar_mapeo,
    cargar_mapeo,
    calcular_pedido,
    cargar_datos_plantilla,
    escribir_carrito,
    obtener_plantilla,
    _guardar_plantilla,
    MAPEO_PATH,
    GRUPOS_GRANEL,
)

st.set_page_config(page_title="Pedido Semanal Grido", page_icon="🍦", layout="wide")

# ── Sidebar: plantilla del carrito ────────────────────────────────────────────

with st.sidebar:
    st.header("Configuración")

    st.subheader("Plantilla del carrito")
    plantilla_actual = obtener_plantilla()
    if plantilla_actual:
        st.success(f"Plantilla cargada: {plantilla_actual.name}")
    else:
        st.warning("No hay plantilla guardada")

    nueva_plantilla = st.file_uploader(
        "Actualizar plantilla del carrito",
        type=["xlsx"],
        key="plantilla_upload",
        help="Solo necesario si cambió la plantilla del Modelo de Carrito",
    )
    if nueva_plantilla:
        contenido = _guardar_plantilla(nueva_plantilla)
        if contenido:
            st.session_state["plantilla_bytes"] = contenido
        st.success("Plantilla actualizada")
        st.rerun()

    st.divider()
    st.subheader("Mapeo de productos")

    mapeo_upload = st.file_uploader(
        "Cargar mapeo (.csv)",
        type=["csv"],
        key="mapeo_upload",
        help="Subí un mapeo_productos.csv corregido manualmente",
    )
    if mapeo_upload:
        mapeo_cargado = pd.read_csv(mapeo_upload, dtype={"codigo_carrito": str})
        mapeo_cargado["codigo_carrito"] = (
            mapeo_cargado["codigo_carrito"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
        st.session_state["mapeo_df"] = mapeo_cargado
        try:
            mapeo_cargado.to_csv(MAPEO_PATH, index=False)
        except OSError:
            pass
        st.success("Mapeo actualizado")
        st.rerun()

    mapeo_existente = cargar_mapeo()
    if mapeo_existente is not None:
        n_total = len(mapeo_existente)
        n_sin = (mapeo_existente["descripcion_carrito"] == "SIN MAPEO").sum()
        n_ok = n_total - n_sin
        st.metric("Productos mapeados", f"{n_ok}/{n_total}")
        if n_sin > 0:
            st.warning(f"{n_sin} productos sin mapeo")

        csv_buffer = mapeo_existente.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Descargar mapeo actual",
            data=csv_buffer,
            file_name="mapeo_productos.csv",
            mime="text/csv",
        )
    else:
        st.info("Se generará automáticamente al procesar los datos")

# ── Área principal ────────────────────────────────────────────────────────────

st.title("Pedido Semanal de Helado")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Cajas Terminadas")
    file_cajas = st.file_uploader(
        "Archivo de cajas terminadas (.xls)",
        type=["xls"],
        key="cajas",
    )
    if file_cajas:
        st.success(f"{file_cajas.name}")

with col2:
    st.subheader("Mix Ventas")
    file_mix = st.file_uploader(
        "Archivo de mix ventas (.xls)",
        type=["xls"],
        key="mix",
    )
    if file_mix:
        st.success(f"{file_mix.name}")

with col3:
    st.subheader("Stock")
    file_stock = st.file_uploader(
        "Archivo de stock (.xlsx)",
        type=["xlsx"],
        key="stock",
    )
    if file_stock:
        st.success(f"{file_stock.name}")

st.divider()

archivos_listos = all([file_cajas, file_mix, file_stock])


def _resolver_plantilla() -> Path | BytesIO | None:
    """Devuelve la plantilla desde disco o session_state."""
    p = obtener_plantilla()
    if p:
        return p
    if "plantilla_bytes" in st.session_state:
        return BytesIO(st.session_state["plantilla_bytes"])
    return None


plantilla_ok = _resolver_plantilla() is not None

if not plantilla_ok:
    st.warning(
        "Cargá la plantilla del Modelo de Carrito en la barra lateral antes de continuar."
    )

col_pct1, col_pct2, col_btn = st.columns([1, 1, 2])

with col_pct1:
    pct_stock_seg = st.select_slider(
        "% Stock de Seguridad",
        options=[0, 25, 50, 75, 100],
        value=100,
        help="Porcentaje del stock de seguridad a considerar en el cálculo del pedido",
    )

with col_pct2:
    pct_ajuste_venta = st.slider(
        "Ajuste venta (%)",
        min_value=-50,
        max_value=50,
        value=0,
        step=1,
        help="Crecimiento o decrecimiento de la venta para el pedido. Ej: -8% en semanas decrecientes.",
    )

with col_btn:
    st.markdown("<br>", unsafe_allow_html=True)
    btn_calcular = st.button(
        "Calcular Pedido",
        type="primary",
        disabled=not (archivos_listos and plantilla_ok),
        use_container_width=True,
    )

if btn_calcular:
    with st.spinner("Procesando archivos..."):
        ventas_granel = cargar_cajas_terminadas(file_cajas)
        ventas_empaq = cargar_mixventas(file_mix)
        ventas = pd.concat([ventas_granel, ventas_empaq], ignore_index=True)

        st.session_state["ventas_info"] = (
            f"Ventas cargadas: {len(ventas_granel)} sabores granel + "
            f"{len(ventas_empaq)} productos empaquetados"
        )

        stock_df = cargar_stock(file_stock)
        mapeo = generar_mapeo(ventas, stock_df)
        st.session_state["mapeo_df"] = mapeo

        sin_mapeo = mapeo[mapeo["descripcion_carrito"] == "SIN MAPEO"]
        st.session_state["sin_mapeo_df"] = sin_mapeo

        pedido_df = calcular_pedido(
            ventas, mapeo, stock_df,
            pct_stock_seg=pct_stock_seg,
            pct_ajuste_venta=pct_ajuste_venta,
        )

    plantilla = _resolver_plantilla()
    cubicaje_dict, precio_dict, peso_dict = cargar_datos_plantilla(plantilla)
    pedido_df["cubicaje_unit"] = pedido_df["codigo_carrito"].map(cubicaje_dict).fillna(0)
    pedido_df["precio_unit"] = pedido_df["codigo_carrito"].map(precio_dict).fillna(0)
    pedido_df["peso_unit"] = pedido_df["codigo_carrito"].map(peso_dict).fillna(0)

    st.session_state["pedido_base"] = pedido_df
    st.session_state["pedido_params"] = {
        "pct_stock_seg": pct_stock_seg,
        "pct_ajuste_venta": pct_ajuste_venta,
    }
    st.session_state["calc_version"] = st.session_state.get("calc_version", 0) + 1

# ── Resultados (persisten y se actualizan al editar) ─────────────────────────

if "pedido_base" in st.session_state:
    if "ventas_info" in st.session_state:
        st.info(st.session_state["ventas_info"])

    sin_mapeo = st.session_state.get("sin_mapeo_df", pd.DataFrame())
    if not sin_mapeo.empty:
        st.warning(
            f"**{len(sin_mapeo)} productos sin mapeo** (no se incluirán en el pedido). "
            f"Descargá el mapeo desde la barra lateral, corregilo y volvelo a cargar."
        )
        with st.expander("Ver productos sin mapeo"):
            st.dataframe(
                sin_mapeo[["nombre_venta", "tipo"]],
                use_container_width=True,
                hide_index=True,
            )

    pedido_df = st.session_state["pedido_base"].copy()
    editor_key = f"pedido_editor_v{st.session_state.get('calc_version', 0)}"

    # Backfill peso_unit for sessions initiated before this column existed
    if "peso_unit" not in pedido_df.columns:
        plantilla = _resolver_plantilla()
        if plantilla:
            _, _, peso_dict = cargar_datos_plantilla(plantilla)
            pedido_df["peso_unit"] = pedido_df["codigo_carrito"].map(peso_dict).fillna(0)
        else:
            pedido_df["peso_unit"] = 0.0
        st.session_state["pedido_base"] = pedido_df

    # Apply pending edits from previous render so derived columns stay in sync
    if editor_key in st.session_state:
        for row_str, changes in st.session_state[editor_key].get("edited_rows", {}).items():
            if "Pedido" in changes:
                pedido_df.at[int(row_str), "pedido"] = changes["Pedido"]

    pedido_df["cubicaje_total"] = pedido_df["pedido"] * pedido_df["cubicaje_unit"]
    pedido_df["precio_total"] = pedido_df["pedido"] * pedido_df["precio_unit"]

    st.session_state["pedido_base"]["pedido"] = pedido_df["pedido"].values

    # --- Header ---
    n_pedir = int((pedido_df["pedido"] > 0).sum())
    n_total = len(pedido_df)
    params = st.session_state.get("pedido_params", {})

    st.subheader(f"Pedido: {n_pedir} de {n_total} productos a pedir")
    captions = []
    if params.get("pct_stock_seg", 100) < 100:
        captions.append(f"Stock de seguridad al **{params['pct_stock_seg']}%**")
    if params.get("pct_ajuste_venta", 0) != 0:
        captions.append(f"Venta ajustada **{params['pct_ajuste_venta']:+.0f}%**")
    if captions:
        st.caption(" · ".join(captions))

    # --- Tabla editable ---
    COL_RENAME = {
        "codigo_carrito": "Código",
        "descripcion": "Descripción",
        "grupo": "Grupo",
        "venta": "Venta Sem.",
        "stock_real": "Stock Real",
        "stock_seg": "Stock Seg.",
        "pedido": "Pedido",
        "cubicaje_unit": "Cubicaje Unit.",
        "cubicaje_total": "Cubicaje Ped.",
        "precio_unit": "Precio Unit.",
        "precio_total": "Precio Total",
    }
    display_df = pedido_df.rename(columns=COL_RENAME)

    edited_display = st.data_editor(
        display_df,
        key=editor_key,
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in display_df.columns if c != "Pedido"],
        column_config={
            "Venta Sem.": st.column_config.NumberColumn(format="%.1f"),
            "Stock Real": st.column_config.NumberColumn(format="%.0f"),
            "Stock Seg.": st.column_config.NumberColumn(format="%.0f"),
            "Pedido": st.column_config.NumberColumn(format="%.0f", min_value=0),
            "Cubicaje Unit.": st.column_config.NumberColumn(format="%.4f"),
            "Cubicaje Ped.": st.column_config.NumberColumn(format="%.2f"),
            "Precio Unit.": st.column_config.NumberColumn(format="$ %.0f"),
            "Precio Total": st.column_config.NumberColumn(format="$ %.0f"),
        },
    )

    # --- Métricas recalculadas desde los valores editados ---
    edited_pedido = edited_display["Pedido"].fillna(0)
    cub_unit = pedido_df["cubicaje_unit"].values
    pre_unit = pedido_df["precio_unit"].values
    peso_unit = pedido_df["peso_unit"].values

    mask_pos = edited_pedido > 0
    total_bultos = int(edited_pedido[mask_pos].sum())
    total_cubicaje = float((edited_pedido[mask_pos].values * cub_unit[mask_pos.values]).sum())
    subtotal_sin_iva = float((edited_pedido[mask_pos].values * pre_unit[mask_pos.values]).sum())
    total_con_iva = subtotal_sin_iva * 1.21
    total_kilos = float((edited_pedido.values * peso_unit).sum())

    is_granel = pedido_df["grupo"].isin(GRUPOS_GRANEL)
    cajas_granel = int(edited_pedido[is_granel.values].sum())

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Bultos", f"{total_bultos:,}")
    m2.metric("Cajas Granel", f"{cajas_granel:,}")
    m3.metric("Kilos Totales", f"{total_kilos:,.1f} kg")
    m4.metric("Cubicaje Total", f"{total_cubicaje:,.2f}")
    m5.metric("Subtotal (sin IVA)", f"$ {subtotal_sin_iva:,.0f}")
    m6.metric("Total (con IVA 21%)", f"$ {total_con_iva:,.0f}")

    # --- Descarga con valores editados ---
    pedido_export = pedido_df.copy()
    pedido_export["pedido"] = edited_pedido.values

    plantilla = _resolver_plantilla()
    excel_buffer = escribir_carrito(plantilla, pedido_export)

    st.download_button(
        label="Descargar Carrito Excel",
        data=excel_buffer,
        file_name="pedido_semanal.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )
