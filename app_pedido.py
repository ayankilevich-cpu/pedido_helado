"""
App Streamlit para calcular el pedido semanal de helado.

Uso local:
    streamlit run app_pedido.py

Deploy en Streamlit Cloud:
    Subir repo a GitHub y conectar desde share.streamlit.io
"""

import numpy as np
import streamlit as st
import pandas as pd
from datetime import date, timedelta
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
    validar_plantilla_carrito,
    cargar_planificacion,
    _guardar_planificacion,
    obtener_planificacion,
    obtener_semanas,
    semana_default,
    MAPEO_PATH,
    PLAN_PATH,
    GRUPOS_GRANEL,
    AJUSTE_NINGUNO,
    AJUSTE_SUBE,
    AJUSTE_BAJA,
    AJUSTE_PLAN_CERO,
    AJUSTE_SIN_PLAN,
)

st.set_page_config(page_title="Pedido Semanal Grido", page_icon="🍦", layout="wide")


def _pedido_numpy_desde_editor(
    editor_key: str,
    edited_df: pd.DataFrame,
    col_pedido: str,
    n_rows: int,
) -> np.ndarray:
    """
    Columna Pedido tras st.data_editor: el valor devuelto a veces no refleja el último
    cambio; edited_rows en session_state es más fiable. Todo en numpy por índice fijo.
    """
    out = (
        pd.to_numeric(edited_df[col_pedido], errors="coerce")
        .fillna(0)
        .to_numpy(dtype=float)
    )
    if len(out) < n_rows:
        out = np.pad(out, (0, n_rows - len(out)))
    elif len(out) > n_rows:
        out = out[:n_rows]
    else:
        out = out.copy()
    w = st.session_state.get(editor_key)
    edited_rows = None
    if isinstance(w, dict):
        edited_rows = w.get("edited_rows")
    if edited_rows:
        for row_str, changes in edited_rows.items():
            if not isinstance(changes, dict) or col_pedido not in changes:
                continue
            val = changes[col_pedido]
            if val is None:
                continue
            try:
                idx = int(row_str)
                v = float(val)
                if 0 <= idx < len(out):
                    out[idx] = v
            except (TypeError, ValueError):
                pass
    return np.maximum(out, 0.0)


# ── Sidebar: plantilla del carrito ────────────────────────────────────────────

with st.sidebar:
    st.header("Configuración")

    st.subheader("Plantilla del carrito")
    st.caption(
        "Aquí va el **Modelo de Carrito** (.xlsx del portal, hoja `data` con columnas "
        "Codigo, Cubicaje, Peso, precio). No uses este cuadro para el archivo de **Stock** "
        "(ese se carga abajo en la página principal)."
    )
    plantilla_actual = obtener_plantilla()
    if plantilla_actual:
        try:
            _cub_p, _pre_p, _peso_p = cargar_datos_plantilla(plantilla_actual)
            _n_prod = len(_cub_p)
            _mtime = plantilla_actual.stat().st_mtime
            _fecha = pd.Timestamp(_mtime, unit="s").strftime("%d/%m/%Y %H:%M")
            st.success(
                f"Plantilla activa: **{plantilla_actual.name}**  \n"
                f"{_n_prod} productos · actualizada {_fecha}"
            )
            with st.expander("Verificar precios cargados"):
                _muestras = [
                    ("4000036", "LIMON AL AGUA 7,8KG"),
                    ("4000057", "CHOCOLATE SUIZO 7,8KG"),
                    ("4000050", "SUPER GRIDITO 7,8KG"),
                    ("4000043", "CHOCOLATE 7,8KG"),
                    ("4000953", "SELECCION ARGENTINA 7,8KG"),
                ]
                _df = pd.DataFrame(
                    [
                        {
                            "Código": c,
                            "Producto": n,
                            "Precio": _pre_p.get(c),
                            "Cubicaje": _cub_p.get(c),
                            "Peso": _peso_p.get(c),
                        }
                        for c, n in _muestras
                    ]
                )
                st.dataframe(
                    _df,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Precio": st.column_config.NumberColumn(format="$ %.2f"),
                        "Cubicaje": st.column_config.NumberColumn(format="%.3f"),
                        "Peso": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
        except Exception as _e:
            st.success(f"Plantilla cargada: {plantilla_actual.name}")
            st.caption(f"(no se pudo previsualizar: {_e})")
    else:
        st.warning("No hay plantilla guardada")

    if st.button("🔄 Recargar plantilla del disco", use_container_width=True):
        st.session_state.pop("plantilla_bytes", None)
        st.session_state.pop("_plantilla_file_sig", None)
        st.session_state.pop("_plantilla_mtime", None)
        st.session_state["calc_version"] = st.session_state.get("calc_version", 0) + 1
        st.rerun()

    nueva_plantilla = st.file_uploader(
        "Actualizar plantilla del carrito (precios, cubicaje, peso)",
        type=["xlsx", "xlsm"],
        key="plantilla_upload",
        help="Excel «Modelo de Carrito»; al guardarlo, se actualizan precios y totales del pedido mostrado.",
    )
    # No usar st.rerun() aquí: con el archivo aún en el uploader, Streamlit puede
    # encadenar reruns y el botón «Calcular Pedido» no llega a procesarse.
    if nueva_plantilla:
        sig = (nueva_plantilla.name, nueva_plantilla.size)
        if st.session_state.get("_plantilla_file_sig") != sig:
            raw = nueva_plantilla.read()
            nueva_plantilla.seek(0)
            ok, err = validar_plantilla_carrito(raw)
            if not ok:
                st.error(err)
            else:
                contenido = _guardar_plantilla(nueva_plantilla)
                if contenido:
                    st.session_state["plantilla_bytes"] = contenido
                    st.session_state["plantilla_version"] = (
                        st.session_state.get("plantilla_version", 0) + 1
                    )
                st.session_state["_plantilla_file_sig"] = sig
                st.success(
                    "Plantilla actualizada (precios y datos del carrito). "
                    "Podés calcular el pedido cuando quieras."
                )

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

    st.divider()
    st.subheader("Planificación de compras")
    st.caption(
        "CSV con la planificación semanal de compras (columnas `Semana_<n>_<Mes>_<Año>`). "
        "El cálculo de pedido se ajustará para no quedar fuera del rango "
        "[97% , 105%] de la planificación de la semana elegida."
    )

    plan_path_actual = obtener_planificacion()
    if plan_path_actual:
        st.success("Planificación cargada")
    else:
        st.info("Sin planificación cargada (el ajuste por plan no se aplicará)")

    plan_upload = st.file_uploader(
        "Cargar planificación (.csv)",
        type=["csv"],
        key="plan_upload",
    )
    # No usar st.rerun() aquí: con el archivo aún en el uploader, Streamlit puede
    # encadenar reruns y el botón «Calcular Pedido» no llega a procesarse. El
    # mismo run ya actualiza session_state y el cuerpo principal lee el plan.
    if plan_upload:
        sig = (plan_upload.name, plan_upload.size)
        if st.session_state.get("_plan_file_sig") != sig:
            try:
                plan_preview = cargar_planificacion(plan_upload)
                n_semanas = len(obtener_semanas(plan_preview))
                if n_semanas == 0:
                    st.error(
                        "El CSV no tiene columnas `Semana_<n>_<Mes>_<Año>` reconocibles."
                    )
                else:
                    _guardar_planificacion(plan_upload)
                    st.session_state["plan_df"] = plan_preview
                    st.session_state["_plan_file_sig"] = sig
                    st.success(
                        f"Planificación actualizada: {len(plan_preview)} productos · "
                        f"{n_semanas} semanas. Podés calcular el pedido cuando quieras."
                    )
            except Exception as e:
                st.error(f"No se pudo leer el CSV: {e}")

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


# Detección de cambio de plantilla en disco: si el archivo `carrito_template.xlsx`
# fue reemplazado externamente (o por una nueva subida), invalidamos TODO lo
# cacheado en session_state que dependa de la plantilla (`plantilla_bytes`,
# `pedido_base`, `mapeo_df`, editor) para forzar a recalcular desde cero con
# precios/cubicaje/peso del disco actual.
_plantilla_disk = obtener_plantilla()
_plantilla_mtime = _plantilla_disk.stat().st_mtime if _plantilla_disk else None
_prev_mtime = st.session_state.get("_plantilla_mtime")
if _plantilla_mtime != _prev_mtime:
    if _prev_mtime is not None:
        for _k in (
            "plantilla_bytes", "_plantilla_file_sig",
            "pedido_base", "mapeo_df", "sin_mapeo_df",
            "pedido_params", "ventas_info",
        ):
            st.session_state.pop(_k, None)
        st.session_state["calc_version"] = st.session_state.get("calc_version", 0) + 1
    st.session_state["_plantilla_mtime"] = _plantilla_mtime

plantilla_ok = _resolver_plantilla() is not None

if not plantilla_ok:
    st.warning(
        "Cargá la plantilla del Modelo de Carrito en la barra lateral antes de continuar."
    )


def _resolver_plan() -> pd.DataFrame | None:
    """Devuelve el DataFrame del plan (de session_state o disco) o None."""
    if "plan_df" in st.session_state:
        return st.session_state["plan_df"]
    p = obtener_planificacion()
    if p is None:
        return None
    try:
        df = cargar_planificacion(p)
        st.session_state["plan_df"] = df
        return df
    except Exception:
        return None


plan_df = _resolver_plan()
plan_semanas = obtener_semanas(plan_df) if plan_df is not None else []

col_pct1, col_pct2, col_sem, col_btn = st.columns([1, 1, 1.4, 1.3])

with col_pct1:
    pct_stock_seg = st.slider(
        "% Stock de Seguridad",
        min_value=0,
        max_value=100,
        value=100,
        step=1,
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

with col_sem:
    if plan_semanas:
        opciones = [c for c, _ in plan_semanas]
        fechas = {c: f for c, f in plan_semanas}
        default_col = semana_default(plan_df, date.today())
        idx_default = opciones.index(default_col) if default_col in opciones else 0

        def _label_semana(col: str) -> str:
            inicio = fechas[col]
            fin = inicio + timedelta(days=6)
            n = col.split("_")[1]
            return f"S{n} · {inicio.strftime('%d/%m')} → {fin.strftime('%d/%m/%Y')}"

        semana_sel = st.selectbox(
            "Semana de planificación",
            options=opciones,
            index=idx_default,
            format_func=_label_semana,
            help="Semanas del CSV: van de domingo a sábado.",
        )
    else:
        semana_sel = None
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption("Sin planificación cargada")

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
            plan_df=plan_df,
            plan_col=semana_sel,
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
        "semana_sel": semana_sel,
    }
    st.session_state["calc_version"] = st.session_state.get("calc_version", 0) + 1

# ── Resultados (persisten y se actualizan al editar) ─────────────────────────

if "pedido_base" in st.session_state:
    if "ventas_info" in st.session_state:
        st.info(st.session_state["ventas_info"])

    # Precios/cubicaje/peso siempre desde la plantilla actual (nueva subida en sidebar)
    plantilla_viva = _resolver_plantilla()
    if plantilla_viva:
        cubicaje_dict, precio_dict, peso_dict = cargar_datos_plantilla(plantilla_viva)
        pb = st.session_state["pedido_base"].copy()
        pb["cubicaje_unit"] = pb["codigo_carrito"].map(cubicaje_dict).fillna(0)
        pb["precio_unit"] = pb["codigo_carrito"].map(precio_dict).fillna(0)
        pb["peso_unit"] = pb["codigo_carrito"].map(peso_dict).fillna(0)
        st.session_state["pedido_base"] = pb

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

    # Backfill columnas de planificación (sesiones iniciadas antes del feature)
    for col, default in [
        ("plan_sem", pd.NA),
        ("pedido_calc", None),
        ("ajuste_plan", AJUSTE_NINGUNO),
        ("pedido_inicial", None),
    ]:
        if col not in pedido_df.columns:
            if col == "pedido_calc":
                pedido_df[col] = pedido_df["pedido"]
            elif col == "pedido_inicial":
                pedido_df[col] = pedido_df["pedido"]
            else:
                pedido_df[col] = default

    # Apply pending edits from previous render so derived columns stay in sync
    if editor_key in st.session_state:
        for row_str, changes in st.session_state[editor_key].get("edited_rows", {}).items():
            if "Pedido" in changes:
                pedido_df.at[int(row_str), "pedido"] = changes["Pedido"]

    pedido_df["cubicaje_total"] = pedido_df["pedido"] * pedido_df["cubicaje_unit"]
    pedido_df["precio_total"] = pedido_df["pedido"] * pedido_df["precio_unit"]

    plan_aplicado = pedido_df["ajuste_plan"].fillna("").ne("").any() or pedido_df["plan_sem"].notna().any()

    def _texto_ajuste(row) -> str:
        aj = row["ajuste_plan"]
        if aj == AJUSTE_SUBE:
            delta = int(row["pedido_inicial"]) - int(row["pedido_calc"])
            return f"🔺 +{delta}"
        if aj == AJUSTE_BAJA:
            delta = int(row["pedido_calc"]) - int(row["pedido_inicial"])
            return f"🔻 −{delta}"
        if aj == AJUSTE_PLAN_CERO:
            return "🟡 plan = 0"
        if aj == AJUSTE_SIN_PLAN:
            return "⚠️ sin plan"
        return ""

    pedido_df["ajuste_txt"] = pedido_df.apply(_texto_ajuste, axis=1) if plan_aplicado else ""

    n_total = len(pedido_df)
    params = st.session_state.get("pedido_params", {})

    st.subheader("Detalle por producto")
    captions = []
    if params.get("pct_stock_seg", 100) < 100:
        captions.append(f"Stock de seguridad al **{params['pct_stock_seg']}%**")
    if params.get("pct_ajuste_venta", 0) != 0:
        captions.append(f"Venta ajustada **{params['pct_ajuste_venta']:+.0f}%**")
    if plan_aplicado and params.get("semana_sel"):
        captions.append(f"Plan: **{params['semana_sel']}** · banda 97%–105%")
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
        "plan_sem": "Plan Sem.",
        "pedido_calc": "Pedido Calc.",
        "ajuste_txt": "Ajuste",
        "pedido": "Pedido",
        "cubicaje_unit": "Cubicaje Unit.",
        "cubicaje_total": "Cubicaje Ped.",
        "precio_unit": "Precio Unit.",
        "precio_total": "Precio Total",
    }

    cols_visibles = [
        "codigo_carrito", "descripcion", "grupo",
        "venta", "stock_real", "stock_seg",
    ]
    if plan_aplicado:
        cols_visibles += ["plan_sem", "pedido_calc", "ajuste_txt"]
    cols_visibles += [
        "pedido", "cubicaje_unit", "cubicaje_total",
        "precio_unit", "precio_total",
    ]
    # Mantener columnas auxiliares para detectar el ajuste por fila al estilizar
    display_df = pedido_df[cols_visibles + ["ajuste_plan"]].rename(columns=COL_RENAME)

    column_config = {
        "Venta Sem.": st.column_config.NumberColumn(format="%.1f"),
        "Stock Real": st.column_config.NumberColumn(format="%.0f"),
        "Stock Seg.": st.column_config.NumberColumn(format="%.0f"),
        "Pedido": st.column_config.NumberColumn(format="%.0f", min_value=0),
        "Cubicaje Unit.": st.column_config.NumberColumn(format="%.4f"),
        "Cubicaje Ped.": st.column_config.NumberColumn(format="%.2f"),
        "Precio Unit.": st.column_config.NumberColumn(format="$ %.0f"),
        "Precio Total": st.column_config.NumberColumn(format="$ %.0f"),
        "ajuste_plan": None,  # oculta la columna auxiliar
    }
    if plan_aplicado:
        column_config.update({
            "Plan Sem.": st.column_config.NumberColumn(
                format="%.0f",
                help="Cantidad planificada para la semana seleccionada.",
            ),
            "Pedido Calc.": st.column_config.NumberColumn(
                format="%.0f",
                help="Cálculo previo al ajuste por planificación.",
            ),
            "Ajuste": st.column_config.TextColumn(
                help="Ajuste aplicado por la banda 97%–105% del plan."
            ),
        })

    # Coloreado de la columna Pedido según el ajuste por planificación
    def _styler(df: pd.DataFrame):
        def _row(row):
            aj = row.get("ajuste_plan", "")
            if aj == AJUSTE_SUBE:
                style = "background-color: #d4edda; color: #155724; font-weight: 600"
            elif aj == AJUSTE_BAJA:
                style = "background-color: #f8d7da; color: #721c24; font-weight: 600"
            elif aj in (AJUSTE_PLAN_CERO, AJUSTE_SIN_PLAN):
                style = "background-color: #fff3cd; color: #856404; font-weight: 600"
            else:
                style = ""
            return [style if c == "Pedido" else "" for c in row.index]
        return df.style.apply(_row, axis=1)

    edited_display = st.data_editor(
        _styler(display_df) if plan_aplicado else display_df,
        key=editor_key,
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in display_df.columns if c != "Pedido"],
        column_config=column_config,
    )

    # --- Métricas: numpy + edited_rows (evita desalineación de índices y DF “viejo”) ---
    n_rows = len(pedido_df)
    ep = _pedido_numpy_desde_editor(editor_key, edited_display, "Pedido", n_rows)
    cub = pedido_df["cubicaje_unit"].to_numpy(dtype=float, copy=False)
    pre = pedido_df["precio_unit"].to_numpy(dtype=float, copy=False)
    peso = pedido_df["peso_unit"].to_numpy(dtype=float, copy=False)
    granel_mask = pedido_df["grupo"].isin(GRUPOS_GRANEL).to_numpy()

    pos = ep > 0
    total_bultos = int(ep[pos].sum())
    total_cubicaje = float((ep[pos] * cub[pos]).sum())
    subtotal_sin_iva = float((ep[pos] * pre[pos]).sum())
    total_con_iva = subtotal_sin_iva * 1.21
    total_kilos = float((ep * peso).sum())
    cajas_granel = int(ep[granel_mask].sum())

    st.session_state["pedido_base"]["pedido"] = ep

    n_pedir = int((ep > 0).sum())
    st.subheader(f"Resumen: {n_pedir} de {n_total} productos a pedir")

    if plan_aplicado:
        aj_serie = pedido_df["ajuste_plan"]
        n_sube = int((aj_serie == AJUSTE_SUBE).sum())
        n_baja = int((aj_serie == AJUSTE_BAJA).sum())
        n_cero = int((aj_serie == AJUSTE_PLAN_CERO).sum())
        n_sinp = int((aj_serie == AJUSTE_SIN_PLAN).sum())
        n_aj = n_sube + n_baja + n_cero + n_sinp
        if n_aj:
            partes = []
            if n_sube:
                partes.append(f"🔺 {n_sube} subidos al 97%")
            if n_baja:
                partes.append(f"🔻 {n_baja} bajados al 105%")
            if n_cero:
                partes.append(f"🟡 {n_cero} con plan = 0")
            if n_sinp:
                partes.append(f"⚠️ {n_sinp} sin plan")
            st.caption(f"Ajustados por planificación: **{n_aj}** · " + " · ".join(partes))

    r1c1, r1c2, r1c3 = st.columns(3)
    r1c1.metric("Total Bultos", f"{total_bultos:,}")
    r1c2.metric("Cajas Granel", f"{cajas_granel:,}")
    r1c3.metric("Kilos Totales", f"{total_kilos:,.1f} kg")

    r2c1, r2c2, r2c3 = st.columns(3)
    r2c1.metric("Cubicaje Total", f"{total_cubicaje:,.2f}")
    r2c2.metric("Subtotal (sin IVA)", f"$ {subtotal_sin_iva:,.0f}")
    r2c3.metric("Total (con IVA 21%)", f"$ {total_con_iva:,.0f}")

    # --- Descarga con valores editados ---
    pedido_export = pedido_df.copy()
    pedido_export["pedido"] = ep

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
