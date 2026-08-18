# AGENTS.md — Pedido Semanal Grido

> **Instrucción para agentes de IA:** leé este archivo completo antes de modificar código, hacer deploy o responder preguntas sobre el proyecto. Es la fuente de verdad del onboarding. No hace falta explorar el repo desde cero si este documento está actualizado.

**Última actualización:** 18 de agosto de 2026

---

## 1. Qué es este proyecto

App **Streamlit** para calcular el **pedido semanal de reposición de helado** de una sucursal **Grido** (La Falda, Córdoba). Combina ventas de la semana, stock actual y (opcionalmente) planificación de compras para generar cantidades por producto, con precios/cubicaje/peso del **Modelo de Carrito** del portal Grido, y exporta un Excel listo para cargar en el portal.

**Usuario:** dueño/operador de la sucursal (uso interno, no multi-tenant).

---

## 2. Producción y repositorio

| Concepto | Valor |
|----------|--------|
| **App en vivo** | https://pedido-helado.streamlit.app |
| **Repo GitHub (único válido)** | https://github.com/ayankilevich-cpu/pedido_helado |
| **Rama de deploy** | `main` |
| **Entrypoint** | `streamlit run app_pedido.py` |
| **Plataforma** | Streamlit Cloud (conectado a `pedido_helado`) |

### Repositorio obsoleto (NO usar)

`grido-pedido-semanal` **no despliega** la app online. Push ahí no tiene efecto en producción.

### Carpeta local típica

```
~/Documents/MASTER DATA SCIENCE/Clases/GRIDO/PEDIDOS/
```

Verificar siempre:

```bash
git remote -v   # debe mostrar ayankilevich-cpu/pedido_helado.git
```

---

## 3. Arquitectura

```
Entradas (.xls / .xlsx / .csv)
        │
        ▼
  app_pedido.py          ← UI Streamlit, session_state, uploaders, editor
        │
        ▼
  pedido_logic.py        ← carga, mapeo, cálculo, plantilla, export Excel
        │
        ▼
  pedido_semanal.xlsx    ← Modelo de Carrito con cantidades en col. C
```

### Archivos del repo (fuente de verdad)

| Archivo | Rol |
|---------|-----|
| `app_pedido.py` | Interfaz Streamlit (~750 líneas) |
| `pedido_logic.py` | Lógica de negocio (~820 líneas) |
| `mapeo_productos.csv` | Nombre en ventas → código carrito (110 filas) |
| `data/carrito_template.xlsx` | Plantilla del portal: precios, cubicaje, peso (410 productos) |
| `data/compras_semanales_actual.csv` | Planificación semanal (opcional; se sube por UI o se commitea) |
| `requirements.txt` | Dependencias Python |
| `.streamlit/config.toml` | Tema UI, `maxUploadSize = 50` MB |

### Archivos que NO son entrypoint de prod

- `Autom_Pedidos_Nvo.py` — script legacy
- `Stock.xlsx`, `Ventas_Estimadas.xlsx`, etc. — ejemplos locales (gitignored o untracked)

---

## 4. Flujo de usuario

1. **Sidebar:** plantilla del carrito (viene de `data/carrito_template.xlsx` en repo; también se puede subir `.xlsx`/`.xlsm`).
2. **Sidebar (opcional):** mapeo `.csv`, planificación `.csv`.
3. **Página principal:** subir **Cajas terminadas** (`.xls`), **Mix ventas** (`.xls`), **Stock** (`.xlsx`).
4. Ajustar **% Stock de seguridad**, **Ajuste venta (%)**, semana de planificación (si hay plan).
5. Toggle **Replicar venta** (opcional): ignora stock y plan.
6. **Calcular Pedido** → tabla editable → métricas → **Descargar Carrito Excel**.

---

## 5. Reglas de negocio

### 5.1 Fuentes de ventas

- **Cajas terminadas** (`cargar_cajas_terminadas`): agrupa por `artdescrip` → `nombre_venta`, suma `ctecantidad` → `venta`, tipo `granel`.
- **Mix ventas** (`cargar_mixventas`): filtra `subgrupo == 1` y `bultos > 0`. Productos con patrón `(7,8` en el nombre se reclasifican a `granel` (nombre en MAYÚSCULAS).

Ventas finales = concat de ambos.

### 5.2 Stock

`cargar_stock` lee `.xlsx` **sin header en fila 0** (`skiprows=1`):

| Col Excel | Campo |
|-----------|-------|
| 0 | `grupo` |
| 1 | `codigo` |
| 4 | `descripcion` |
| 5 | `stock_seg` |
| 6 | `stock_real` |

### 5.3 Mapeo venta → código carrito

`generar_mapeo`:

1. Si existe `mapeo_productos.csv`, lo carga y solo agrega productos nuevos.
2. Overrides manuales en `_OVERRIDE_GRANEL` y `_OVERRIDE_EMPAQUETADO` (dentro de `pedido_logic.py`).
3. Fuzzy matching con `rapidfuzz` (`token_sort_ratio`, threshold **40**), restringido por grupo (`GRUPOS_GRANEL` vs `GRUPOS_EMPAQUETADO`).
4. Resultados: código asignado, `SIN MAPEO` (excluido del pedido) o `NO APLICA` (excluido).

**Corregir mapeos:** descargar CSV desde sidebar → editar → volver a subir.

### 5.4 Cálculo del pedido (`calcular_pedido`)

**Modo normal** (default):

```
venta_ajustada = venta × (1 + pct_ajuste_venta / 100)
pedido_calc    = max(0, ceil(venta_ajustada + stock_seg × pct_stock_seg/100 − stock_real))
```

**Ajuste por planificación** (si hay `plan_df` y columna semanal seleccionada):

```
lim_min = ceil(plan × 0.97)    # pct_plan_min = 3%
lim_max = floor(plan × 1.05)   # pct_plan_max = 5%
```

| Condición | Resultado | `ajuste_plan` |
|-----------|-----------|---------------|
| Sin entrada en plan | pedido = 0 | `missing` |
| plan = 0 | pedido = 0 | `zero` |
| pedido_calc < lim_min | pedido = lim_min | `up` |
| pedido_calc > lim_max | pedido = lim_max | `down` |
| Dentro de banda | pedido = pedido_calc | `""` |

**Modo Replicar venta** (`modo_replicar_venta=True`):

```
pedido = ceil(venta_ajustada)
```

Ignora stock real, stock de seguridad y planificación. Útil para reponer exactamente lo vendido.

**SKUs granel sin venta:** productos granel/empaquetado en stock pero sin movimiento en la semana aparecen con `venta=0` y `pedido=0` editable (granel por quiebres; empaquetados también listados).

### 5.5 Plantilla del carrito

Hoja activa del Excel del portal. Columnas usadas:

| Col | Campo |
|-----|-------|
| B | `Codigo` |
| F | `Cubicaje` |
| G | `Peso` (kg) |
| I | `precio` (sin IVA) |

`cargar_datos_plantilla` **debe usar `pd.read_excel`** (vectorizado). **No** iterar celda a celda con openpyxl (tarda ~27 s y congela Streamlit Cloud).

`escribir_carrito` escribe cantidades en **columna C** (fila 2+) del template y devuelve BytesIO para descarga.

### 5.6 Planificación semanal

CSV con columnas `Semana_<n>_<Mes>_<Año>` (ej. `Semana_18_Mayo_2026`). Semanas van **domingo → sábado**. Clave de join: `codigo_homologado` ↔ `codigo_carrito`.

### 5.7 Totales en UI

- Subtotal = Σ(pedido × precio_unit)
- Total con IVA = subtotal × **1.21**
- Cubicaje, kilos, cajas granel calculados con numpy desde el editor

---

## 6. Estado de Streamlit (`session_state`)

Claves importantes:

| Clave | Contenido |
|-------|-----------|
| `pedido_base` | DataFrame del pedido calculado + precios/cubicaje |
| `pedido_params` | `% stock`, `% venta`, semana plan, `modo_replicar_venta` |
| `mapeo_df` | Mapeo activo |
| `sin_mapeo_df` | Productos sin mapear |
| `plan_df` | Planificación cargada |
| `plantilla_bytes` | Copia en memoria si se subió plantilla por UI |
| `calc_version` | Incrementa al recalcular; invalida key del `data_editor` |
| `_plantilla_mtime` | Detecta cambio de plantilla en disco |
| `_plantilla_file_sig` / `_plan_file_sig` | `(nombre, tamaño)` para evitar re-procesar uploads |
| `auth_ok` | Gate de contraseña (ver §7 "Secrets"): `True` tras login correcto en la sesión |

**Invalidación de cache:** si cambia `mtime` de `data/carrito_template.xlsx`, se limpian `plantilla_bytes`, `pedido_base`, `mapeo_df`, etc.

---

## 7. Trampas y decisiones de implementación

### Streamlit Cloud

- **Filesystem efímero:** uploads guardados con `_guardar_plantilla` / `_guardar_planificacion` / `to_csv(MAPEO_PATH)` **no persisten** entre sesiones/reboots — el contenedor puede reiniciarse (sleep por inactividad, redeploy) y vuelve a lo que esté commiteado en el repo. Ambas funciones ahora devuelven `(contenido, guardado_en_disco)`; si `guardado_en_disco=False` la UI avisa que el cambio dura solo la sesión actual.
  - **Excepción — plantilla del carrito:** si el secret `GITHUB_TOKEN` está configurado, subir una plantilla nueva desde el sidebar además la commitea al repo automáticamente vía `actualizar_archivo_en_github` (API de contenidos de GitHub), así que sí persiste entre reinicios sin pasos manuales. Ver "Secrets" más abajo. `mapeo_productos.csv` y la planificación semanal **no** tienen este mecanismo (solo el flujo manual: descargar CSV, editar, volver a subir) — evaluar si agregarlo si la fricción es real (ver propuesta de mejora, tier 3).
  - Alternativa manual sin secret configurado: **commit** de `data/carrito_template.xlsx` + push + reboot app (procedimiento de §11).

### Secrets (Streamlit Cloud → Manage app → Settings → Secrets)

Todos son **opcionales** — sin configurarlos la app funciona igual que antes (login desactivado, plantilla subida solo persiste hasta el próximo reinicio).

| Secret | Efecto |
|--------|--------|
| `APP_PASSWORD` | Si está seteado, `_password_gate()` en `app_pedido.py` exige esa contraseña antes de mostrar la app (gate simple, un solo usuario — no hay sistema de cuentas). Sin este secret, no pide login (así queda el desarrollo local). |
| `GITHUB_TOKEN` | Token con permiso de escritura sobre el repo (`repo` classic o "Contents: Read and write" fine-grained). Si está, subir una plantilla nueva la commitea al repo automáticamente (ver arriba). Sin este secret, la plantilla subida por UI solo dura hasta el próximo reinicio del contenedor. |
| `GITHUB_REPO` | Override del repo destino para el commit automático. Por defecto `ayankilevich-cpu/pedido_helado` (`GITHUB_REPO_DEFAULT` en `pedido_logic.py`) — normalmente no hace falta tocarlo. |

### UI / reruns

- **No usar `st.rerun()`** inmediatamente después de `file_uploader` (plantilla, plan, mapeo). Provoca reruns encadenados y el botón «Calcular Pedido» no se procesa. Usar firma `(nombre, tamaño)` y procesar en el mismo run.
- **Scroll del editor:** `st.data_editor` hace rerun y la página vuelve al top. Fix: snippet JS con `sessionStorage` (`pedido_scroll_y`) inyectado vía `st_components.html` **antes** del editor (commit `04a6c7b`).
- **Ediciones del editor:** usar `_pedido_numpy_desde_editor` leyendo `edited_rows` de `session_state` — el DataFrame devuelto por `st.data_editor` a veces no refleja el último cambio.
- **`st.set_page_config` debe ser el primer comando de Streamlit del script.** `_password_gate()` se llama justo después (y hace `st.stop()` si no está autenticado) — no insertar nada de UI entre esas dos líneas.

### Errores y logging

- Los 3 loaders de ventas/stock (`cargar_cajas_terminadas`, `cargar_mixventas`, `cargar_stock`) validan columnas/forma antes de procesar y levantan `ArchivoInvalidoError` con un mensaje accionable (ej. "¿subiste el archivo correcto en ese cuadro?") en vez de dejar pasar un `KeyError` crudo. El bloque `if btn_calcular:` en `app_pedido.py` atrapa `ArchivoInvalidoError` → `st.error(str(e))`, y cualquier otra excepción → mensaje genérico + `logger.exception(...)`.
- `pedido_logic.py` y `app_pedido.py` usan `logging` (stdlib, no rompe la pureza de `pedido_logic.py`). Streamlit Cloud captura stdout/stderr en "Manage app → Logs". Los `except OSError` de guardado en disco ya no son silenciosos: loguean `logger.warning(...)`.

### Performance

- `cargar_datos_plantilla`: pandas + `usecols=[1,5,6,8]` (~60 ms). Commit `959479a`.
- Cacheada con `st.cache_data` vía `_cargar_datos_plantilla_ui` / `_cargar_datos_plantilla_disco` en `app_pedido.py`, keyed por `(ruta, mtime)` — se llama hasta 4 veces por sesión (sidebar, cálculo, refresco de resultados, backfill de peso) y sin cache releía el mismo Excel cada vez. La key con `mtime` invalida el cache solo cuando el archivo cambia en disco.

### Código

- `numpy` se usa en `app_pedido.py` y está declarado en `requirements.txt`.
- `pedido_logic.py` es puro (sin `import streamlit`). El mapeo se resuelve así: `cargar_mapeo_disco()` en `pedido_logic.py` lee solo de disco; `_cargar_mapeo_ui()` en `app_pedido.py` prioriza `session_state["mapeo_df"]` y si no hay, cae a `cargar_mapeo_disco()`. Mantener esta separación al tocar mapeo — es lo que permite testear `pedido_logic.py` sin contexto Streamlit (ago 2026, ver §12).
- `requests` es dependencia directa (persistencia vía GitHub) — declarada en `requirements.txt`, no solo transitiva de `streamlit`.

---

## 8. Contratos de datos (schemas)

### `mapeo_productos.csv`

```
nombre_venta, tipo, codigo_carrito, descripcion_carrito, score
```

### Plan CSV

```
codigo_homologado, descripcion, linea_producto, categoria, Semana_<n>_<Mes>_<Año>, ..., total_temporada
```

### Salida del cálculo (`pedido_df`)

```
codigo_carrito, descripcion, grupo, venta, stock_real, stock_seg,
plan_sem, pedido_calc, ajuste_plan, pedido_inicial, pedido
```

---

## 9. Mapa de funciones (dónde tocar qué)

| Tarea | Archivo | Función / zona |
|-------|---------|----------------|
| Cambiar fórmula de pedido | `pedido_logic.py` | `calcular_pedido` |
| Agregar override de mapeo | `pedido_logic.py` | `_OVERRIDE_GRANEL`, `_OVERRIDE_EMPAQUETADO` |
| Cambiar umbral fuzzy | `pedido_logic.py` | `generar_mapeo(score_threshold=...)` |
| Lógica de plan/semanas | `pedido_logic.py` | `cargar_planificacion`, `obtener_semanas`, `_ajustar` |
| Leer precios plantilla | `pedido_logic.py` | `cargar_datos_plantilla` |
| Validar Excel subido | `pedido_logic.py` | `validar_plantilla_carrito` |
| Export carrito | `pedido_logic.py` | `escribir_carrito` |
| UI / sliders / botones | `app_pedido.py` | bloques `with st.sidebar`, `btn_calcular` |
| Tabla editable / métricas | `app_pedido.py` | bloque `if "pedido_base" in st.session_state` |
| Scroll persistente | `app_pedido.py` | `st_components.html` antes de `st.data_editor` |
| Modo replicar venta | ambos | toggle en UI → param en `calcular_pedido` |
| Mensaje de error de un upload inválido | `pedido_logic.py` | `ArchivoInvalidoError` en `cargar_cajas_terminadas`/`cargar_mixventas`/`cargar_stock` |
| Persistencia de plantilla vía GitHub | `pedido_logic.py` / `app_pedido.py` | `actualizar_archivo_en_github` / bloque de subida en sidebar (usa secret `GITHUB_TOKEN`) |
| Password gate | `app_pedido.py` | `_password_gate()` (usa secret `APP_PASSWORD`) |
| Cache de la plantilla | `app_pedido.py` | `_cargar_datos_plantilla_ui` / `_cargar_datos_plantilla_disco` (`@st.cache_data`) |

---

## 10. Desarrollo local

```bash
cd "Clases/GRIDO/PEDIDOS"
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run app_pedido.py
```

Verificar sintaxis y tests antes de commit:

```bash
python3 -m py_compile app_pedido.py pedido_logic.py
pytest -q
```

`tests/` cubre `calcular_pedido` (modo normal, replicar venta, banda de plan,
SKUs sin venta, regresión del bug de descripciones cruzadas) y el parseo de
semanas (`_parsear_semana`, `inicio_semana`, `fin_semana`, `semana_default`).
Corre sin contexto Streamlit — `pedido_logic.py` no importa `streamlit`
(ver §7 "Código"). CI en `.github/workflows/ci.yml` corre `py_compile` +
`pytest` en cada push/PR (no bloquea el redeploy automático de Streamlit
Cloud, que dispara con cualquier push a `main` — el valor es detectar
roturas antes de mergear).

---

## 11. Deploy a producción

1. Confirmar remote: `git remote -v` → `pedido_helado`
2. Commit en `main`
3. `git push origin main`
4. Streamlit Cloud redeploya automáticamente
5. Si cambió **plantilla** o **dependencias**: Manage app → **Reboot app**
6. Verificar en sidebar: cantidad de productos (~410) y precios de muestra

### Actualizar plantilla del carrito (procedimiento estándar)

1. Descargar **Modelo de Carrito** del portal Grido (.xlsx, hoja `data` con datos).
2. Reemplazar `data/carrito_template.xlsx` en el repo.
3. Commit + push.
4. Reboot app en Streamlit Cloud.
5. Hard refresh navegador (`Cmd+Shift+R`).

**MD5 referencia plantilla (mayo 2026):** `e28230696deed8779a00080a717c1dee`

**Precios de verificación:**

| Código | Producto | Precio col. I |
|--------|----------|---------------|
| 4000036 | LIMÓN AL AGUA 7,8 KG | $ 13.672,81 |
| 4000043 | CHOCOLATE 7,8 KG | $ 25.787,04 |
| 4000050 | SUPER GRIDITO 7,8 KG | $ 25.787,04 |
| 4000057 | CHOCOLATE SUIZO 7,8 KG | $ 30.159,11 |
| 4000953 | SELECCIÓN ARGENTINA 7,8 KG | $ 30.159,11 |

---

## 12. Historial de cambios relevantes

| Fecha / commit | Cambio |
|----------------|--------|
| `959479a` | Performance: `cargar_datos_plantilla` con pandas |
| `620c012` / `74536a9` | Modo **Replicar venta** |
| `9b705ac` / `666dbe5` | SKUs granel sin venta en listado |
| `04a6c7b` | Fix scroll persistente en `data_editor` |
| Mayo 2026 | Plantilla nueva 410 productos; remote corregido a `pedido_helado` |
| Ago 2026 | Documentación `AGENTS.md` + Cursor rule |
| Ago 2026 | Fix bug de descripciones cruzadas en `calcular_pedido` (`zip` posicional con códigos duplicados en `mapeo_productos.csv`) |
| Ago 2026 | Desacople de `streamlit`: `cargar_mapeo()` → `cargar_mapeo_disco()` (puro) + `_cargar_mapeo_ui()` en `app_pedido.py` |
| Ago 2026 | `tests/` con pytest (`calcular_pedido`, parseo de semanas) + CI en `.github/workflows/ci.yml` |
| Ago 2026 | Cache de `cargar_datos_plantilla` con `st.cache_data` (key `(ruta, mtime)`) |
| Ago 2026 | Errores de upload accionables: `ArchivoInvalidoError` en los 3 loaders de ventas/stock + try/except en `btn_calcular` |
| Ago 2026 | Logging (`logging`, stdlib) en `pedido_logic.py`/`app_pedido.py`; los `except OSError` de guardado en disco dejan de ser silenciosos |
| Ago 2026 | Password gate opcional (`_password_gate()`, secret `APP_PASSWORD`) |
| Ago 2026 | Persistencia de la plantilla del carrito vía GitHub (`actualizar_archivo_en_github`, secret `GITHUB_TOKEN`): subir una plantilla nueva desde el sidebar ahora la commitea al repo automáticamente, así sobrevive a un reinicio del contenedor sin pasos manuales |

---

## 13. Qué NO hacer (checklist anti-errores)

- [ ] Push a `grido-pedido-semanal` pensando que actualiza prod
- [ ] `st.rerun()` tras file uploader de plantilla/plan/mapeo
- [ ] Reimplementar `cargar_datos_plantilla` con loop openpyxl celda a celda
- [ ] Asumir que uploads en Cloud persisten en disco
- [ ] Modificar lógica de negocio en `app_pedido.py` (pertenece a `pedido_logic.py`)
- [ ] Commitear `.xls` de ejemplo con datos reales (están en `.gitignore`)
- [ ] Olvidar `pytest -q` antes de push (o dejar que lo corra solo el CI)
- [ ] Olvidar `py_compile` antes de push
- [ ] Commitear un `GITHUB_TOKEN` o cualquier secret en el código (van solo en Streamlit Cloud → Secrets)
- [ ] Asumir que subir la plantilla persiste entre reinicios sin `GITHUB_TOKEN` configurado

---

## 14. Instrucción corta para el usuario → agente

Cuando el usuario diga *"trabajá en el pedido de Grido"* o similar, el agente debe:

1. Leer **este archivo** (`AGENTS.md`).
2. Confirmar que `origin` apunta a `pedido_helado`.
3. Identificar si el cambio es UI (`app_pedido.py`) o lógica (`pedido_logic.py`).
4. Respetar las trampas de Streamlit Cloud documentadas arriba.
5. No re-explorar el repo salvo que el cambio toque código no documentado aquí.

---

*Mantené este archivo actualizado cuando agregues features, cambies fórmulas o modifiques el flujo de deploy.*
