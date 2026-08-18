# Pedido Semanal Grido

App Streamlit para calcular el pedido semanal de reposición de helado (granel + empaquetado), con precios, cubicaje, peso y exportación al Modelo de Carrito del portal Grido.

**Última actualización:** 18 de agosto de 2026

---

## Documentación para agentes de IA

> **Si sos un agente de IA:** leé **[AGENTS.md](./AGENTS.md)** completo antes de modificar código o hacer deploy. Ahí está todo el contexto del proyecto (arquitectura, reglas de negocio, trampas de Streamlit Cloud, mapa de funciones y runbook).

Para usuarios humanos, este README es un resumen; el detalle operativo está en `AGENTS.md`.

---

## App en producción

| Concepto | Valor |
|----------|--------|
| **URL** | https://pedido-helado.streamlit.app |
| **Repositorio** | https://github.com/ayankilevich-cpu/pedido_helado |
| **Rama** | `main` |

> **Importante:** Streamlit Cloud despliega desde **`pedido_helado`**. El repo `grido-pedido-semanal` es obsoleto y no actualiza la app online.

---

## Estructura del proyecto

```
PEDIDOS/
├── AGENTS.md                  # ← Fuente de verdad para agentes de IA
├── README.md                  # Este archivo (resumen humano)
├── app_pedido.py              # UI Streamlit
├── pedido_logic.py            # Carga, mapeo, cálculo, plantilla, carrito Excel
├── mapeo_productos.csv        # Nombre venta → código carrito
├── requirements.txt
├── data/
│   ├── carrito_template.xlsx  # Modelo de Carrito del portal (410 productos)
│   └── compras_semanales_actual.csv  # Plan semanal (opcional)
├── .streamlit/config.toml
└── .cursor/rules/pedido-helado.mdc  # Regla Cursor (apunta a AGENTS.md)
```

---

## Uso rápido

### Local

```bash
cd Clases/GRIDO/PEDIDOS
pip install -r requirements.txt
streamlit run app_pedido.py
```

### Producción (Streamlit Cloud)

1. Subir **Cajas terminadas** (.xls), **Mix ventas** (.xls), **Stock** (.xlsx).
2. Opcional: planificación (.csv), mapeo corregido (.csv) en sidebar.
3. Ajustar % stock de seguridad, ajuste de venta, semana de plan.
4. Opcional: activar **Replicar venta** (ignora stock y plan).
5. **Calcular Pedido** → editar cantidades → **Descargar Carrito Excel**.

---

## Funcionalidades

- Cálculo de pedido por producto (venta ajustada, stock real, stock de seguridad %).
- Planificación semanal opcional con banda **97%–105%**.
- Modo **Replicar venta** para reponer exactamente lo vendido.
- SKUs granel sin venta aparecen con pedido 0 editable.
- Precios, cubicaje y peso desde plantilla; totales con IVA 21%.
- Tabla editable con scroll persistente; descarga Excel carrito.
- Mapeo fuzzy + overrides manuales; validación de plantilla al subir.

---

## Actualizar plantilla del carrito

Streamlit Cloud **no persiste** uploads entre sesiones. Procedimiento fiable:

1. Descargar Modelo de Carrito del portal (.xlsx).
2. Reemplazar `data/carrito_template.xlsx` en el repo.
3. `git add` → commit → `git push origin main`.
4. Reboot app en Streamlit Cloud + hard refresh del navegador.

Ver precios de verificación y MD5 en [AGENTS.md §11](./AGENTS.md#11-deploy-a-producción).

---

## Desarrollo y deploy

```bash
git remote -v   # debe ser ayankilevich-cpu/pedido_helado.git
python3 -m py_compile app_pedido.py pedido_logic.py
git push origin main
```

Detalle completo en [AGENTS.md §10–11](./AGENTS.md).
