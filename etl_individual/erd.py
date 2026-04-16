"""
Genera un diagrama ERD de alta resolución para diapositivas.
Ejecutar: python3 etl_individual/erd.py
Salida:   informes/erd_altrans.png  (300 DPI, fondo blanco)
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "informes" / "erd_altrans.png"
OUT.parent.mkdir(parents=True, exist_ok=True)

# ── Paleta ────────────────────────────────────────────────────────────
C_HEADER_CATALOG = "#1a5276"   # azul oscuro — catálogos
C_HEADER_FACT    = "#784212"   # marrón — hechos
C_HEADER_MAIN    = "#1e8449"   # verde — tabla principal
C_BODY           = "#fdfefe"
C_BORDER_CAT     = "#2e86c1"
C_BORDER_FACT    = "#a04000"
C_BORDER_MAIN    = "#27ae60"
C_PK             = "#1a5276"
C_FK             = "#922b21"
C_ARROW          = "#555555"

# ── Definición de tablas: (x, y_top, ancho, nombre, campos) ──────────
#    campos: list of (texto, estilo)  estilo ∈ "pk" | "fk" | "uk" | ""

TABLES = {
    "propietarios": dict(
        x=0.02, y=0.95, w=0.14,
        header=C_HEADER_CATALOG, border=C_BORDER_CAT,
        fields=[
            ("id  PK", "pk"),
            ("tipo", ""),
            ("nombre  UK", ""),
            ("nit_cedula", ""),
        ],
    ),
    "conductores": dict(
        x=0.02, y=0.70, w=0.14,
        header=C_HEADER_CATALOG, border=C_BORDER_CAT,
        fields=[
            ("id  PK", "pk"),
            ("cedula  UK", ""),
            ("nombre", ""),
            ("celular", ""),
        ],
    ),
    "vehiculos": dict(
        x=0.02, y=0.47, w=0.14,
        header=C_HEADER_CATALOG, border=C_BORDER_CAT,
        fields=[
            ("placa  PK", "pk"),
            ("placa_remolque", ""),
            ("propietario_id  FK", "fk"),
        ],
    ),
    "clientes": dict(
        x=0.02, y=0.28, w=0.14,
        header=C_HEADER_CATALOG, border=C_BORDER_CAT,
        fields=[
            ("id  PK", "pk"),
            ("nombre  UK", ""),
        ],
    ),
    "lugares": dict(
        x=0.02, y=0.13, w=0.14,
        header=C_HEADER_CATALOG, border=C_BORDER_CAT,
        fields=[
            ("id  PK", "pk"),
            ("nombre  UK", ""),
            ("municipio", ""),
            ("departamento", ""),
        ],
    ),
    "agencias": dict(
        x=0.19, y=0.13, w=0.12,
        header=C_HEADER_CATALOG, border=C_BORDER_CAT,
        fields=[
            ("id  PK", "pk"),
            ("nombre  UK", ""),
        ],
    ),
    "responsables": dict(
        x=0.19, y=0.02, w=0.12,
        header=C_HEADER_CATALOG, border=C_BORDER_CAT,
        fields=[
            ("id  PK", "pk"),
            ("nombre  UK", ""),
        ],
    ),
    "manifiestos": dict(
        x=0.36, y=0.97, w=0.22,
        header=C_HEADER_MAIN, border=C_BORDER_MAIN,
        fields=[
            ("manifiesto  PK", "pk"),
            ("periodo / año / mes", ""),
            ("semana / consecutivo", ""),
            ("fecha_despacho", ""),
            ("conductor_id  FK", "fk"),
            ("placa  FK", "fk"),
            ("cliente_id  FK", "fk"),
            ("origen_id  FK", "fk"),
            ("destino_id  FK", "fk"),
            ("agencia_id  FK", "fk"),
            ("responsable_id  FK", "fk"),
            ("valor_remesa", ""),
            ("flete_conductor", ""),
            ("anticipo", ""),
            ("archivo_origen", ""),
        ],
    ),
    "remesas": dict(
        x=0.66, y=0.97, w=0.16,
        header=C_HEADER_FACT, border=C_BORDER_FACT,
        fields=[
            ("id  PK", "pk"),
            ("manifiesto_id  FK", "fk"),
            ("codigo_remesa", ""),
        ],
    ),
    "pagos_conductor": dict(
        x=0.66, y=0.72, w=0.16,
        header=C_HEADER_FACT, border=C_BORDER_FACT,
        fields=[
            ("id  PK", "pk"),
            ("manifiesto_id  UK+FK", "fk"),
            ("fecha_cumplido", ""),
            ("dias_cumplido", ""),
            ("estado", ""),
            ("condicion_pago", ""),
            ("novedades", ""),
            ("fecha_pago", ""),
            ("valor_pagado", ""),
            ("entidad_financiera", ""),
            ("responsable_id  FK", "fk"),
        ],
    ),
    "facturacion": dict(
        x=0.66, y=0.35, w=0.16,
        header=C_HEADER_FACT, border=C_BORDER_FACT,
        fields=[
            ("id  PK", "pk"),
            ("manifiesto_id  UK+FK", "fk"),
            ("factura_no", ""),
            ("fecha_factura", ""),
            ("factura_electronica", ""),
            ("dias_para_facturar", ""),
            ("mes_facturacion", ""),
            ("estado_interno", ""),
            ("responsable_id  FK", "fk"),
        ],
    ),
}

ROW_H   = 0.028   # altura de cada fila de campo
HEAD_H  = 0.038   # altura del encabezado
PAD     = 0.006   # padding interno

# ── Dibujo ────────────────────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(22, 17))
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
ax.axis("off")
fig.patch.set_facecolor("white")

box_meta = {}   # tabla → (x_left, x_right, y_top, y_bottom) en coords axes

def draw_table(ax, name, cfg):
    x, y_top, w = cfg["x"], cfg["y"], cfg["w"]
    fields = cfg["fields"]
    n = len(fields)
    total_h = HEAD_H + n * ROW_H + PAD

    # Sombra sutil
    shadow = mpatches.FancyBboxPatch(
        (x + 0.003, y_top - total_h - 0.003), w, total_h,
        boxstyle="round,pad=0.005", linewidth=0,
        facecolor="#cccccc", transform=ax.transAxes, zorder=1,
    )
    ax.add_patch(shadow)

    # Cuerpo
    body = mpatches.FancyBboxPatch(
        (x, y_top - total_h), w, total_h,
        boxstyle="round,pad=0.005", linewidth=1.5,
        edgecolor=cfg["border"], facecolor=C_BODY,
        transform=ax.transAxes, zorder=2,
    )
    ax.add_patch(body)

    # Encabezado
    header = mpatches.FancyBboxPatch(
        (x, y_top - HEAD_H), w, HEAD_H,
        boxstyle="round,pad=0.005", linewidth=0,
        facecolor=cfg["header"], transform=ax.transAxes, zorder=3,
    )
    ax.add_patch(header)

    ax.text(x + w / 2, y_top - HEAD_H / 2, name,
            ha="center", va="center", fontsize=8.5, fontweight="bold",
            color="white", transform=ax.transAxes, zorder=4)

    # Campos
    for i, (label, style) in enumerate(fields):
        fy = y_top - HEAD_H - (i + 0.5) * ROW_H
        color = C_PK if style == "pk" else (C_FK if style == "fk" else "#222222")
        weight = "bold" if style in ("pk", "fk") else "normal"
        fs = 6.8
        ax.text(x + PAD * 2, fy, label,
                ha="left", va="center", fontsize=fs,
                fontweight=weight, color=color,
                transform=ax.transAxes, zorder=4)

        # Separador entre filas (excepto última)
        if i < n - 1:
            ax.plot([x, x + w], [y_top - HEAD_H - (i + 1) * ROW_H,
                                  y_top - HEAD_H - (i + 1) * ROW_H],
                    color="#e0e0e0", lw=0.4, transform=ax.transAxes, zorder=3)

    box_meta[name] = (x, x + w, y_top, y_top - total_h)


for name, cfg in TABLES.items():
    draw_table(ax, name, cfg)


# ── Flechas FK ───────────────────────────────────────────────────────

def mid_right(name):
    x0, x1, yt, yb = box_meta[name]
    return (x1, (yt + yb) / 2)

def mid_left(name):
    x0, x1, yt, yb = box_meta[name]
    return (x0, (yt + yb) / 2)

def bot_center(name):
    x0, x1, yt, yb = box_meta[name]
    return ((x0 + x1) / 2, yb)

def top_center(name):
    x0, x1, yt, yb = box_meta[name]
    return ((x0 + x1) / 2, yt)

ARROW_KW = dict(
    arrowstyle="-|>", color=C_ARROW, lw=1.2,
    mutation_scale=10, transform=ax.transAxes, zorder=5,
)

def arrow(src, dst):
    ax.annotate("", xy=dst, xytext=src,
                arrowprops=dict(arrowstyle="-|>", color=C_ARROW,
                                lw=1.1, mutation_scale=9),
                xycoords="axes fraction", textcoords="axes fraction", zorder=5)

# Catálogos → manifiestos
arrow(mid_right("conductores"),  mid_left("manifiestos"))
arrow(mid_right("vehiculos"),    (box_meta["manifiestos"][0], box_meta["manifiestos"][2] - HEAD_H - 4 * ROW_H))
arrow(mid_right("clientes"),     (box_meta["manifiestos"][0], box_meta["manifiestos"][2] - HEAD_H - 6 * ROW_H))
arrow(mid_right("lugares"),      (box_meta["manifiestos"][0], box_meta["manifiestos"][2] - HEAD_H - 7 * ROW_H))
arrow(mid_right("agencias"),     (box_meta["manifiestos"][0], box_meta["manifiestos"][2] - HEAD_H - 9 * ROW_H))
arrow(mid_right("responsables"), (box_meta["manifiestos"][0], box_meta["manifiestos"][2] - HEAD_H - 10 * ROW_H))

# propietarios → vehiculos
arrow(mid_right("propietarios"), mid_left("vehiculos"))

# manifiestos → hechos
arrow(mid_right("manifiestos"), mid_left("remesas"))
arrow(mid_right("manifiestos"), mid_left("pagos_conductor"))
arrow(mid_right("manifiestos"), mid_left("facturacion"))

# responsables → pagos_conductor y facturacion
arrow((box_meta["responsables"][1], box_meta["responsables"][2]),
      (box_meta["pagos_conductor"][0],
       box_meta["pagos_conductor"][3] + ROW_H * 0.5))

arrow((box_meta["responsables"][1], box_meta["responsables"][2]),
      (box_meta["facturacion"][0],
       box_meta["facturacion"][3] + ROW_H * 0.5))

# ── Leyenda ───────────────────────────────────────────────────────────

legend_x, legend_y = 0.84, 0.18
ax.text(legend_x, legend_y, "Leyenda", fontsize=8, fontweight="bold",
        transform=ax.transAxes, va="top")
items = [
    (C_HEADER_MAIN,    "Tabla principal"),
    (C_HEADER_FACT,    "Tablas de hechos"),
    (C_HEADER_CATALOG, "Catálogos"),
    (C_PK,             "PK  Clave primaria"),
    (C_FK,             "FK  Clave foránea"),
]
for i, (color, label) in enumerate(items):
    yy = legend_y - 0.035 - i * 0.03
    rect = mpatches.Rectangle((legend_x, yy - 0.01), 0.015, 0.018,
                               facecolor=color, transform=ax.transAxes, zorder=5)
    ax.add_patch(rect)
    ax.text(legend_x + 0.022, yy, label, fontsize=7.2,
            transform=ax.transAxes, va="center")

# ── Título ────────────────────────────────────────────────────────────

ax.text(0.5, 1.01, "Altrans S.A.S — Modelo Entidad–Relación",
        ha="center", va="bottom", fontsize=14, fontweight="bold",
        transform=ax.transAxes)
ax.text(0.5, 0.99, "Base de datos operativa · módulo individual",
        ha="center", va="top", fontsize=9, color="#555555",
        transform=ax.transAxes)

plt.tight_layout(pad=0.3)
plt.savefig(OUT, dpi=300, bbox_inches="tight", facecolor="white")
print(f"ERD guardado → {OUT}")
