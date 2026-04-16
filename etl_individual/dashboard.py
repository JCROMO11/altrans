"""
Dashboard Altrans — Reunión
Ejecutar: streamlit run etl_individual/dashboard.py
"""

import os
import textwrap
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
DB_URL = os.environ["DATABASE_URL"]

st.set_page_config(
    page_title="Altrans — Dashboard",
    page_icon="🚛",
    layout="wide",
)

# ── Conexión ──────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


# ── Datos base ────────────────────────────────────────────────────────

df_main = query("""
    SELECT
        m.manifiesto,
        m.periodo,
        m.año,
        m.mes,
        m.semana,
        m.fecha_despacho,
        m.flete_conductor,
        m.anticipo,
        m.valor_remesa,
        m.archivo_origen,
        c.nombre                          AS cliente,
        lo.nombre                         AS origen,
        ld.nombre                         AS destino,
        lo.departamento                   AS dpto_origen,
        ld.departamento                   AS dpto_destino,
        cond.nombre                       AS conductor,
        p.estado                          AS estado_pago,
        p.condicion_pago,
        p.valor_pagado,
        p.fecha_pago,
        p.dias_cumplido,
        f.factura_no,
        f.estado_interno,
        f.dias_para_facturar,
        f.fecha_factura
    FROM manifiestos m
    LEFT JOIN clientes   c    ON c.id    = m.cliente_id
    LEFT JOIN lugares    lo   ON lo.id   = m.origen_id
    LEFT JOIN lugares    ld   ON ld.id   = m.destino_id
    LEFT JOIN conductores cond ON cond.id = m.conductor_id
    LEFT JOIN pagos_conductor p ON p.manifiesto_id = m.manifiesto
    LEFT JOIN facturacion     f ON f.manifiesto_id = m.manifiesto
""")

df_main["periodo"] = pd.to_datetime(df_main["periodo"])
df_main["mes_label"] = df_main["periodo"].dt.strftime("%b %Y")

# ── Sidebar — Filtros ─────────────────────────────────────────────────

st.sidebar.title("Filtros")

años = sorted(df_main["año"].dropna().unique().astype(int).tolist())
año_sel = st.sidebar.multiselect("Año", años, default=años)

meses_orden = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
               "JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
meses_disp = [m for m in meses_orden if m in df_main["mes"].unique()]
mes_sel = st.sidebar.multiselect("Mes", meses_disp, default=meses_disp)

clientes_disp = sorted(df_main["cliente"].dropna().unique().tolist())
cliente_sel = st.sidebar.multiselect("Cliente", clientes_disp, default=[])

df = df_main[df_main["año"].isin(año_sel) & df_main["mes"].isin(mes_sel)]
if cliente_sel:
    df = df[df["cliente"].isin(cliente_sel)]

# ── Header ────────────────────────────────────────────────────────────

st.title("🚛 Altrans S.A.S — Dashboard Operativo")
st.caption(f"Datos: {df['periodo'].min().strftime('%b %Y')} → {df['periodo'].max().strftime('%b %Y')}  |  {len(df):,} manifiestos seleccionados")

st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────

k1, k2, k3, k4, k5 = st.columns(5)

total_manifiestos = len(df)
# Flete acordado: lo que se pactó pagar al conductor por cada viaje
flete_acordado    = df["flete_conductor"].sum()
# Pagado a conductores: lo registrado en pagos_conductor.valor_pagado
total_pagado      = df["valor_pagado"].sum()
dias_facturar_med = df["dias_para_facturar"].median()
sin_factura       = df["factura_no"].isna().sum()

def cop_m(val):
    """Formatea valor en millones COP."""
    return f"${val/1e6:,.0f} M"

k1.metric("Manifiestos",              f"{total_manifiestos:,}")
k2.metric("Flete acordado (conductores)", cop_m(flete_acordado),
          help="Suma del flete pactado con cada conductor por viaje")
k3.metric("Pagado a conductores",     cop_m(total_pagado),
          help="Suma de valor_pagado registrado. Puede estar incompleto si falta el dato en el Excel original.")
k4.metric("Mediana días para facturar", f"{dias_facturar_med:.0f} días",
          help="Días entre fecha de despacho y emisión de factura")
k5.metric("Sin número de factura",    f"{sin_factura:,}",
          delta=f"{sin_factura/total_manifiestos*100:.1f}% del total",
          delta_color="inverse",
          help="Manifiestos que no tienen factura_no registrada")

st.divider()

# ── Fila 1: Volumen y flete mensual ───────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    st.subheader("Manifiestos por mes")
    vol = (
        df.groupby("mes_label")
        .size().reset_index(name="n")
        .assign(periodo=lambda x: pd.to_datetime(x["mes_label"], format="%b %Y"))
        .sort_values("periodo")
    )
    fig = px.bar(vol, x="mes_label", y="n", text="n",
                 color_discrete_sequence=["#1f77b4"])
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_title="", yaxis_title="Manifiestos",
                      showlegend=False, height=350)
    st.plotly_chart(fig, width="stretch")

with col2:
    st.subheader("Flete acordado por mes ($ millones COP)")
    flete_mes = (
        df.groupby("mes_label")["flete_conductor"]
        .sum().div(1e6).reset_index()
        .rename(columns={"flete_conductor": "flete_M"})
        .assign(periodo=lambda x: pd.to_datetime(x["mes_label"], format="%b %Y"))
        .sort_values("periodo")
    )
    fig2 = px.line(flete_mes, x="mes_label", y="flete_M",
                   markers=True, color_discrete_sequence=["#ff7f0e"])
    fig2.update_layout(xaxis_title="", yaxis_title="$ Millones COP", height=350)
    fig2.update_traces(line_width=2.5)
    st.plotly_chart(fig2, width="stretch")

# ── Fila 2: Top clientes y rutas ──────────────────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.subheader("Top 10 clientes por flete acordado")
    st.caption("Clientes cuyas cargas generaron más flete para conductores")
    top_cli = (
        df.groupby("cliente")["flete_conductor"]
        .sum().div(1e6).reset_index()
        .rename(columns={"flete_conductor": "flete_M"})
        .sort_values("flete_M", ascending=False)
        .head(10)
        .assign(cliente=lambda x: x["cliente"].str[:30])
    )
    fig3 = px.bar(top_cli, x="flete_M", y="cliente",
                  orientation="h",
                  text=top_cli["flete_M"].apply(lambda v: f"${v:,.0f}M"),
                  color_discrete_sequence=["#2ca02c"])
    fig3.update_traces(textposition="outside")
    fig3.update_layout(yaxis=dict(autorange="reversed"),
                       xaxis_title="$ Millones COP", yaxis_title="",
                       height=380)
    st.plotly_chart(fig3, width="stretch")

with col4:
    st.subheader("Top 10 rutas por volumen")
    st.caption("Departamento origen → Departamento destino")
    # Excluir rutas con origen o destino desconocido
    df_rutas = df.dropna(subset=["dpto_origen", "dpto_destino"]).copy()
    df_rutas["ruta"] = df_rutas["dpto_origen"] + " → " + df_rutas["dpto_destino"]
    top_rutas = (
        df_rutas.groupby("ruta").size().reset_index(name="n")
        .sort_values("n", ascending=False).head(10)
    )
    fig4 = px.bar(top_rutas, x="n", y="ruta", orientation="h",
                  text_auto=True, color_discrete_sequence=["#9467bd"])
    fig4.update_layout(yaxis=dict(autorange="reversed"),
                       xaxis_title="Manifiestos", yaxis_title="",
                       height=380)
    st.plotly_chart(fig4, width="stretch")

# ── Fila 3: Estados de pago y días para facturar ──────────────────────

col5, col6 = st.columns(2)

with col5:
    st.subheader("Condición de pago acordada")
    st.caption("Tipo de pago pactado con el conductor")
    estados = (
        df["estado_pago"].fillna("SIN REGISTRAR")
        .value_counts().reset_index()
    )
    fig5 = px.pie(estados, names="estado_pago", values="count",
                  hole=0.4, color_discrete_sequence=px.colors.qualitative.Set2)
    fig5.update_traces(textposition="inside", textinfo="percent+label")
    fig5.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig5, width="stretch")

with col6:
    st.subheader("Días entre despacho y factura")
    st.caption("Distribución de días_para_facturar (0–120 días)")
    dias = df["dias_para_facturar"].dropna()
    dias = dias[(dias >= 0) & (dias <= 120)]
    fig6 = px.histogram(dias, nbins=40, color_discrete_sequence=["#d62728"])
    fig6.update_layout(xaxis_title="Días", yaxis_title="Manifiestos",
                       showlegend=False, height=380)
    fig6.add_vline(x=dias.median(), line_dash="dash", line_color="black",
                   annotation_text=f"Mediana: {dias.median():.0f} días",
                   annotation_position="top right")
    st.plotly_chart(fig6, width="stretch")

# ── Fila 4: Top conductores y estado interno ──────────────────────────

col7, col8 = st.columns(2)

with col7:
    st.subheader("Top 10 conductores por flete acordado")
    st.caption("Total de flete pactado acumulado por conductor ($ millones COP)")
    top_cond = (
        df.groupby("conductor")["flete_conductor"]
        .sum().div(1e6).reset_index()
        .rename(columns={"flete_conductor": "flete_M"})
        .sort_values("flete_M", ascending=False)
        .head(10)
        .assign(conductor=lambda x: x["conductor"].str.title().str[:25])
    )
    fig7 = px.bar(top_cond, x="flete_M", y="conductor",
                  orientation="h",
                  text=top_cond["flete_M"].apply(lambda v: f"${v:,.0f}M"),
                  color_discrete_sequence=["#8c564b"])
    fig7.update_traces(textposition="outside")
    fig7.update_layout(yaxis=dict(autorange="reversed"),
                       xaxis_title="$ Millones COP", yaxis_title="",
                       height=380)
    st.plotly_chart(fig7, width="stretch")

with col8:
    st.subheader("Estado interno de manifiestos")
    st.caption("Estado operativo según seguimiento interno")
    ei = (
        df["estado_interno"].fillna("SIN REGISTRAR")
        .value_counts().reset_index()
    )
    fig8 = px.bar(ei, x="count", y="estado_interno", orientation="h",
                  text_auto=True, color_discrete_sequence=["#e377c2"])
    fig8.update_layout(yaxis=dict(autorange="reversed"),
                       xaxis_title="Manifiestos", yaxis_title="",
                       height=380)
    st.plotly_chart(fig8, width="stretch")

st.divider()

# ── Tabla detalle ─────────────────────────────────────────────────────

with st.expander("Ver detalle de manifiestos"):
    cols_show = ["manifiesto", "periodo", "cliente", "origen", "destino",
                 "conductor", "flete_conductor", "valor_pagado",
                 "estado_pago", "factura_no", "dias_para_facturar", "estado_interno"]
    st.dataframe(
        df[cols_show].sort_values("periodo", ascending=False),
        width="stretch",
        height=400,
    )
