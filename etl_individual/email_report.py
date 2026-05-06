"""
Genera un Excel con toda la DB (manifiestos_flat) y lo envía por correo.

Variables de entorno requeridas:
    DATABASE_URL         — PostgreSQL connection string de Supabase
    REPORT_EMAIL_FROM    — Correo remitente (Gmail)
    REPORT_EMAIL_PASSWORD — Contraseña de aplicación de Gmail
    REPORT_EMAIL_TO      — Destinatarios separados por coma

Uso local:
    python -m etl_individual.email_report
"""

import io
import os
import smtplib
import sys
from datetime import date
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

_DIR = Path(__file__).parent.parent

QUERY = """
    SELECT
        manifiesto,
        mes,
        año,
        fecha_despacho,
        origen,
        departamento_origen,
        destino,
        departamento_destino,
        cliente,
        valor_remesa,
        flete_conductor,
        flete_neto_conductor,
        anticipo,
        placa,
        tipo_vehiculo,
        conductor,
        celular,
        cedula_conductor,
        propietario,
        agencia_despachadora,
        nombre_responsable,
        remesas,
        fecha_cumplido,
        CASE WHEN fecha_cumplido IS NOT NULL
             THEN CURRENT_DATE - fecha_cumplido
        END AS dias_cumplido,
        compromiso_pago,
        novedades,
        novedad_conductor,
        novedad_empresa,
        ajuste_positivo_flete,
        ajuste_negativo_flete,
        estado_interno,
        responsable_estado_interno,
        fecha_pago,
        valor_pagado,
        entidad_financiera,
        responsable,
        factura_no,
        fecha_factura,
        factura_electronica,
        mes_facturacion,
        dias_para_facturar,
        archivo_origen
    FROM public.manifiestos_flat
    ORDER BY fecha_despacho DESC, manifiesto DESC
"""

COLUMNAS = [
    "Manifiesto", "Mes", "Año", "Fecha Despacho",
    "Origen", "Dpto. Origen", "Destino", "Dpto. Destino",
    "Cliente", "Valor Remesa", "Flete Conductor", "Flete Neto Conductor", "Anticipo",
    "Placa", "Tipo Vehículo", "Conductor", "Celular",
    "Cédula Conductor", "Propietario", "Agencia", "Responsable Despacho",
    "Remesas", "Fecha Cumplido", "Días Cumplido", "Compromiso Pago",
    "Novedades", "Novedad Conductor", "Novedad Empresa",
    "Ajuste Positivo Flete", "Ajuste Negativo Flete",
    "Estado Interno", "Responsable Estado Int.",
    "Fecha Pago", "Valor Pagado", "Entidad Financiera", "Responsable Pago",
    "Factura No", "Fecha de Emisión de Factura", "Factura Electrónica",
    "Mes Facturación", "Días para Facturar", "Archivo Origen",
]


def build_excel(db_url: str) -> bytes:
    engine = create_engine(db_url, echo=False)
    print("Consultando DB...")
    with engine.connect() as conn:
        rows = conn.execute(text(QUERY)).fetchall()
    print(f"  {len(rows):,} manifiestos recuperados")

    df = pd.DataFrame(rows, columns=COLUMNAS)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Manifiestos")
        ws = writer.sheets["Manifiestos"]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)
    return buf.getvalue()


def send_email(excel_bytes: bytes, from_addr: str, password: str, to_addrs: list[str]):
    hoy = date.today()
    subject = f"Reporte mensual Altrans — {hoy.strftime('%B %Y').capitalize()}"
    filename = f"altrans_reporte_{hoy.strftime('%Y_%m')}.xlsx"

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    msg["Subject"] = subject

    body = (
        f"Estimados,\n\n"
        f"Adjunto encontrarán el reporte mensual de manifiestos Altrans "
        f"correspondiente a {hoy.strftime('%B %Y').capitalize()}.\n\n"
        f"El archivo contiene toda la información operativa, de seguimiento, "
        f"tesorería y facturación actualizada a la fecha de envío.\n\n"
        f"Este correo es generado automáticamente."
    )
    msg.attach(MIMEText(body, "plain", "utf-8"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(excel_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    msg.attach(part)

    print(f"Enviando a: {', '.join(to_addrs)}")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(from_addr, password)
        server.sendmail(from_addr, to_addrs, msg.as_string())
    print("  Correo enviado correctamente.")


def main():
    db_url   = os.environ.get("DATABASE_URL", "")
    from_addr = os.environ.get("REPORT_EMAIL_FROM", "")
    password  = os.environ.get("REPORT_EMAIL_PASSWORD", "")
    to_raw    = os.environ.get("REPORT_EMAIL_TO", "")

    missing = [k for k, v in {
        "DATABASE_URL": db_url,
        "REPORT_EMAIL_FROM": from_addr,
        "REPORT_EMAIL_PASSWORD": password,
        "REPORT_EMAIL_TO": to_raw,
    }.items() if not v]
    if missing:
        sys.exit(f"ERROR: Variables de entorno faltantes: {', '.join(missing)}")

    to_addrs = [e.strip() for e in to_raw.split(",") if e.strip()]

    excel_bytes = build_excel(db_url)
    send_email(excel_bytes, from_addr, password, to_addrs)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(_DIR / ".env", override=True)
    main()
