from sqlalchemy import (
    BigInteger, CheckConstraint, Column, Date, DateTime, Enum, ForeignKey,
    Integer, Numeric, SmallInteger, String, Text,
    UniqueConstraint, func,
)
from sqlalchemy.orm import DeclarativeBase

# ── Enums canónicos ───────────────────────────────────────────────────
# Úsalos para poblar menús desplegables en formularios de ingreso.

tipo_propietario_enum = Enum(
    "PERSONA", "EMPRESA",
    name="tipo_propietario",
)

mes_enum = Enum(
    "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE",
    name="mes_calendario",
)

estado_pago_enum = Enum(
    "PAGO A 15 DIAS", "PAGO A 20 DIAS", "PAGO A 30 DIAS", "PAGO A 5-8 DIAS",
    "CONTRAENTREGA", "PRONTO PAGO", "PAGO NORMAL", "PAGO INMEDIATO",
    "URBANO", "PAGADO", "ANULADO", "PRIORITARIO", "RNDC", "OTROS",
    name="estado_pago",
)

condicion_pago_enum = Enum(
    "PAGO NORMAL", "CONTRAENTREGA", "PRONTO PAGO", "CONTINGENCIA 20-25 DH",
    name="condicion_pago",
)

entidad_financiera_enum = Enum(
    "TRANSF BANCOLOMBIA", "TRANSF DAVIVIENDA",
    "CHEQUE BANCOLOMBIA", "CHEQUE DAVIVIENDA",
    "TRANSF BANCO DE BOGOTA", "CHEQUE BANCO DE BOGOTA",
    "CHEQUE", "TRANSF/CHEQUE", "ANULADO", "OTRO",
    name="entidad_financiera",
)

estado_interno_enum = Enum(
    "CUMPLIDO", "NO SE HA CUMPLIDO", "PENDIENTE FACTURA ELECTRONICA",
    "FACTURA RECIBIDA", "NOVEDAD PENDIENTE", "ANULADO",
    name="estado_interno",
)


class Base(DeclarativeBase):
    pass


# ── Catálogos ─────────────────────────────────────────────────────────

class Propietario(Base):
    __tablename__ = "propietarios"

    id         = Column(Integer, primary_key=True)
    tipo       = Column(tipo_propietario_enum, nullable=False)
    nombre     = Column(String(200), nullable=False, unique=True)
    nit_cedula = Column(String(20))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Conductor(Base):
    __tablename__ = "conductores"
    __table_args__ = (
        CheckConstraint(r"cedula IS NULL OR cedula ~ '^\d+$'", name="chk_cedula_solo_digitos"),
        # CheckConstraint(r"celular ~ '^[0-9]{10}$'", name="chk_celular_formato"),  # pendiente confirmar números ecuatorianos
    )

    id         = Column(Integer, primary_key=True)
    cedula     = Column(String(20), unique=True)       # NULL permitido (múltiples conductores sin cédula)
    nombre     = Column(String(200), nullable=False)
    celular    = Column(String(20))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Vehiculo(Base):
    __tablename__ = "vehiculos"

    placa          = Column(String(10), primary_key=True)
    placa_remolque = Column(String(10))
    propietario_id = Column(Integer, ForeignKey("propietarios.id"))
    created_at     = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Cliente(Base):
    __tablename__ = "clientes"

    id         = Column(Integer, primary_key=True)
    nombre     = Column(String(300), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Lugar(Base):
    __tablename__ = "lugares"

    id           = Column(Integer, primary_key=True)
    nombre       = Column(String(200), nullable=False, unique=True)
    municipio    = Column(String(100))
    departamento = Column(String(50))
    created_at   = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Agencia(Base):
    __tablename__ = "agencias"
    __table_args__ = (
        CheckConstraint(
            "nombre IN ('CALI', 'IPIALES', 'BOGOTA', 'BUENAVENTURA', 'ANULADO')",
            name="chk_agencia_nombre_valido",
        ),
    )

    id         = Column(Integer, primary_key=True)
    nombre     = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Responsable(Base):
    __tablename__ = "responsables"

    id         = Column(Integer, primary_key=True)
    nombre     = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ── Tablas de hechos ──────────────────────────────────────────────────

class Manifiesto(Base):
    __tablename__ = "manifiestos"
    __table_args__ = (
        CheckConstraint("manifiesto BETWEEN 10000 AND 9999999", name="chk_manifiesto_digitos"),
        CheckConstraint("año BETWEEN 2020 AND 2030",            name="chk_año_rango"),
        CheckConstraint("fecha_despacho >= '2023-01-01'",       name="chk_fecha_despacho_desde_2023"),
        CheckConstraint("valor_remesa >= 0",                    name="chk_valor_remesa_no_negativo"),
        CheckConstraint("flete_conductor >= 0",                 name="chk_flete_conductor_no_negativo"),
        CheckConstraint("anticipo >= 0",                        name="chk_anticipo_no_negativo"),
        # CheckConstraint("anticipo <= flete_conductor", name="chk_anticipo_lte_flete"),  # pendiente confirmar
    )

    manifiesto          = Column(BigInteger, primary_key=True)
    periodo             = Column(Date)
    año                 = Column(SmallInteger)
    mes                 = Column(mes_enum)
    consecutivo_semanal = Column(Integer)
    fecha_despacho      = Column(Date, nullable=False)
    conductor_id        = Column(Integer, ForeignKey("conductores.id"))
    placa               = Column(String(10), ForeignKey("vehiculos.placa"))
    cliente_id          = Column(Integer, ForeignKey("clientes.id"))
    origen_id           = Column(Integer, ForeignKey("lugares.id"),   nullable=False)
    destino_id          = Column(Integer, ForeignKey("lugares.id"),   nullable=False)
    agencia_id          = Column(Integer, ForeignKey("agencias.id"),  nullable=False)
    responsable_id      = Column(Integer, ForeignKey("responsables.id"))
    valor_remesa        = Column(Numeric(14, 2))
    flete_conductor     = Column(Numeric(14, 2), nullable=False)
    anticipo            = Column(Numeric(14, 2))
    created_at          = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Remesa(Base):
    __tablename__ = "remesas"
    __table_args__ = (
        UniqueConstraint("manifiesto_id", "codigo_remesa"),
        CheckConstraint("codigo_remesa IS NULL OR codigo_remesa ~ '^[0-9]{5,6}$'", name="chk_codigo_remesa_formato"),
    )

    id            = Column(Integer, primary_key=True)
    manifiesto_id = Column(BigInteger, ForeignKey("manifiestos.manifiesto"), nullable=False)
    codigo_remesa = Column(String(50))
    created_at    = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class PagoConductor(Base):
    """
    Un pago por manifiesto (UNIQUE en manifiesto_id).
    Si la empresa confirma que puede haber pagos parciales,
    cambiar unique=True por index=True en manifiesto_id.
    """
    __tablename__ = "pagos_conductor"
    __table_args__ = (
        CheckConstraint("valor_pagado >= 0", name="chk_valor_pagado_no_negativo"),
        # PENDIENTE CONFIRMAR CON EMPRESA
        # CheckConstraint("fecha_pago BETWEEN '2020-01-01' AND '2030-12-31'",     name="chk_fecha_pago_rango"),
        # CheckConstraint("fecha_cumplido BETWEEN '2020-01-01' AND '2030-12-31'", name="chk_fecha_cumplido_rango"),
    )

    # dias_cumplido se omite como columna persistida; calcular en consultas como:
    #   CURRENT_DATE - fecha_cumplido AS dias_cumplido
    # o crear la vista:
    #   CREATE OR REPLACE VIEW v_pagos_conductor AS
    #     SELECT *, CURRENT_DATE - fecha_cumplido AS dias_cumplido
    #     FROM pagos_conductor;

    id                 = Column(Integer, primary_key=True)
    manifiesto_id      = Column(BigInteger, ForeignKey("manifiestos.manifiesto"), nullable=False, unique=True)
    fecha_cumplido     = Column(Date)
    estado             = Column(estado_pago_enum)
    condicion_pago     = Column(condicion_pago_enum)
    novedades          = Column(Text)
    fecha_pago         = Column(Date)
    valor_pagado       = Column(Numeric(14, 2))
    entidad_financiera = Column(entidad_financiera_enum)
    responsable_id     = Column(Integer, ForeignKey("responsables.id"))
    created_at         = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Facturacion(Base):
    """
    Una factura por manifiesto (UNIQUE en manifiesto_id).
    Si puede haber refacturaciones, cambiar unique=True por index=True.
    """
    __tablename__ = "facturacion"
    __table_args__ = (
        CheckConstraint("mes_facturacion BETWEEN 1 AND 12", name="chk_mes_facturacion_rango"),
        # CheckConstraint("fecha_factura BETWEEN '2020-01-01' AND '2030-12-31'", name="chk_fecha_factura_rango"),
    )

    # dias_para_facturar no se almacena: calculado en v_facturacion como
    #   (fecha_factura - manifiestos.fecha_despacho)

    id                  = Column(Integer, primary_key=True)
    manifiesto_id       = Column(BigInteger, ForeignKey("manifiestos.manifiesto"), nullable=False, unique=True)
    factura_no          = Column(String(50))
    fecha_factura       = Column(Date)
    factura_electronica = Column(String(200))
    mes_facturacion     = Column(SmallInteger)
    estado_interno      = Column(estado_interno_enum)
    responsable_id      = Column(Integer, ForeignKey("responsables.id"))
    created_at          = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
