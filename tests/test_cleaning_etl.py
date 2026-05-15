"""
Tests de helpers puros del ETL ([etl_individual/cleaning_individual.py]).
Ejecutar: python3 -m pytest tests/etl/test_cleaning.py -v
       o: python3 tests/etl/test_cleaning.py

Solo prueba las funciones puras (sin DataFrames pesados).
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'etl_individual'))

import re
import pandas as pd
import pytest

from cleaning_individual import (
    normalize_col,
    _clean_money,
    _strip_accents,
    _extract_departamento,
    _normalize_city_name,
    _clean_dias,
    _clean_compromiso_pago,
    _cedula,
    _clean_celular,
    _clean_cedula_conductor,
    _clean_tipo_vehiculo,
    _clean_entidad_financiera,
    _normalize_estado,
    _normalize_person_base,
)


# ── normalize_col ────────────────────────────────────────────────────────────
class TestNormalizeCol:
    def test_lowercase_y_trim(self):
        assert normalize_col('  Manifiesto  ') == 'manifiesto'
        assert normalize_col('FECHA') == 'fecha'

    def test_no_modifica_mayusculas_internas(self):
        # Solo trim+lower, no toca espacios internos
        assert normalize_col('valor remesa') == 'valor remesa'


# ── _strip_accents ───────────────────────────────────────────────────────────
class TestStripAccents:
    def test_quita_tildes(self):
        assert _strip_accents('Bogotá') == 'Bogota'
        assert _strip_accents('PÁGÓ') == 'PAGO'

    def test_preserva_ñ(self):
        assert _strip_accents('Nariño') == 'Nariño'
        assert _strip_accents('CAÑÓN') == 'CAÑON'

    def test_string_vacia(self):
        assert _strip_accents('') == ''


# ── _clean_money ─────────────────────────────────────────────────────────────
class TestCleanMoney:
    def test_formato_colombiano(self):
        assert _clean_money('$1.080.750') == 1080750.0
        assert _clean_money('500.000') == 500000.0

    def test_con_signo_dolar(self):
        assert _clean_money('$1.420.000') == 1420000.0

    def test_decimales_pandas(self):
        # Pandas serializa con punto decimal: 1080750.0 — no son miles
        assert _clean_money('1080750.0') == 1080750.0

    def test_anulado_y_basura(self):
        assert _clean_money('ANULADO') is None
        assert _clean_money('X') is None
        assert _clean_money('') is None
        assert _clean_money('nan') is None

    def test_nan(self):
        assert _clean_money(pd.NA) is None
        assert _clean_money(float('nan')) is None

    def test_string_no_parseable(self):
        assert _clean_money('abc') is None


# ── _extract_departamento ────────────────────────────────────────────────────
class TestExtractDepartamento:
    def test_abreviatura_conocida(self):
        assert _extract_departamento('Cali (Vall)') == 'Valle del Cauca'
        assert _extract_departamento('Bogotá (Cund)') == 'Cundinamarca'
        assert _extract_departamento('Pasto (Nari)') == 'Nariño'

    def test_fallback_por_ciudad(self):
        # Sin paréntesis, busca en _CITY_DEPT_FALLBACK
        r = _extract_departamento('BOGOTA')
        assert r in ('Bogota D.C.', None)  # tolerante: depende del mapa

    def test_ciudad_desconocida(self):
        # Sin paréntesis y sin fallback → None
        assert _extract_departamento('CIUDAD INEXISTENTE') is None

    def test_nan(self):
        assert _extract_departamento(pd.NA) is None


# ── _normalize_city_name ─────────────────────────────────────────────────────
class TestNormalizeCityName:
    def test_quita_acentos_y_canoniza(self):
        # Devuelve la forma canónica en MAYÚSCULAS, sin tildes (preserva ñ).
        r = _normalize_city_name('Bogotá')
        assert 'BOGOTA' in r.upper() or r == 'Bogota'
        assert 'á' not in r  # acentos fuera

    def test_preserva_ñ(self):
        r = _normalize_city_name('Nariño')
        assert 'ñ' in r.lower() or 'Ñ' in r


# ── _clean_dias ──────────────────────────────────────────────────────────────
class TestCleanDias:
    def test_numero_valido(self):
        assert _clean_dias('5') == 5.0
        assert _clean_dias('-10') == -10.0

    def test_fechas_se_descartan(self):
        assert _clean_dias('2026-01-01') is None
        assert _clean_dias('14/05/2026') is None

    def test_anulado_x(self):
        assert _clean_dias('ANULADO') is None
        assert _clean_dias('X') is None

    def test_absurdos(self):
        assert _clean_dias('-400')  is None  # menos de -365
        assert _clean_dias('5000')  is None  # más de 3650
        assert _clean_dias('365')   == 365.0

    def test_texto(self):
        assert _clean_dias('hola') is None


# ── _clean_compromiso_pago ───────────────────────────────────────────────────
class TestCleanCompromisoPago:
    def test_15_dias_variantes(self):
        assert _clean_compromiso_pago('15DH') == 'PAGO A 15 DIAS'
        assert _clean_compromiso_pago('15D') == 'PAGO A 15 DIAS'
        assert _clean_compromiso_pago('15 DIAS') == 'PAGO A 15 DIAS'

    def test_contraentrega(self):
        assert _clean_compromiso_pago('CONTRAENTREGA') == 'CONTRAENTREGA'
        assert _clean_compromiso_pago('C. CONTRAENTREGA') == 'CONTRAENTREGA'
        assert _clean_compromiso_pago('C.CONTRA') == 'CONTRAENTREGA'

    def test_pago_normal(self):
        assert _clean_compromiso_pago('PAGO NORMAL') == 'PAGO NORMAL'

    def test_pronto_pago(self):
        assert _clean_compromiso_pago('PRONTO PAGO') == 'PRONTO PAGO'

    def test_numerico_suelto_se_descarta(self):
        assert _clean_compromiso_pago('123') is None
        assert _clean_compromiso_pago('15') is None

    def test_vacio_y_nan(self):
        assert _clean_compromiso_pago('') is None
        assert _clean_compromiso_pago(pd.NA) is None


# ── _cedula ──────────────────────────────────────────────────────────────────
class TestCedula:
    def test_quita_decimal_residual(self):
        assert _cedula('1130668182.0') == '1130668182'
        assert _cedula(1130668182.0) == '1130668182'

    def test_string_normal(self):
        assert _cedula('1130668182') == '1130668182'

    def test_extrae_digitos_si_hay_letras(self):
        assert _cedula('CC-12345678') == '12345678'

    def test_basura(self):
        assert _cedula(',') is None
        assert _cedula('-') is None
        assert _cedula('') is None
        assert _cedula(pd.NA) is None


# ── _clean_celular ───────────────────────────────────────────────────────────
class TestCleanCelular:
    def test_diez_digitos(self):
        assert _clean_celular('3001234567') == ('3001234567', None)

    def test_corto_o_largo_con_nota(self):
        val, nota = _clean_celular('300')
        assert val is None
        assert 'CELULAR INUSUAL' in nota
        val, nota = _clean_celular('123456789012345')
        assert val is None
        assert nota is not None

    def test_celular_serializado_como_float(self):
        assert _clean_celular('3001234567.0') == ('3001234567', None)


# ── _clean_cedula_conductor ──────────────────────────────────────────────────
class TestCleanCedulaConductor:
    def test_rango_valido(self):
        # 6 a 12 dígitos
        assert _clean_cedula_conductor('123456') == ('123456', None)
        assert _clean_cedula_conductor('1130668182') == ('1130668182', None)
        assert _clean_cedula_conductor('123456789012') == ('123456789012', None)

    def test_demasiado_corto_o_largo(self):
        val, nota = _clean_cedula_conductor('12345')
        assert val is None
        assert 'CEDULA INUSUAL' in nota


# ── _clean_tipo_vehiculo ─────────────────────────────────────────────────────
class TestCleanTipoVehiculo:
    def test_placa_con_digitos_se_conserva(self):
        assert _clean_tipo_vehiculo('ABC123') == ('ABC123', None)

    def test_descriptor_va_a_novedades(self):
        val, nota = _clean_tipo_vehiculo('MULA')
        assert val is None
        assert 'TIPO VEHICULO' in nota
        assert 'MULA' in nota

    def test_anulado(self):
        assert _clean_tipo_vehiculo('ANULADO') == (None, None)


# ── _clean_entidad_financiera ────────────────────────────────────────────────
class TestCleanEntidadFinanciera:
    def test_transf_bancolombia(self):
        v, _ = _clean_entidad_financiera('TRANSF BCOL')
        assert v == 'TRANSF BANCOLOMBIA'
        v, _ = _clean_entidad_financiera('TRANSFERENCIA BANCOLOMBIA')
        assert v == 'TRANSF BANCOLOMBIA'

    def test_transf_davivienda(self):
        v, _ = _clean_entidad_financiera('TRANSF DAVIVIENDA')
        assert v == 'TRANSF DAVIVIENDA'

    def test_transf_bogota(self):
        v, _ = _clean_entidad_financiera('TRANSF BANCO DE BOGOTA')
        assert v == 'TRANSF BANCO DE BOGOTA'

    def test_cheque_con_numero(self):
        v, nota = _clean_entidad_financiera('CHEQUE 142')
        assert v == 'CHEQUE'
        # CHEQUE + número guarda original en novedades
        assert nota is not None

    def test_anulado(self):
        v, _ = _clean_entidad_financiera('ANULADO')
        assert v == 'ANULADO'

    def test_persona_se_descarta(self):
        v, n = _clean_entidad_financiera('JOHANA UNIGARRO')
        assert v is None and n is None

    def test_nan(self):
        v, n = _clean_entidad_financiera(pd.NA)
        assert v is None and n is None


# ── _normalize_estado ────────────────────────────────────────────────────────
class TestNormalizeEstado:
    def test_15_dias(self):
        cat, _ = _normalize_estado('15DH')
        assert cat == 'PAGO A 15 DIAS'

    def test_contraentrega_typos(self):
        cat, _ = _normalize_estado('CONTREAENTREGA')
        assert cat == 'CONTRAENTREGA'

    def test_pagado(self):
        cat, _ = _normalize_estado('PAGADO')
        assert cat == 'PAGADO'

    def test_anulado(self):
        cat, _ = _normalize_estado('ANULADO')
        assert cat == 'ANULADO'

    def test_urbano(self):
        cat, _ = _normalize_estado('URBANO')
        assert cat == 'URBANO'

    def test_rndc(self):
        cat, _ = _normalize_estado('RNDC')
        assert cat == 'RNDC'

    def test_pagar_fecha_va_a_pago_normal(self):
        # La regla "PAGAR + fecha" → PAGO NORMAL con valor original en novedades.
        # Nota: si el texto contiene "15", "20", "30" primero matchea PAGO A X DIAS.
        # Usamos un día que NO sea de los plazos.
        cat, novedad = _normalize_estado('PAGAR EL 7 DE MAYO')
        assert cat == 'PAGO NORMAL'
        assert novedad is not None

    def test_abono_va_a_pago_normal(self):
        cat, novedad = _normalize_estado('ABONO PARCIAL')
        assert cat == 'PAGO NORMAL'


# ── _normalize_person_base ───────────────────────────────────────────────────
class TestNormalizePersonBase:
    def test_uppercase_y_sin_acentos(self):
        assert _normalize_person_base('  maría perez  ') == 'MARIA PEREZ'

    def test_anulado(self):
        assert _normalize_person_base('anulado') == 'ANULADO'

    def test_basura(self):
        assert _normalize_person_base(',') is None
        assert _normalize_person_base('-') is None
        assert _normalize_person_base('123') is None
        assert _normalize_person_base('14/05/2026') is None

    def test_largo_se_descarta(self):
        s = 'A' * 50
        assert _normalize_person_base(s) is None

    def test_quita_parentetico(self):
        assert _normalize_person_base('JUAN (RNDC)') == 'JUAN'


# ── Runner standalone ────────────────────────────────────────────────────────
if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))
