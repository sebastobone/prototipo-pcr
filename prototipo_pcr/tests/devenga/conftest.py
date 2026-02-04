from dataclasses import dataclass
from datetime import date
from typing import Optional, List
import polars as pl
import pytest


@dataclass
class Fechas:
    fecha_valoracion: date
    fecha_cancelacion_poliza: Optional[date] = None
    fecha_expedicion_poliza: Optional[date] = None
    fecha_contabilizacion_recibo: Optional[date] = None
    fecha_inicio_vigencia_recibo: Optional[date] = None
    fecha_fin_vigencia_recibo: Optional[date] = None
    fecha_inicio_vigencia_cobertura: Optional[date] = None
    fecha_fin_vigencia_cobertura: Optional[date] = None
    fecha_inicio_vigencia_poliza: Optional[date] = None
    fecha_fin_vigencia_poliza: Optional[date] = None

@dataclass
class ResultadosDevengo:
    estado_devengo: Optional[str] = None
    dias_devengados: Optional[int] = None
    dias_no_devengados: Optional[int] = None
    constitucion: Optional[float] = None
    liberacion: Optional[float] = None
    liberacion_acum: Optional[float] = None
    saldo: Optional[float] = None
    fecha_liberacion: Optional[date] = None

@dataclass
class registro_prueba_componente_inversion:
    fecha_contabilizacion_recibo: date
    tipo_op: str
    valor_prima: float
    resultado_esperado: ResultadosDevengo


def crear_input_devengo(
    fechas: Fechas, tipo_insumo: str, tipo_negocio: str, prima: float, moneda: str = "COP"
) -> pl.DataFrame:
    """
    Crea un dataframe de entrada para la funcion de devengo.
    Busca replicar el insumo consumido desde el datalake.
    """
    df = pl.DataFrame(
        {
            "tipo_insumo": [tipo_insumo],
            "tipo_negocio": [tipo_negocio],
            "fecha_expedicion_poliza": fechas.fecha_expedicion_poliza,
            "fecha_contabilizacion_recibo": fechas.fecha_contabilizacion_recibo,
            "fecha_inicio_vigencia_recibo": fechas.fecha_inicio_vigencia_recibo,
            "fecha_fin_vigencia_recibo": fechas.fecha_fin_vigencia_recibo,
            "fecha_inicio_vigencia_cobertura": fechas.fecha_inicio_vigencia_cobertura,
            "fecha_fin_vigencia_cobertura": fechas.fecha_fin_vigencia_cobertura,
            "fecha_valoracion": fechas.fecha_valoracion,
            "moneda": moneda,
            "valor_prima_emitida": [prima],
        }
    )
    df = df.with_columns(
        [
            pl.col(col).cast(pl.Date)
            for col in df.collect_schema().names()
            if "fecha" in col
        ]
    )
    return df

def crear_input_componente_inversion(
    fechas: Fechas, tipo_insumo: str, tipo_negocio: str, moneda: str, 
    poliza: str, registros_prueba: List[registro_prueba_componente_inversion]
) -> pl.DataFrame:
    """
    Crea un dataframe de entrada para componente de inversión con múltiples registros.
    Específica para pruebas de componente_inversion con múltiples casos.
    """
    n_registros = len(registros_prueba)
    
    df = pl.DataFrame(
        {
            "tipo_insumo": [tipo_insumo] * n_registros,
            "tipo_negocio": [tipo_negocio] * n_registros,
            "poliza": [poliza] * n_registros,
            "fecha_cancelacion_poliza": [fechas.fecha_cancelacion_poliza] * n_registros,
            "fecha_expedicion_poliza": [fechas.fecha_expedicion_poliza] * n_registros,
            "compania": ['02'] * n_registros,
            "ramo_sura": ['081'] * n_registros,
            "tipo_op": [r.tipo_op for r in registros_prueba],
            "moneda": [moneda] * n_registros,
            "fecha_contabilizacion_recibo": [r.fecha_contabilizacion_recibo for r in registros_prueba],
            "fecha_inicio_vigencia_poliza": [fechas.fecha_inicio_vigencia_poliza] * n_registros,
            "fecha_fin_vigencia_poliza": [fechas.fecha_fin_vigencia_poliza] * n_registros,
            "valor_prima_emitida": [r.valor_prima for r in registros_prueba],
            "fecha_valoracion": [fechas.fecha_valoracion] * n_registros,
        }
    )
    
    df = df.with_columns(
        [
            pl.col(col).cast(pl.Date)
            for col in df.collect_schema().names()
            if "fecha" in col
        ]
    )
    
    return df

def diferencia_fechas_dias(
    fecha_fin: date, fecha_inicio: date, incluir_extremos: bool = True
) -> int:
    return (fecha_fin - fecha_inicio).days + int(incluir_extremos)


def extraer_resultado_devengo(
        resultado: pl.DataFrame
) -> ResultadosDevengo:
    return ResultadosDevengo(
        estado_devengo=resultado.get_column("estado_devengo").item(0),
        dias_devengados=resultado.get_column("dias_devengados").item(0),
        dias_no_devengados=resultado.get_column("dias_no_devengados").item(0),
        constitucion=resultado.get_column("valor_constitucion").item(0),
        liberacion=resultado.get_column("valor_liberacion").item(0),
        liberacion_acum=resultado.get_column("valor_liberacion_acum").item(0),
        saldo=resultado.get_column("saldo").item(0),
    )

def extraer_resultado_componente_inversion(
        resultado: pl.DataFrame
) -> ResultadosDevengo:
    return ResultadosDevengo(
        constitucion=resultado.get_column("valor_constitucion").item(0),
        liberacion=resultado.get_column("valor_liberacion").item(0),
        liberacion_acum=resultado.get_column("valor_liberacion_acum").item(0),
        saldo=resultado.get_column("saldo").item(0),
        fecha_liberacion=resultado.get_column("fecha_fin_devengo").item(0)
    )

def validar_resultado_devengo(
    resultado_devengo: ResultadosDevengo, resultado_esperado: ResultadosDevengo
):
    assert (
        resultado_devengo.estado_devengo == resultado_esperado.estado_devengo
    ), "Estado devengo incorrecto"
    assert (
        resultado_devengo.dias_devengados == resultado_esperado.dias_devengados
    ), "Dias devengados incorrectos"
    assert (
        resultado_devengo.dias_no_devengados == resultado_esperado.dias_no_devengados
    ), "Dias no devengados incorrectos"
    assert resultado_devengo.constitucion == pytest.approx(
        resultado_esperado.constitucion
    ), "Constitucion incorrecta"
    assert resultado_devengo.liberacion == pytest.approx(
        resultado_esperado.liberacion
    ), "Liberacion incorrecta"
    assert resultado_devengo.liberacion_acum == pytest.approx(
        resultado_esperado.liberacion_acum
    ), "Liberacion acumulada incorrecta"
    assert resultado_devengo.saldo == pytest.approx(
        resultado_esperado.saldo
    ), "Saldo incorrecto"

def validar_resultado_componente_inversion(
    resultado_devengo: ResultadosDevengo, resultado_esperado: ResultadosDevengo
):
    assert resultado_devengo.constitucion == pytest.approx(
            resultado_esperado.constitucion
        ), "Constitucion incorrecta"
    assert resultado_devengo.liberacion == pytest.approx(
        resultado_esperado.liberacion
    ), "Liberacion incorrecta"
    assert resultado_devengo.liberacion_acum == pytest.approx(
        resultado_esperado.liberacion_acum
    ), "Liberacion acumulada incorrecta"
    assert resultado_devengo.saldo == pytest.approx(
        resultado_esperado.saldo
    ), "Saldo incorrecto"
    assert (
        resultado_devengo.fecha_liberacion == resultado_esperado.fecha_liberacion
    ), "Fecha Liberación"

def validar_resultados_multiples(
    resultado_df: pl.DataFrame, registros_prueba: list
):
    """Valida múltiples registros"""
    for registro in registros_prueba:

        resultado_filtrado = resultado_df.filter(
            pl.col("fecha_contabilizacion_recibo") == registro.fecha_contabilizacion_recibo
        ).head(1)

        resultado_devengo = extraer_resultado_componente_inversion(resultado_filtrado)

        try:
            validar_resultado_componente_inversion(resultado_devengo, registro.resultado_esperado)
        except AssertionError as e:
            raise AssertionError(
                f"Fallo en fecha_contab={registro.fecha_contabilizacion_recibo}, tipo_op={registro.tipo_op}: {str(e)}"
            )