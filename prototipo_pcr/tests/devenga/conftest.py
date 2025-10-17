from dataclasses import dataclass
from datetime import date
import polars as pl
from src import parametros as p
import pytest


@dataclass
class Fechas:
    fecha_valoracion: date
    fecha_expedicion_poliza: date
    fecha_contabilizacion_recibo: date
    fecha_inicio_vigencia_recibo: date
    fecha_fin_vigencia_recibo: date
    fecha_inicio_vigencia_cobertura: date
    fecha_fin_vigencia_cobertura: date


@dataclass
class ResultadosDevengo:
    estado_devengo: str
    dias_devengados: int
    dias_no_devengados: int
    constitucion: float
    liberacion: float
    liberacion_acum: float
    saldo: float


def crear_input_devengo(
    fechas: Fechas, tipo_insumo: str, tipo_negocio: str, prima: float
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


def diferencia_fechas_dias(
    fecha_fin: date, fecha_inicio: date, incluir_extremos: bool = True
) -> int:
    return (fecha_fin - fecha_inicio).days + int(incluir_extremos)


def extraer_resultado_devengo(resultado: pl.DataFrame) -> ResultadosDevengo:
    return ResultadosDevengo(
        estado_devengo=resultado.get_column("estado_devengo").item(0),
        dias_devengados=resultado.get_column("dias_devengados").item(0),
        dias_no_devengados=resultado.get_column("dias_no_devengados").item(0),
        constitucion=resultado.get_column("valor_constitucion").item(0),
        liberacion=resultado.get_column("valor_liberacion").item(0),
        liberacion_acum=resultado.get_column("valor_liberacion_acum").item(0),
        saldo=resultado.get_column("saldo").item(0),
    )


def validar_resultado_devengo(
    resultado_devengo: ResultadosDevengo, resultado_esperado: ResultadosDevengo
):
    assert resultado_devengo.estado_devengo == resultado_esperado.estado_devengo, (
        "Estado devengo incorrecto"
    )
    assert resultado_devengo.dias_devengados == resultado_esperado.dias_devengados, (
        "Dias devengados incorrectos"
    )
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
    assert resultado_devengo.saldo == pytest.approx(resultado_esperado.saldo), (
        "Saldo incorrecto"
    )


@pytest.fixture
def param_contabilidad() -> pl.DataFrame:
    return pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_PARAMETROS_CONTAB).filter(
        pl.col("estado_insumo") == 1
    )


@pytest.fixture
def excepciones_df() -> pl.DataFrame:
    return pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_EXCEPCIONES_50_50)
