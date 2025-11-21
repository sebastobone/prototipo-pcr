from datetime import date
import polars as pl
from src import devenga, prep_insumo
from tests.devenga import conftest as cf


def test_pdn(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2026, 1, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2026, 1, 31),
        fecha_inicio_vigencia_recibo=date(2025, 1, 1),
        fecha_fin_vigencia_recibo=date(2025, 12, 31),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 1),
        fecha_fin_vigencia_cobertura=date(2025, 12, 31),
    )
    prima = 1200

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion)
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="entra_devengado",
        dias_devengados=cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
        ),
        dias_no_devengados=0,
        constitucion=-prima,
        liberacion=prima,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)
