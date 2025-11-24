from datetime import date
import polars as pl
from src import devenga, prep_insumo
from tests.devenga import conftest as cf


def test_pdn_valoracion_antes_contabilizacion(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2024, 12, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 5, 31),
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
    assert df_resultado.is_empty(), "No debería procesar este registro"

def test_pdn_valoracion_en_vigencia_antes_contabilizacion(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 3, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 5, 31),
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
    assert df_resultado.is_empty(), "No debería procesar este registro"

def test_pdn_valoracion_en_vigencia_mismo_mes_contabilizacion(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 5, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 5, 10),
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
    dias_devengados = cf.diferencia_fechas_dias(
            fechas.fecha_valoracion,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_vigencia = cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_no_devengados = dias_vigencia - dias_devengados

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=dias_devengados,
        dias_no_devengados=dias_no_devengados,
        constitucion=-prima,
        liberacion=dias_devengados*prima/dias_vigencia,
        liberacion_acum=dias_devengados*prima/dias_vigencia,
        saldo=-dias_no_devengados*prima/dias_vigencia,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_valoracion_en_vigencia_posteriora_contabilizacion(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 6, 30),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 5, 10),
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
    dias_devengados = cf.diferencia_fechas_dias(
            fechas.fecha_valoracion,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_vigencia = cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_no_devengados = dias_vigencia - dias_devengados

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=dias_devengados,
        dias_no_devengados=dias_no_devengados,
        constitucion=0,
        liberacion=30*prima/dias_vigencia,
        liberacion_acum=dias_devengados*prima/dias_vigencia,
        saldo=-dias_no_devengados*prima/dias_vigencia,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_valoracion_en_mes_fin_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 12, 15),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 5, 10),
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
    dias_devengados = cf.diferencia_fechas_dias(
            fechas.fecha_valoracion,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_vigencia = cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_no_devengados = dias_vigencia - dias_devengados

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=dias_devengados,
        dias_no_devengados=dias_no_devengados,
        constitucion=0,
        liberacion=15*prima/dias_vigencia,
        liberacion_acum=dias_devengados*prima/dias_vigencia,
        saldo=-dias_no_devengados*prima/dias_vigencia,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_valoracion_dia_finalizacion(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 12, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 5, 10),
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
    dias_devengados = cf.diferencia_fechas_dias(
            fechas.fecha_valoracion,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_vigencia = cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_no_devengados = dias_vigencia - dias_devengados

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="finalizado",
        dias_devengados=dias_devengados,
        dias_no_devengados=dias_no_devengados,
        constitucion=0,
        liberacion=31*prima/dias_vigencia,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_finalizado(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2026, 12, 15),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 5, 10),
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
    dias_vigencia = cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True
        )
    dias_devengados = min(
        cf.diferencia_fechas_dias(
            fechas.fecha_valoracion,
            fechas.fecha_inicio_vigencia_cobertura,
            incluir_extremos=True)
            , dias_vigencia
        )
    dias_no_devengados = dias_vigencia - dias_devengados

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="finalizado",
        dias_devengados=dias_devengados,
        dias_no_devengados=dias_no_devengados,
        constitucion=0,
        liberacion=0,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)