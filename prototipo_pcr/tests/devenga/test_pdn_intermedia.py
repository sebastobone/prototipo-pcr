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

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
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

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
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

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
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

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
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

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
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

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
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

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
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

def test_pdn_50_50_contabilizado_mes_inicio_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2024, 10, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 10, 4),
        fecha_inicio_vigencia_recibo=date(2024, 10, 8),
        fecha_fin_vigencia_recibo=date(2024, 11, 8),
        fecha_inicio_vigencia_cobertura=date(2024, 10, 8),
        fecha_fin_vigencia_cobertura=date(2024, 11, 8),
    )
    prima = 1200

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4_local")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=-prima,
        liberacion=prima/2,
        liberacion_acum=prima/2,
        saldo=-prima/2,
        regla_devengo="mensual_devengo_50_50"
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado, test_5050=True)

def test_pdn_50_50_contabilizado_mes_fin_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2024, 11, 30),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 11, 4),
        fecha_inicio_vigencia_recibo=date(2024, 10, 10),
        fecha_fin_vigencia_recibo=date(2024, 11, 10),
        fecha_inicio_vigencia_cobertura=date(2024, 10, 10),
        fecha_fin_vigencia_cobertura=date(2024, 10, 10),
    )
    prima = 1200

    df = (
        cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima)
        .pipe(
            prep_insumo.prep_input_prima_directo,
            param_contabilidad,
            excepciones_df,
            fechas.fecha_valoracion,
        )
        .with_columns(
            pl.lit(0).alias("aplica_comp_financ"),
            pl.lit(None).cast(pl.Float64).alias("acreditacion_intereses"),
        )
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4_local")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="finalizado",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=-prima,
        liberacion=prima,
        liberacion_acum=prima,
        saldo=0,
        regla_devengo="mensual_devengo_50_50"
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado, test_5050=True)