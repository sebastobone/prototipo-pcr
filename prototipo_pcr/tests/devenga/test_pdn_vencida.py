from datetime import date
import polars as pl
from src import devenga, prep_insumo
from tests.devenga import conftest as cf


def test_pdn_vencida_valorada_mes_del_recibo(
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


def test_pdn_vencida_valorada_despues_del_recibo(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion vencida en meses posteriores a la
    entrada del recibo. No se deberian presentar estos casos, pero en caso
    de que lo hagan, no deberia haber ningun movimiento.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2026, 2, 28),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2026, 1, 31),
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

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="entra_devengado",
        dias_devengados=cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
        ),
        dias_no_devengados=0,
        constitucion=0,
        liberacion=0,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_vencida_50_50_vigencia_entre_mes_niif17(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 1, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 1, 9),
        fecha_inicio_vigencia_recibo=date(2025, 1, 16),
        fecha_fin_vigencia_recibo=date(2025, 1, 22),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 16),
        fecha_fin_vigencia_cobertura=date(2025, 1, 22),
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

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="finalizado",
        dias_devengados=7,
        dias_no_devengados=0,
        constitucion=-prima,
        liberacion=prima,
        liberacion_acum=prima,
        saldo=0,
        regla_devengo="diario"
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado, test_5050=True)

def test_pdn_vencida_50_50_diario_vigencia_entre_mes_niif4(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 1, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 1, 9),
        fecha_inicio_vigencia_recibo=date(2025, 1, 16),
        fecha_fin_vigencia_recibo=date(2025, 1, 22),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 16),
        fecha_fin_vigencia_cobertura=date(2025, 1, 22),
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
        dias_devengados=7,
        dias_no_devengados=0,
        constitucion=-prima,
        liberacion=prima,
        liberacion_acum=prima,
        saldo=0,
        regla_devengo='mensual_devengo_diario'
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado, test_5050=True)

def test_pdn_vencida_50_50_finaliza_y_constituye_mismo_mes(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 1, 31),
        fecha_expedicion_poliza=date(2025, 1, 11),
        fecha_contabilizacion_recibo=date(2025, 1, 21),
        fecha_inicio_vigencia_recibo=date(2025, 1, 11),
        fecha_fin_vigencia_recibo=date(2025, 2, 5),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 11),
        fecha_fin_vigencia_cobertura=date(2025, 2, 5),
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

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_vencida_50_50_enero_marzo(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 3, 31),
        fecha_expedicion_poliza=date(2025, 1, 31),
        fecha_contabilizacion_recibo=date(2025, 3, 22),
        fecha_inicio_vigencia_recibo=date(2025, 1, 31),
        fecha_fin_vigencia_recibo=date(2025, 3, 1),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 31),
        fecha_fin_vigencia_cobertura=date(2025, 3, 1),
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
        estado_devengo="entra_devengado",
        dias_devengados=30,
        dias_no_devengados=0,
        constitucion=-prima,
        liberacion=prima,
        liberacion_acum=prima,
        saldo=0,
        regla_devengo="mensual_devengo_diario"
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado, test_5050=True)


def test_pdn_vencida_50_50_enero_marzo_2(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 4, 30),
        fecha_expedicion_poliza=date(2025, 1, 31),
        fecha_contabilizacion_recibo=date(2025, 3, 22),
        fecha_inicio_vigencia_recibo=date(2025, 1, 31),
        fecha_fin_vigencia_recibo=date(2025, 3, 1),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 31),
        fecha_fin_vigencia_cobertura=date(2025, 3, 1),
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
        estado_devengo="entra_devengado",
        dias_devengados=30,
        dias_no_devengados=0,
        constitucion=0,
        liberacion=0,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_vencida_50_50_entra_devengado(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 1, 31),
        fecha_expedicion_poliza=date(2024, 9, 2),
        fecha_contabilizacion_recibo=date(2025, 1, 14),
        fecha_inicio_vigencia_recibo=date(2024, 9, 2),
        fecha_fin_vigencia_recibo=date(2024, 9, 6),
        fecha_inicio_vigencia_cobertura=date(2024, 9, 2),
        fecha_fin_vigencia_cobertura=date(2024, 9, 6),
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
        estado_devengo="entra_devengado",
        dias_devengados=5,
        dias_no_devengados=0,
        constitucion=-prima,
        liberacion=prima,
        liberacion_acum=prima,
        saldo=0,
        regla_devengo="mensual_devengo_diario"
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado, test_5050=True)

def test_pdn_vencida_50_50_entra_devengado_valoracion_posterior(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 2, 28),
        fecha_expedicion_poliza=date(2024, 9, 2),
        fecha_contabilizacion_recibo=date(2025, 1, 14),
        fecha_inicio_vigencia_recibo=date(2024, 9, 2),
        fecha_fin_vigencia_recibo=date(2024, 9, 6),
        fecha_inicio_vigencia_cobertura=date(2024, 9, 2),
        fecha_fin_vigencia_cobertura=date(2024, 9, 6),
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
        estado_devengo="entra_devengado",
        dias_devengados=5,
        dias_no_devengados=0,
        constitucion=0,
        liberacion=0,
        liberacion_acum=prima,
        saldo=0,
        regla_devengo="mensual_devengo_diario"
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado, test_5050=True)