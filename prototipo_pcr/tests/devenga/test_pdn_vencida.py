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
        constitucion=0,
        liberacion=0,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_vencida_50_50_vigencia_entre_mes_niif17(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
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

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion)
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="finalizado",
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

def test_pdn_vencida_50_50_vigencia_entre_mes_niif4(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
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

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=-prima,
        liberacion=prima/2,
        liberacion_acum=prima/2,
        saldo=-prima/2,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_vencida_50_50_finalizado_y_constituido_mismo_mes(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 1, 31),
        fecha_expedicion_poliza=date(2025, 1, 11),
        fecha_contabilizacion_recibo=date(2025, 1, 21),
        fecha_inicio_vigencia_recibo=date(2025, 1, 11),
        fecha_fin_vigencia_recibo=date(2025, 1, 20),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 11),
        fecha_fin_vigencia_cobertura=date(2025, 1, 20),
    )
    prima = 1200

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=-prima,
        liberacion=prima/2,
        liberacion_acum=prima/2,
        saldo=-prima/2,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_vencida_50_50_enero_marzo(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 3, 31),
        fecha_expedicion_poliza=date(2025, 1, 31),
        fecha_contabilizacion_recibo=date(2025, 3, 22),
        fecha_inicio_vigencia_recibo=date(2025, 1, 31),
        fecha_fin_vigencia_recibo=date(2025, 3, 3),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 31),
        fecha_fin_vigencia_cobertura=date(2025, 3, 3),
    )
    prima = 1200

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=-prima,
        liberacion=prima/2,
        liberacion_acum=prima/2,
        saldo=-prima/2,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_vencida_50_50_enero_marzo_2(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 4, 30),
        fecha_expedicion_poliza=date(2025, 1, 31),
        fecha_contabilizacion_recibo=date(2025, 3, 22),
        fecha_inicio_vigencia_recibo=date(2025, 1, 31),
        fecha_fin_vigencia_recibo=date(2025, 3, 3),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 31),
        fecha_fin_vigencia_cobertura=date(2025, 3, 3),
    )
    prima = 1200

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=0,
        liberacion=prima/2,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_vencida_50_50_entra_devengado(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
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

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="entra_devengado",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=-prima,
        liberacion=prima,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_vencida_50_50_entra_devengado_valoracion_posterior(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    ###########
    """
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

    df = cf.crear_input_devengo(fechas, "produccion_directo", "directo", prima).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).filter(pl.col("tipo_contabilidad") == "ifrs4")
    resultado_devengo = cf.extraer_resultado_devengo(df_resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="entra_devengado",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=0,
        liberacion=0,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)