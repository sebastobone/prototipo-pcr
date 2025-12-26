from datetime import date
import polars as pl
from src import devenga, prep_insumo
from tests.devenga import conftest as cf


def test_pdn_anticipada_valorada_mes_del_recibo(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion anticipada en el mes que entra el recibo.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2024, 11, 30),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 11, 15),
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
        estado_devengo="no_iniciado",
        dias_devengados=0,
        dias_no_devengados=cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
        ),
        constitucion=-prima,
        liberacion=0,
        liberacion_acum=0,
        saldo=-prima,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_anticipada_valorada_pre_de_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion anticipada en un mes posterior a la
    entrada del recibo, pero previo al inicio de vigencia.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2024, 12, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 11, 15),
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

    resultado = devenga.devengar(df, fechas.fecha_valoracion)
    resultado_devengo = cf.extraer_resultado_devengo(resultado)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="no_iniciado",
        dias_devengados=0,
        dias_no_devengados=cf.diferencia_fechas_dias(
            fechas.fecha_fin_vigencia_cobertura,
            fechas.fecha_inicio_vigencia_cobertura,
        ),
        constitucion=0,
        liberacion=0,
        liberacion_acum=0,
        saldo=-prima,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_anticipada_valorada_en_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion anticipada en un mes posterior al
    inicio de vigencia, pero previo al fin de vigencia.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 6, 30),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 11, 15),
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

    dias_mes = cf.diferencia_fechas_dias(
        fechas.fecha_valoracion, fechas.fecha_valoracion.replace(day=1)
    )

    dias_devengados = cf.diferencia_fechas_dias(
        fechas.fecha_valoracion,
        fechas.fecha_inicio_vigencia_cobertura,
    )

    dias_no_devengados = cf.diferencia_fechas_dias(
        fechas.fecha_fin_vigencia_cobertura,
        fechas.fecha_valoracion,
        False,
    )

    valor_devengo_diario = prima / (dias_devengados + dias_no_devengados)

    resultado_esperado = cf.ResultadosDevengo(
        estado_devengo="en_curso",
        dias_devengados=dias_devengados,
        dias_no_devengados=dias_no_devengados,
        constitucion=0,
        liberacion=dias_mes * valor_devengo_diario,
        liberacion_acum=dias_devengados * valor_devengo_diario,
        saldo=-dias_no_devengados * valor_devengo_diario,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)


def test_pdn_anticipada_valorada_post_de_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion anticipada en un mes posterior al
    fin de vigencia.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2026, 1, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 11, 15),
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
        estado_devengo="finalizado",
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

def test_pdn_anticipada_50_50_valoracion_mes_recibo(
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
        fecha_inicio_vigencia_recibo=date(2025, 1, 15),
        fecha_fin_vigencia_recibo=date(2025, 2, 15),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 15),
        fecha_fin_vigencia_cobertura=date(2025, 2, 15),
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
        estado_devengo="no_iniciado",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=-prima,
        liberacion=0,
        liberacion_acum=0,
        saldo=-prima,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_anticipada_50_50_valoracion_post_recibo_pre_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2024, 11, 30),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 10, 4),
        fecha_inicio_vigencia_recibo=date(2025, 1, 15),
        fecha_fin_vigencia_recibo=date(2025, 2, 15),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 15),
        fecha_fin_vigencia_cobertura=date(2025, 2, 15),
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
        estado_devengo="no_iniciado",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=0,
        liberacion=0,
        liberacion_acum=0,
        saldo=-prima,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_anticipada_50_50_valoracion_mes_inicio_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 1, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 10, 4),
        fecha_inicio_vigencia_recibo=date(2025, 1, 15),
        fecha_fin_vigencia_recibo=date(2025, 2, 15),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 15),
        fecha_fin_vigencia_cobertura=date(2025, 2, 15),
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
        liberacion_acum=prima/2,
        saldo=-prima/2,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)

def test_pdn_anticipada_50_50_valoracion_mes_fin_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 2, 28),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 10, 4),
        fecha_inicio_vigencia_recibo=date(2025, 1, 15),
        fecha_fin_vigencia_recibo=date(2025, 2, 15),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 15),
        fecha_fin_vigencia_cobertura=date(2025, 2, 15),
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
        estado_devengo="finalizado",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=0,
        liberacion=prima/2,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)
    

def test_pdn_anticipada_50_50_valoracion_post_fin_vigencia(
    param_contabilidad: pl.DataFrame, excepciones_df: pl.DataFrame
):
    """
    Valoracion de PCR para produccion en medio de la vigencia, esto es, cuando 
    la totalidad de la vigencia está entre fecha_inicio_periodo y fecha_valoracion.
    """
    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 3, 31),
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2024, 10, 4),
        fecha_inicio_vigencia_recibo=date(2025, 1, 15),
        fecha_fin_vigencia_recibo=date(2025, 2, 15),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 15),
        fecha_fin_vigencia_cobertura=date(2025, 2, 15),
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
        estado_devengo="finalizado",
        dias_devengados=None,
        dias_no_devengados=None,
        constitucion=0,
        liberacion=0,
        liberacion_acum=prima,
        saldo=0,
    )

    cf.validar_resultado_devengo(resultado_devengo, resultado_esperado)