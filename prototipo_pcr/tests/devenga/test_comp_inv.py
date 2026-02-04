from datetime import date
import polars as pl
from src import devenga, prep_insumo
from tests.devenga import conftest as cf

def test_componente_inversion_poliza_no_cancelada():
    """
    Test para validar un registro de una póliza que no tiene fecha de cancelación y, 
    por tanto, la fecha de liberación debería ser la fecha_fin_vigencia_poliza
    """
    param_contabilidad = pl.read_excel(
        rf'inputs\insumos.xlsx', sheet_name='param_contabilidad'
    ).filter(pl.col("estado_insumo") == 1)

    fechas = cf.Fechas(
        fecha_valoracion=date(2026, 1, 31),
        fecha_expedicion_poliza=date(2025, 10, 1),
        fecha_cancelacion_poliza=None,
        fecha_inicio_vigencia_poliza=date(2025, 10, 1),
        fecha_fin_vigencia_poliza=date(2026, 10, 1)
    )
    prima = 8649
    registros_prueba = [
        cf.registro_prueba_componente_inversion(
            fecha_contabilizacion_recibo=date(2025, 11, 5),
            tipo_op='23',
            valor_prima=prima,
            resultado_esperado=cf.ResultadosDevengo(
                constitucion=0, liberacion=0, liberacion_acum=0, saldo=-prima, fecha_liberacion=date(2026, 10, 1)
            )
        ),
        cf.registro_prueba_componente_inversion(
            fecha_contabilizacion_recibo=date(2026, 1, 5),
            tipo_op='23',
            valor_prima=prima,
            resultado_esperado=cf.ResultadosDevengo(
                constitucion=-prima, liberacion=0, liberacion_acum=0, saldo=-prima, fecha_liberacion=date(2026, 10, 1)
            )
        ),
    ]

    df_input = cf.crear_input_componente_inversion(
        fechas,
        tipo_insumo="componente_inversion_directo",
        tipo_negocio="directo",
        moneda="COP",
        poliza='100',
        registros_prueba=registros_prueba
    )

    df_preparado = df_input.pipe(
        prep_insumo.prep_input_componente_inversion,
        param_contabilidad,
        fechas.fecha_valoracion,
    ).with_columns(
        pl.lit(None).alias('dias_devengados'),
        pl.lit(None).alias('dias_no_devengados'),
        pl.lit(None).alias('control_suma_dias'),
        pl.lit(None).alias('dias_constitucion'),
        pl.lit(None).alias('dias_liberacion'),
        pl.lit(None).alias('valor_devengo_diario'),
        pl.lit(None).alias('estado_devengo'),
        pl.lit(0).alias('candidato_devengo_50_50')
    )

    df_resultado = devenga.devengar(
        df_preparado, fechas.fecha_valoracion
    )
    df_resultado.write_clipboard()
    cf.validar_resultados_multiples(df_resultado, registros_prueba)

def test_componente_inversion_poliza_cancelada_sin_anulaciones():
    """
    Test para validar
    """
    param_contabilidad = pl.read_excel(
        rf'inputs\insumos.xlsx', sheet_name='param_contabilidad'
    ).filter(pl.col("estado_insumo") == 1)

    fechas = cf.Fechas(
        fecha_valoracion=date(2025, 12, 31),
        fecha_expedicion_poliza=date(2024, 5, 15),
        fecha_cancelacion_poliza=date(2025, 11, 15),
        fecha_inicio_vigencia_poliza=date(2025, 5, 15),
        fecha_fin_vigencia_poliza=date(2026, 5, 15)
    )
    prima = 12109
    
    registros_prueba = [
        cf.registro_prueba_componente_inversion(
            fecha_contabilizacion_recibo=date(2025, 11, 6),
            tipo_op='23',
            valor_prima=prima,
            resultado_esperado=cf.ResultadosDevengo(
                constitucion=0, liberacion=0, liberacion_acum=0, saldo=-prima, fecha_liberacion=date(2026, 5, 15)
            )
        ),
        cf.registro_prueba_componente_inversion(
            fecha_contabilizacion_recibo=date(2025, 12, 24),
            tipo_op='23',
            valor_prima=prima,
            resultado_esperado=cf.ResultadosDevengo(
                constitucion=-prima, liberacion=0, liberacion_acum=0, saldo=-prima, fecha_liberacion=date(2026, 5, 15)
            )
        ),
    ]

    df_input = cf.crear_input_componente_inversion(
        fechas,
        tipo_insumo="componente_inversion_directo",
        tipo_negocio="directo",
        moneda="COP",
        poliza='100',
        registros_prueba=registros_prueba
    )

    df_preparado = df_input.pipe(
        prep_insumo.prep_input_componente_inversion,
        param_contabilidad,
        fechas.fecha_valoracion,
    ).with_columns(
        pl.lit(None).alias('dias_devengados'),
        pl.lit(None).alias('dias_no_devengados'),
        pl.lit(None).alias('control_suma_dias'),
        pl.lit(None).alias('dias_constitucion'),
        pl.lit(None).alias('dias_liberacion'),
        pl.lit(None).alias('valor_devengo_diario'),
        pl.lit(None).alias('estado_devengo'),
        pl.lit(0).alias('candidato_devengo_50_50')
    )

    df_resultado = devenga.devengar(df_preparado, fechas.fecha_valoracion)
    cf.validar_resultados_multiples(df_resultado, registros_prueba)

def test_componente_inversion_poliza_cancelada_con_anulacion():
    """
    Test para validar una póliza cancelada que incluye una anulación (tipo_op 26).
    La póliza se cancela el 2024-07-16, antes de completar su vigencia.
    Incluye:
    - Dos registros tipo_op 23 (emisiones normales) con prima positiva
    - Un registro tipo_op 26 (anulación) con prima negativa
    """
    param_contabilidad = pl.read_excel(
        rf'inputs\insumos.xlsx', sheet_name='param_contabilidad'
    ).filter(pl.col("estado_insumo") == 1)

    fechas = cf.Fechas(
        fecha_valoracion=date(2024, 7, 31),
        fecha_expedicion_poliza=date(2024, 6, 11),
        fecha_cancelacion_poliza=date(2024, 7, 16),
        fecha_inicio_vigencia_poliza=date(2024, 6, 11),
        fecha_fin_vigencia_poliza=date(2025, 6, 11)
    )
    
    registros_prueba = [
        cf.registro_prueba_componente_inversion(
            fecha_contabilizacion_recibo=date(2024, 6, 11),
            tipo_op='23',
            valor_prima=17298,
            resultado_esperado=cf.ResultadosDevengo(
                constitucion=0, liberacion=17298, liberacion_acum=17298, saldo=0, fecha_liberacion=date(2024, 7, 17)
            )
        ),
        cf.registro_prueba_componente_inversion(
            fecha_contabilizacion_recibo=date(2024, 7, 6),
            tipo_op='23',
            valor_prima=17298,
            resultado_esperado=cf.ResultadosDevengo(
                constitucion=-17298, liberacion=17298, liberacion_acum=17298, saldo=0, fecha_liberacion=date(2024, 7, 17)
            )
        ),
        cf.registro_prueba_componente_inversion(
            fecha_contabilizacion_recibo=date(2024, 7, 17),
            tipo_op='26',
            valor_prima=-14508,
            resultado_esperado=cf.ResultadosDevengo(
                constitucion=14508, liberacion=-14508, liberacion_acum=-14508, saldo=0, fecha_liberacion=date(2024, 7, 17)
            )
        ),
    ]

    df_input = cf.crear_input_componente_inversion(
        fechas,
        tipo_insumo="componente_inversion_directo",
        tipo_negocio="directo",
        moneda="COP",
        poliza='100',
        registros_prueba=registros_prueba
    )

    df_preparado = df_input.pipe(
        prep_insumo.prep_input_componente_inversion,
        param_contabilidad,
        fechas.fecha_valoracion,
    ).with_columns(
        pl.lit(None).alias('dias_devengados'),
        pl.lit(None).alias('dias_no_devengados'),
        pl.lit(None).alias('control_suma_dias'),
        pl.lit(None).alias('dias_constitucion'),
        pl.lit(None).alias('dias_liberacion'),
        pl.lit(None).alias('valor_devengo_diario'),
        pl.lit(None).alias('estado_devengo'),
        pl.lit(0).alias('candidato_devengo_50_50')
    )

    df_resultado = devenga.devengar(df_preparado, fechas.fecha_valoracion)

    df_resultado.write_clipboard()
    cf.validar_resultados_multiples(df_resultado, registros_prueba)

test_componente_inversion_poliza_cancelada_con_anulacion()