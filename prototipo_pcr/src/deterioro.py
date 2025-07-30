"""
Módulo de cálculo del deterioro del activo por reaseguro
"""

import src.cruces as cruces
import src.parametros as p
import polars as pl
import duckdb
import datetime as dt



def calc_deterioro(
    output_devengo_fluc: pl.DataFrame,
    riesgo_credito: pl.DataFrame,
    fe_valoracion:dt.date,
) -> pl.DataFrame:
    
    # Solo aplica para la fecha de valoracion y para reaseguro
    base_det = output_devengo_fluc.filter(
        (pl.col('fecha_valoracion') == fe_valoracion) & 
        (pl.col('tipo_negocio').is_in(['mantenido', 'retrocedido']))
    )

    # Cruza probabilidad de default PD vigente por reasegurador
    duckdb.register("base_det", base_det)
    duckdb.register("riesgo_credito", riesgo_credito)
    deterioro_pcr = duckdb.sql(
        """
        SELECT 
            pcr.*
            ,pd.probabilidad_incumplimiento prob_incumplimiento_actual
            ,pd_ant.probabilidad_incumplimiento prob_incumplimiento_anterior
        FROM base_det as pcr
            LEFT JOIN riesgo_credito as pd
                ON pcr.nit_reasegurador = pd.nit_reasegurador
                AND pcr.fecha_valoracion BETWEEN 
                    pd.fecha_inicio_vigencia AND 
                        COALESCE(pd.fecha_fin_vigencia, '3000-12-31')
            LEFT JOIN riesgo_credito as pd_ant
                ON pcr.nit_reasegurador = pd_ant.nit_reasegurador
                AND pcr.fecha_valoracion_anterior BETWEEN 
                    pd_ant.fecha_inicio_vigencia AND 
                        COALESCE(pd_ant.fecha_fin_vigencia, '3000-12-31')
        """
    ).pl(
    ).with_columns(
        (pl.col('prob_incumplimiento_actual') - pl.col('prob_incumplimiento_anterior'))
        .alias('cambio_prob_incumplimiento')
    )
    deterioro_pcr.write_clipboard()
    print(output_devengo_fluc.shape, base_det.shape, deterioro_pcr.shape)

    # calculo de movimientos de deterioro
    # se constituye deterioro si la probabilidad de default aumenta
    constitucion_det = pl.col('saldo') * pl.max_horizontal(pl.col('cambio_prob_incumplimiento'), pl.lit(0.0))
    # la liberación tiene dos partes
    # se libera porque el saldo cambia
    lib_cambio_saldo = pl.min_horizontal(pl.col('saldo') - pl.col('saldo_anterior'), pl.lit(0.0)) * pl.col('prob_incumplimiento_anterior')
    # se libera si baja la probabilidad
    lib_cambio_pd = pl.min_horizontal(pl.col('cambio_prob_incumplimiento'), pl.lit(0.0)) * pl.col('saldo')
    deterioro_pcr = deterioro_pcr.with_columns([
        constitucion_det.alias("constitucion_deterioro"),
        (lib_cambio_saldo + lib_cambio_pd).alias('liberacion_deterioro')
    ])

    # la parte a la cual no le aplica el deterioro tendra NA en las columnas creadas
    base_resto = output_devengo_fluc.filter(
        ~((pl.col('fecha_valoracion') == fe_valoracion) & 
        (pl.col('tipo_negocio').is_in(['mantenido', 'retrocedido'])))
    ).with_columns([
        # crea las columnas de deterioro con nulos para poder combinar
        pl.lit(None).cast(pl.Float64).alias(det_col)
        for det_col in [
            'prob_incumplimiento_actual', 
            'prob_incumplimiento_anterior',
            'cambio_prob_incumplimiento', 
            'constitucion_deterioro', 
            'liberacion_deterioro', 
            ]
    ])
    print(output_devengo_fluc.shape, base_det.shape, base_resto.shape, deterioro_pcr.shape)

    return base_resto.vstack(deterioro_pcr)