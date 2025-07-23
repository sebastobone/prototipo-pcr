"""
Módulo de cálculo del deterioro del activo por reaseguro bajo IFRS17
"""

import src.cruces as cruces
import polars as pl
import duckdb
import datetime as dt
from typing import List


def calcular_deterioro(
    output_devengo: pl.DataFrame,
    riesgo_credito: pl.DataFrame,
    variables_uoa: List[str],
    historico_deterioro:pl.DataFrame,
    tasas_cambio:pl.DataFrame,
    fe_valoracion:dt.date,
) -> pl.DataFrame:
    """
    Calcula el deterioro de activos por reaseguro según criterios de IFRS17
    a partir del output de devengo y la probabilidad de incumplimiento
    Regresa movimientos de deterioro, la constitucion es negativa y la liberacion es positiva
    """

    agrupadoras = variables_uoa + ["fecha_valoracion"]

    historico_deterioro = historico_deterioro.cast({
        col: output_devengo.schema[col]
        for col in agrupadoras if col in output_devengo.columns
    }).cast({'valor_deterioro': pl.Float64, 'saldo_pcr': pl.Float64, 'probabilidad_incumplimiento': pl.Float64})

    saldo_uoa = output_devengo.filter(
        # Solo aplica para la fecha de valoracion y para reaseguro
        (pl.col('fecha_valoracion') == fe_valoracion) & (~pl.col('tipo_negocio').is_in(['directo', 'camara_soat']))
    ).group_by(agrupadoras).agg(
        # Totaliza por unidad de cuenta, reasegurador y fecha
        pl.col("saldo").sum().alias("saldo_pcr")
    )

    # Cruza probabilidad de default PD vigente por reasegurador
    duckdb.register("saldo_uoa", saldo_uoa)
    duckdb.register("riesgo_credito", riesgo_credito)
    deterioro_pcr = duckdb.sql(
        """
        SELECT 
            pcr.*
            ,pd.probabilidad_incumplimiento
        FROM saldo_uoa as pcr
            LEFT JOIN riesgo_credito as pd
                ON pcr.nit_reasegurador = pd.nit_reasegurador
                AND pcr.fecha_valoracion BETWEEN 
                    pd.fecha_inicio_vigencia AND 
                        COALESCE(pd.fecha_fin_vigencia, '3000-12-31')
        """
    ).pl()
    
    if historico_deterioro.is_empty():
        # Garantiza que 'liberacion' exista aunque el join no se haga
        deterioro_pcr = deterioro_pcr.with_columns(pl.lit(0.0).alias('liberacion'))
    else:
        deterioro_pcr = deterioro_pcr.join(
            historico_deterioro
                .filter(pl.col('fecha_valoracion') < fe_valoracion)
                .sort("fecha_valoracion")
                .group_by(variables_uoa)
                .tail(1)
                .rename({"valor_deterioro": "liberacion"})
                .select(variables_uoa + ["liberacion"]),
            how='left',
            on=variables_uoa
        ).with_columns(
            pl.col("liberacion").fill_null(0.0)
        )

    deterioro_pcr = deterioro_pcr.with_columns([
        pl.when(pl.col("saldo_pcr") > 0)
        .then(pl.col("saldo_pcr") * pl.col("probabilidad_incumplimiento"))
        .otherwise(0.0).alias("valor_deterioro")
        ]).filter(pl.col('valor_deterioro') > 0)
    
    # Actualiza el historico con el ultimo deterioro
    historico_actualizado = pl.concat(
        [historico_deterioro, deterioro_pcr.select(historico_deterioro.columns)], how='diagonal')

    # pivot para que quede de una vez por tipo de movimiento
    output_deterioro = (
        deterioro_pcr
        .rename({'valor_deterioro': 'constitucion'})
        .select(agrupadoras + ['saldo_pcr', 'probabilidad_incumplimiento', 'liberacion', 'constitucion'])
        .unpivot(
            index=agrupadoras + ['saldo_pcr', 'probabilidad_incumplimiento'],
            variable_name='tipo_movimiento',
            value_name='valor_md'
        )
        # calcula valor en pesos con tasa de cambio de valoracion
        .pipe(cruces.cruzar_tasas_cambio, tasas_cambio, False, False, False)
        .with_columns(
            ((   # constitucion negativa y liberacion positiva
                pl.when(pl.col('tipo_movimiento')=='constitucion')
                .then(pl.lit(-1))
                .otherwise(pl.lit(1))
            ) * pl.col('valor_md'))
            .alias('valor_md')
        ).with_columns(
            (pl.col('valor_md') * pl.col('tasa_cambio_fecha_valoracion')).alias('valor_ml')
        )
    )

    return output_deterioro, historico_actualizado