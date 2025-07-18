"""
Módulo de cálculo del deterioro del activo por reaseguro bajo IFRS17
Requiere:
- Output de devengo (output_devengo)
- Tabla riesgo_credito con columnas: fecha_inicio, fecha_fin, nit_reasegurador, probabilidad_incumplimiento
- Tabla cuentas_reaseguro con columnas: fecha_valoracion, tipo_cuenta ('cxc_comisiones', 'cxp_reasegurador'), valor_saldo, y columnas necesarias para agregación
"""

import polars as pl
import duckdb
import datetime as dt
from typing import List


def calcular_deterioro(
    output_devengo: pl.DataFrame,
    riesgo_credito: pl.DataFrame,
    cuentas_reaseguro: pl.DataFrame,
    variables_uoa: List[str],
    historico_deterioro:pl.DataFrame,
    fe_valoracion:dt.Date
) -> pl.DataFrame:
    """
    Calcula el deterioro de activos por reaseguro según criterios de IFRS17

    Args:
        output_devengo: DataFrame con el resultado del devengo
        riesgo_credito: DataFrame con probabilidad de incumplimiento por reasegurador y fecha
        parametros_deterioro: DataFrame que define los meses a deteriorar por tipo_contabilidad
        cuentas_reaseguro: DataFrame con CxC y CxP a incluir solo para ifrs_corporativo
        variables_uoa: Lista de variables que definen la unidad de cuenta
    
    Returns:
        DataFrame con el deterioro por unidad de cuenta y fecha_valoracion
    """

    agrupadoras = variables_uoa + ["fecha_valoracion"]
    saldo_uoa = output_devengo.filter(
        # Solo aplica para la fecha de valoracion y para reaseguro
        (pl.col('fecha_valoracion') == fe_valoracion) & (~pl.col('tipo_negocio').is_in(['directo', 'camara_soat']))
    ).groupby(agrupadoras).agg(
        # Totaliza por unidad de cuenta, reasegurador y fecha
        pl.col("saldo").sum().alias("saldo_pcr")
    ).join(
        # Cruza con saldos cta corriente NIIF17C
        cuentas_reaseguro.with_columns(
                pl.lit('ifrs17_corporativo').alias('tipo_contabilidad')
            ).with_columns([
                # Ajusta el signo con el que debe considerarse el saldo
                pl.when(pl.col("tipo_cuenta") == "cxc_comisiones").then(pl.col("valor"))
                .when(pl.col("tipo_cuenta") == "cxp_reasegurador").then(-pl.col("valor"))
                .otherwise(0).alias("valor_ctacte")
            ]).group_by(agrupadoras).agg(
                pl.col('valor_ctacte').sum().alias('saldo_ctacte')
            ),
        on=agrupadoras, how='left'
    ).with_columns(
        (pl.col("saldo_pcr") + pl.col("saldo_ctacte").fill_null(0.0)).alias("saldo_neto_pcr")
    )

    # Cruza probabilidad de default PD vigente por reasegurador
    deterioro_pcr = duckdb(
        """
        SELECT 
            pcr.*
            ,pd.probabilidad_incumplimiento
        FROM saldo_uoa as pcr
            LEFT JOIN riesgo_credito as pd
                ON pcr.nit_reasegurador = pd.nit_reasegurador
                AND pcr.fecha_valoracion BETWEEN pd.fecha_inicio_vigencia AND pd.fecha_fin_vigencia
        """
    ).pl().join(
        # cruza con la pd del ultimo deterioro realizado
        historico_deterioro
            .filter(pl.col('fecha_valoracion') < fe_valoracion)  # Asegura que solo mire deterioros anteriores
            .sort("fecha_valoracion")  # Asegura que la última sea la más reciente
            .group_by(variables_uoa)
            .tail(1)  # Última fila por grupo
            .rename({"probabilidad_incumplimiento": "probabilidad_incumplimiento_base"})
            .select(variables_uoa + ["probabilidad_incumplimiento_base"]),
        how='left',
        on=variables_uoa
    ).with_columns(
        # solo deteriora si es saldo activo y hubo incremento en probabilidad, si no hay prob base usa cero
        (pl.col('probabilidad_incumplimiento') - pl.coalesce(pl.col('probabilidad_incumplimiento_base'), pl.lit(0.0)))
        .alias('cambio_probabilidad')
    ).with_columns(
        pl.when(pl.col("saldo_neto_pcr") > 0) & (pl.col("cambio_probabilidad") > 0)
        .then(pl.col("saldo_neto_pcr") * pl.col("probabilidad_incumplimiento"))
        .otherwise(0.0).alias("valor_deterioro")
    ).select(agrupadoras + ["saldo_neto_pcr", "probabilidad_incumplimiento", "valor_deterioro"])
    
    # Actualiza el historico con el ultimo deterioro
    historico_actualizado = pl.concat([historico_deterioro, deterioro_pcr], how='diagonal')

    return deterioro_pcr, historico_actualizado