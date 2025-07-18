"""
Funciones para el cálculo de la fluctuación por tasa de cambio de los componentes de la reserva
se diferencian dos funciones porque la metodología actual se cambió para ifrs17
"""

import src.aux_tools as aux_tools
import src.cruces as cruces
import polars as pl


def calc_fluctuacion(data_devengo:pl.DataFrame, tasas_cambio:pl.DataFrame) -> pl.DataFrame:

    # Filtra para alicar solo a moneda extranjera
    data_devengo_mext = data_devengo.filter(pl.col('moneda') != "COP")
    data_devengo_mloc = data_devengo.filter(~(pl.col('moneda') != "COP"))

    # Cambios en tasa de cambio para usar segun el tipo de contabilidad
    # incluir columnas de estos deltas en el output
    delta_tc_bautizo = pl.col('tasa_cambio_fecha_valoracion') - pl.col('tasa_cambio_fecha_constitucion')
    delta_tc_mensual = pl.col('tasa_cambio_fecha_valoracion') - pl.col('tasa_cambio_fecha_valoracion_anterior')
    delta_tc_liquid = pl.col('tasa_cambio_fecha_liquidacion') - pl.col('tasa_cambio_fecha_valoracion_anterior')
    # usa el cambio en tasa respecto a la fecha de liquidacion cuando es el ultimo mes de devengo
    es_mes_liquidacion = aux_tools.yyyymm(pl.col('fecha_fin_devengo')) == aux_tools.yyyymm(pl.col('fecha_valoracion'))
    delta_tc_lib = pl.when(es_mes_liquidacion).then(delta_tc_liquid).otherwise(delta_tc_mensual)
    
    data_devengo_mext = data_devengo_mext.with_columns(
        # define fecha cierre anterior para el delta
        pl.col('fecha_valoracion').dt.offset_by('-1mo').alias('fecha_valoracion_anterior')
    ).pipe(cruces.cruzar_tasas_cambio, tasas_cambio).with_columns(
        pl.when(pl.col('tipo_contabilidad') == "ifrs4")
        .then(pl.lit(0.0))  # en 4 no se fluctua constitucion
        .otherwise(pl.col('valor_constitucion').abs() * delta_tc_bautizo) # en 17 con la tasa bautizo
        .alias('fluctuacion_constitucion')
    ).with_columns(
        pl.when(pl.col('tipo_contabilidad') == "ifrs4")
        .then(pl.col('saldo').abs() * delta_tc_mensual)
        .otherwise(pl.col('saldo').abs() * delta_tc_lib)
        .alias('fluctuacion_liberacion')
    )
    cols_fluc =  ["fluctuacion_constitucion", "fluctuacion_liberacion"]
    cols_tasas = [c for c in data_devengo_mext.columns if "tasa_cambio_" in c]
    # concatena con los no fluctuados y llena nulos
    data_devengo_fluc = pl.concat(
            [data_devengo_mloc, data_devengo_mext], how='diagonal').with_columns([
                pl.col(col).fill_null(0.0) for col in cols_fluc
            ]).with_columns([
                pl.col(col).fill_null(1.0) for col in cols_tasas
            ])
    
    return data_devengo_fluc
    