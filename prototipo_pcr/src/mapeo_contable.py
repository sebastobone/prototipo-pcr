"""
Módulo que se encarga de la traducción del output del cálculo de la PCR a las BTs para el motor de posteos contables
- El output del devengo, fluctuación y deterioro es en formato de columnas
- Organiza el output en filas por tipo de movimiento y lo expresa en moneda del documento y local
- Asigna tipo_seguro según la tabla parametrizada
- Asigna código I o E según nacionalidad del reasegurador
- Cruza cada movimiento del output con tabla de mapeo de BTs contables
"""

import polars as pl
import src.parametros as params
import src.cruces as cruces
import duckdb

# Tipo_reserva = PCR CP para todos
# Columnas a Filas
# Componente:
# Tipo_Movimiento: si es transicion cambiarle el nombre al saldo


def asignar_tipo_seguro(base:pl.DataFrame, tipo_seg:pl.DataFrame) -> pl.DataFrame:
    """
    Asigna la clasificacion del ramo por tipo de seguro al output contable
    """
    duckdb.register("base", base)
    duckdb.register("tipo_seg", tipo_seg)
    return duckdb.sql(
        """
        SELECT
            base.*
            ,tseg.tipo_seguro
            ,tseg.codigo_tipo_seguro
        FROM base
        LEFT JOIN tipo_seg as tseg
            ON base.ramo_sura = tseg.ramo
        """
    ).pl()


def cruzar_diccionario_bt(out_devengo_fluct:pl.DataFrame, mapeo_bt:pl.DataFrame) ->pl.DataFrame:
    """
    Cruza output de devengo con la tabla de bts
    """
    return duckdb.sql(
        """
        SELECT
            base.*
            , bt.descripcion AS descripcion_bt
            , bt.bt AS bt
        FROM out_devengo_fluct as base
        INNER JOIN mapeo_bt AS bt
            ON base.tipo_contabilidad = bt.tipo_contabilidad
            AND base.tipo_insumo = bt.tipo_insumo
            AND base.tipo_contrato = bt.tipo_contrato
            AND base.tipo_movimiento = bt.tipo_movimiento
            AND base.anio_liberacion = bt.anio_liberacion
            AND base.transicion = bt.transicion
        """
    ).pl()


def transformar_columnas_calculo_a_filas(out_devengo_fluct: pl.DataFrame, cols_calculadas: list) -> pl.DataFrame:
    """
    El output de devengo es wide, se transforma a long para cruzar con BTs.
    """
    columnas_indice = [col for col in out_devengo_fluct.columns if col not in cols_calculadas]

    if 'tasa_cambio_fecha_valoracion' in out_devengo_fluct.columns:
        multiplicador = pl.col('tasa_cambio_fecha_valoracion')
    else:
        print("[WARN] La columna 'tasa_cambio_fecha_valoracion' no está presente, se asumirá 1.")
        multiplicador = pl.lit(1.0)

    return (
        out_devengo_fluct.unpivot(index=columnas_indice, variable_name="tipo_movimiento", value_name='valor_md')
            .with_columns(
                (multiplicador * pl.col('valor_md')).alias('valor_ml')
            )
            .filter(pl.col("valor_md").is_not_null() & (pl.col("valor_md") != 0.0))
    )



def gen_output_contable(output_devengo_fluct:pl.DataFrame, tabla_mapeo_bt:pl.DataFrame, tabla_tipo_seg:pl.DataFrame):
    """
    Se encarga de aplicar los pasos para obtener el output segun requerimientos contables
    """
    print(sorted(output_devengo_fluct.columns))
    return (
        output_devengo_fluct.filter(pl.col('estado_devengo') != 'finalizado')
        .pipe(asignar_tipo_seguro, tabla_tipo_seg)
        .pipe(transformar_columnas_calculo_a_filas, params.COLUMNAS_CALCULO)
        .pipe(cruzar_diccionario_bt, tabla_mapeo_bt)
    )
