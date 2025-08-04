import polars as pl
import src.parametros as params
import src.cruces as cruces
import duckdb

# Tipo_reserva = PCR CP para todos
# Columnas a Filas
# Componente:
# Tipo_Movimiento: si es transicion cambiarle el nombre al saldo

map_campos_bt = {
    'tipo_contabilidad': 'tipo_contabilidad',
    'compania': 'compania',
    'tipo_seguro': 'tipo_seguro',
    'naturaleza': 'naturaleza',
    'tipo_negocio': 'tipo_negocio',
    'tipo_reaseguro': 'tipo_reaseguro',
    'tipo_reasegurador': 'tipo_reasegurador',
    'componente': 'concepto',
    'tipo_movimiento': 'tipo_movimiento',
    'anio_liberacion': 'indicativo_periodo_movimiento',
    'es_transicion': 'transicion',
}


def asignar_campos_sabana(
        output_det: pl.DataFrame,
        tabla_concepto: pl.DataFrame,
        tabla_tipo_rea: pl.DataFrame,
        tabla_tipo_seg:pl.DataFrame,
    ):
    pass


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


def cruzar_bt(
    out_devengo_fluct:pl.DataFrame, 
    relacion_bt:pl.DataFrame
    ) ->pl.DataFrame:
    """
    Cruza output de devengo con la tabla de bts
    """

    #     """
    #     SELECT
    #         base.*
    #         , bt.descripcion AS descripcion_bt
    #         , bt.bt AS bt
    #     FROM out_devengo_fluct as base
    #     INNER JOIN mapeo_bt AS bt
    #         ON base.tipo_contabilidad = bt.tipo_contabilidad
    #         AND base.tipo_insumo = bt.tipo_insumo
    #         AND base.tipo_contrato = bt.tipo_contrato
    #         AND base.tipo_movimiento = bt.tipo_movimiento
    #         AND base.anio_liberacion = bt.anio_liberacion
    #         AND base.transicion = bt.transicion
    #     """
    # ).pl()

    pass


def pivotear_output(
        out_deterioro_fluct: pl.DataFrame, 
        cols_calculadas: list
    ) -> pl.DataFrame:
    """
    El output de devengo es wide, se transforma a long para cruzar con BTs.
    """
    columnas_indice = [col for col in out_deterioro_fluct.columns if col not in cols_calculadas]

    # Validar que existe la tasa de cambio para expresar valores en pesos
    if 'tasa_cambio_fecha_valoracion' in out_deterioro_fluct.columns:
        multiplicador = pl.col('tasa_cambio_fecha_valoracion')
    else:
        print("[WARN] La columna 'tasa_cambio_fecha_valoracion' no está presente, se asumirá 1.")
        multiplicador = pl.lit(1.0)

    return (
        out_deterioro_fluct
        # columnas calculadas a filas
        .unpivot(
            index=columnas_indice, 
            variable_name="tipo_movimiento", 
            value_name='valor_md'
        )
        # convierte valores validos a pesos
        .filter(pl.col("valor_md").is_not_null())
        .with_columns(
            (multiplicador * pl.col('valor_md')).alias('valor_ml')
        )
        # Reasigna el componente a deterioro cuando corresponde
        .with_columns(
            pl.when(pl.col('tipo_movimiento').is_in(['constitucion_deterioro', 'liberacion_deterioro']))
            .then(pl.lit('deterioro'))
            .otherwise(pl.col('componente'))
            .alias('componente')
        )
    )


def add_registros_dac(out_contable:pl.DataFrame) -> pl.DataFrame:
    """
    Duplica las filas del gasto aplicado al reaseguro proporcional pero con el signo contrario
    esto para incluir el componente de ajuste del DAC cedido. Solo aplica en IFRS 4
    """
    dac_rea = out_contable.filter(
        (pl.col('tipo_insumo').is_in(['gasto_comi_rea_prop', 'gasto_otro_rea_prop'])) &
        (pl.col('tipo_contabilidad') == 'ifrs4')
    ).with_columns([
        pl.lit('ajuste_dac_cedido').alias('tipo_insumo'),
        pl.lit('ajuste_dac_') + pl.col('concepto').str.split('_')[-1]
        (pl.col('valor') * -1 ).alias('valor')
    ])
    return pl.concat([out_contable, dac_rea])


def cruzar_campos_sabana(
    out_det_fluc:pl.DataFrame,
    tabla_nomenclatura:pl.DataFrame,
    tabla_componente:pl.DataFrame
    ) -> pl.DataFrame:

    duckdb.register('base_output', out_det_fluc)
    duckdb.register('nomen', tabla_nomenclatura)
    duckdb.register('componente', tabla_componente)

    output_listo = duckdb.sql(
        """
        SELECT 
            bout.*,
            comp.concepto_cd,
            comp.clasificacion_adicional_cd,
            nomen
        """
    ).pl()
    pass


def gen_output_contable(
        out_det_fluc:pl.DataFrame, 
        tabla_mapeo_bt:pl.DataFrame, 
        tabla_tipo_seg:pl.DataFrame
    ) -> pl.DataFrame:
    """
    Se encarga de aplicar los pasos para obtener el output segun requerimientos contables
    """

    return (
        out_det_fluc
        .with_columns([
            pl.when(pl.col('tipo_seguro') == "directo")
            .then(pl.lit('D'))
            .otherwise(pl.lit('R'))
            .alias('naturaleza'),
            pl.when(pl.col('tipo_seguro') == "reaseguro_proporcional")
            .then(pl.lit('PP'))
            .when(pl.col('tipo_seguro') == "reaseguro__no_proporcional")
            .then(pl.lit('NP'))
            .otherwise(pl.lit('No aplica'))
            .alias('tipo_reaseguro')
        ])
        .pipe(asignar_tipo_seguro, tabla_tipo_seg)
        .pipe(pivotear_output, params.COLUMNAS_CALCULO)
        .pipe(add_registros_dac)
        .pipe(cruzar_bt, tabla_mapeo_bt)
    )
