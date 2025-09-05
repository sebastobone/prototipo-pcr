import polars as pl
import src.parametros as params
import duckdb


def asignar_tipo_seguro(base: pl.DataFrame, tipo_seg: pl.DataFrame) -> pl.DataFrame:
    """
    Asigna la clasificacion del ramo por tipo de seguro al output contable
    """
    return base.join(tipo_seg.rename({"ramo": "ramo_sura"}), on="ramo_sura", how="left")


def cruzar_bt(
    out_devengo_fluct: pl.DataFrame, relacion_bt: pl.DataFrame
) -> pl.DataFrame:
    """
    Cruza output de devengo con la tabla de bts
    """
    print("Registros antes del cruce BT: ", out_devengo_fluct.shape[0])
    con = duckdb.connect(database=":memory:")  # isolate to memory
    result = duckdb.sql(
        """
        SELECT
            out_devengo_fluct.*,
            relacion_bt.naturaleza,
            relacion_bt.bt,
            relacion_bt.descripcion_bt,
        FROM out_devengo_fluct
        LEFT JOIN relacion_bt
            ON out_devengo_fluct.tipo_movimiento_codigo = relacion_bt.tipo_movimiento
            AND out_devengo_fluct.indicativo_periodo_movimiento_codigo = relacion_bt.indicativo_periodo_movimiento
            AND out_devengo_fluct.concepto_codigo = relacion_bt.concepto
            AND out_devengo_fluct.clasificacion_adicional_codigo = relacion_bt.clasificacion_adicional
            AND out_devengo_fluct.tipo_negocio_codigo = relacion_bt.tipo_negocio
            AND out_devengo_fluct.tipo_reaseguro_codigo = relacion_bt.tipo_reaseguro
            AND out_devengo_fluct.tipo_reasegurador_codigo = relacion_bt.tipo_reasegurador
            AND out_devengo_fluct.tipo_seguro_codigo = relacion_bt.tipo_seguro
            AND out_devengo_fluct.compania_codigo = relacion_bt.compania
            AND out_devengo_fluct.tipo_contabilidad_codigo = relacion_bt.tipo_contabilidad
            AND out_devengo_fluct.tipo_reserva = relacion_bt.tipo_reserva
        """
    ).pl()
    con.close()
    print("Registros despues del cruce BT: ", result.shape[0])
    return result


def pivotear_output(
    out_deterioro_fluct: pl.DataFrame, cols_calculadas: list
) -> pl.DataFrame:
    """
    El output de devengo es wide, se transforma a long para cruzar con BTs.
    """
    columnas_indice = [
        col for col in out_deterioro_fluct.columns if col not in cols_calculadas
    ]

    multiplicador = pl.col("tasa_cambio_fecha_valoracion")

    return (
        out_deterioro_fluct
        # columnas calculadas a filas
        .unpivot(
            index=columnas_indice,
            variable_name="tipo_movimiento",
            value_name="valor_md",
        )
        # convierte valores validos a pesos
        .filter(pl.col("valor_md").is_not_null() & (pl.col("valor_md") != 0))
        .with_columns((multiplicador * pl.col("valor_md")).alias("valor_ml"))
        # El deterioro esta asignado como prima en el concepto de BTs
        .with_columns(
            pl.when(
                pl.col("tipo_movimiento").is_in(
                    ["constitucion_deterioro", "liberacion_deterioro"]
                )
            )
            .then(pl.lit("prima"))
            .otherwise(pl.col("componente"))
            .alias("componente")
        )
        .with_columns(
            pl.when(
                pl.col("tipo_movimiento").is_in(
                    ["fluctuacion_constitucion", "fluctuacion_liberacion"]
                )
            )
            .then(pl.lit("fluctuacion"))
            .otherwise(pl.col("tipo_movimiento"))
            .alias("tipo_movimiento")
        )
    )


def add_registros_dac(out_contable: pl.DataFrame) -> pl.DataFrame:
    """
    Duplica las filas del gasto aplicado al reaseguro proporcional pero con el signo contrario
    esto para incluir el componente de ajuste del DAC cedido. Solo aplica en IFRS 4
    """
    dac_rea = (
        out_contable.filter(
            (
                pl.col("tipo_insumo").is_in(
                    ["gasto_comi_rea_prop", "gasto_otro_rea_prop"]
                )
            )
            & (pl.col("tipo_contabilidad") == "ifrs4")
        )
        .with_columns(pl.lit("ajuste_dac_cedido").alias("tipo_insumo"))
        .with_columns(
            [(pl.col(col) * -1).alias(col) for col in ["valor_md", "valor_ml"]]
        )
    )
    return out_contable.vstack(dac_rea)


def homologar_campos(
    out_det_fluc: pl.DataFrame,
    tabla_nomenclatura: pl.DataFrame,
) -> pl.DataFrame:
    tipo_movimiento = obtener_homologacion(
        tabla_nomenclatura, "tipo_movimiento", "tipo_movimiento"
    )
    periodo_movimiento = obtener_homologacion(
        tabla_nomenclatura, "indicativo_periodo_movimiento", "anio_liberacion"
    )
    concepto = obtener_homologacion(tabla_nomenclatura, "concepto", "componente")
    clasificacion_adicional = obtener_homologacion(
        tabla_nomenclatura, "clasificacion_adicional", "clasificacion_adicional"
    )
    tipo_negocio = obtener_homologacion(
        tabla_nomenclatura, "tipo_negocio", "tipo_negocio"
    )
    tipo_reaseguro = obtener_homologacion(
        tabla_nomenclatura, "tipo_reaseguro", "tipo_contrato"
    )
    tipo_reasegurador = obtener_homologacion(
        tabla_nomenclatura, "tipo_reasegurador", "tipo_reasegurador"
    )
    compania = obtener_homologacion(tabla_nomenclatura, "compania", "compania")
    tipo_contabilidad = obtener_homologacion(
        tabla_nomenclatura, "tipo_contabilidad", "tipo_contabilidad"
    )
    

    df = (
        out_det_fluc.join(tipo_movimiento, on="tipo_movimiento", how="left")
        .join(periodo_movimiento, on="anio_liberacion", how="left")
        .join(concepto, on="componente", how="left")
        .join(clasificacion_adicional, on="clasificacion_adicional", how="left")
        .join(tipo_negocio, on="tipo_negocio", how="left")
        .join(tipo_reaseguro, "tipo_contrato", how="left")
        .with_columns(pl.col("tipo_reasegurador").fill_null("no_aplica"))
        .join(tipo_reasegurador, on="tipo_reasegurador", how="left")
        .join(compania, on="compania", how="left")
        .join(tipo_contabilidad, on="tipo_contabilidad", how="left")
        .with_columns(tipo_reserva=pl.lit("PCR_CP"))
    )

    return df


def obtener_homologacion(
    tabla_nomenclatura: pl.DataFrame, campo: str, nombre_prototipo: str
) -> pl.DataFrame:
    return tabla_nomenclatura.filter(pl.col("campo") == campo).select(
        pl.col("prototipo").alias(nombre_prototipo),
        pl.col("codigo").alias(f"{campo}_codigo"),
    )


def gen_output_contable(
    out_det_fluc: pl.DataFrame,
    tabla_mapeo_bt: pl.DataFrame,
    tabla_tipo_seg: pl.DataFrame,
    tabla_nomenclatura: pl.DataFrame,
) -> pl.DataFrame:
    """
    Se encarga de aplicar los pasos para obtener el output segun requerimientos contables
    """

    return (
        out_det_fluc.pipe(asignar_tipo_seguro, tabla_tipo_seg)
        .pipe(pivotear_output, params.COLUMNAS_CALCULO)
        .pipe(add_registros_dac)
        .pipe(homologar_campos, tabla_nomenclatura)
        .pipe(cruzar_bt, tabla_mapeo_bt)
    )
