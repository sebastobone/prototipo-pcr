import polars as pl
import duckdb


# Cruza tipo de insumo con todos los parametros contabilidad que le aplican
def cruzar_param_contabilidad(
    base: pl.DataFrame, param_contabilidad: pl.DataFrame
) -> pl.DataFrame:
    # Si tipo cont viene en base, se eliminan combinaciones invalidas y se debe usar es la de param_cont
    if "tipo_contabilidad" in base.columns:
        join_kind = (
            "INNER"  # usa un inner join para conservar solo combinaciones válidas
        )
        tipocont_key = "AND b.tipo_contabilidad = param_cont.tipo_contabilidad"
        exclude = (
            "EXCLUDE (tipo_contabilidad)"  # excluye tipo_contabilidad que trae la base
        )
    else:
        join_kind, tipocont_key, exclude = "LEFT", "", ""
    return duckdb.sql(
        f"""
        SELECT
             b.* {exclude}
            ,param_cont.tipo_contabilidad as tipo_contabilidad
            ,param_cont.componente
            ,param_cont.clasificacion_adicional
            ,param_cont.tipo_contrato
            ,param_cont.nivel_detalle
            ,param_cont.signo_constitucion
        FROM base AS b
            {join_kind} JOIN param_contabilidad AS param_cont
                ON b.tipo_insumo = param_cont.tipo_insumo
                AND b.tipo_negocio = param_cont.tipo_negocio
                {tipocont_key}
        """
    ).pl()


# Cruza porcentaje descuento con produccion
# las columnas del cruce pueden cambiar segun como venga la tabla del descuento
def cruzar_descuento(
    produccion: pl.DataFrame, descuento: pl.DataFrame, reaseguro=False
) -> pl.DataFrame:
    # usa sufijo de rea para que cruce el dscto con el recibo de rea y no del directo
    suffix_rea = "_rea" if reaseguro else ""
    return duckdb.sql(
        f"""
        SELECT
            prod.*
            , COALESCE(
                dcto.podto_comercial,
                0
            ) AS podto_comercial
            , COALESCE(
                dcto.podto_tecnico,
                0
            ) AS podto_tecnico
        FROM produccion AS prod
            LEFT JOIN descuento AS dcto
                ON prod.compania = dcto.compania
                AND prod.ramo_sura = dcto.ramo_sura
                AND prod.poliza = dcto.poliza
                AND prod.recibo = dcto.recibo{suffix_rea}
                AND prod.poliza_certificado = dcto.poliza_certificado
        """
    ).pl()


# Cruza produccion y gastos segun el nivel de detalle encontrado en la tabla gasto
def cruzar_gastos_expedicion(
    produccion: pl.DataFrame, gastos: pl.DataFrame, reaseguro=False
) -> pl.DataFrame:
    """
    Cruza base de primas con los porcentajes de gasto correspondientes,
    como el cruce puede ser anterior al cruce con parametros, garantiza que
    en este cruce aparezcan todas las combinaciones de la prima con tipo_contabilidad.
    Evita duplicados usando prioridad de coincidencia para usar comodines correctamente
    """
    # El cruce debe ser por fecha expedición póliza y no por fecha de contabilización
    return duckdb.sql("""
        -- prioridad de cruce por comodin para evitar duplicados
        WITH gastos_priorizados AS (
            SELECT
                g.*,
                CASE 
                    WHEN canal <> '*' AND producto <> '*' THEN 1
                    WHEN canal <> '*' AND producto = '*' THEN 2
                    WHEN canal = '*' AND producto = '*' THEN 3
                END AS prioridad_match
            FROM gastos g
        ),
        cruce AS (
            SELECT 
                prod.*,
                g.tipo_contabilidad,
                g.tipo_gasto,
                g.porc_gasto,
                g.prioridad_match,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        prod.tipo_op,
                        prod.tipo_insumo,
                        prod.poliza,
                        prod.poliza_certificado,
                        prod.recibo,
                        prod.amparo,
                        g.tipo_gasto,
                        g.tipo_contabilidad
                    ORDER BY g.prioridad_match
                ) AS rn
            FROM produccion AS prod
            JOIN gastos_priorizados g
                ON prod.compania = g.compania
                AND prod.ramo_sura = g.ramo_sura
                AND (prod.canal = g.canal OR g.canal = '*')
                AND (prod.producto = g.producto OR g.producto = '*')
                AND prod.fecha_expedicion_poliza BETWEEN g.fecha_inicio AND g.fecha_fin
        )
        SELECT * 
        FROM cruce
        WHERE rn = 1
    """).pl()  # .unique()


def cruzar_excepciones_50_50(
    base: pl.DataFrame, excepciones: pl.DataFrame
) -> pl.DataFrame:
    """
    Asigna la marca candidato_50_50 = 0 cuando sea una excepcion a la regla 50/50,
    ignorando columnas no presentes en `base`.
    """
    # Aplica la regla por defecto (marca con 1) si la columna no existe
    if "candidato_devengo_50_50" not in base.columns:
        base = base.with_columns(pl.lit(1).alias("candidato_devengo_50_50"))
    # Filtra columnas relevantes (las que existen en base)
    columnas_base = set(base.columns)
    exc_cols = [col for col in excepciones.columns if col != "candidato_devengo_50_50"]
    # Filtra excepciones: solo considera filas que no exigen columnas ausentes
    excepciones_filtradas = excepciones.filter(
        pl.all_horizontal(
            [((pl.col(col) == "*") | pl.lit(col in columnas_base)) for col in exc_cols]
        )
    )

    # Itera sobre excepciones válidas
    for row in excepciones_filtradas.iter_rows(named=True):
        condition = pl.lit(True)
        for col in exc_cols:
            if col not in columnas_base:
                continue  # ignora columnas ausentes

            if row[col] != "*":
                val = row[col]
                col_dtype = base.schema[col]
                if col_dtype in [pl.Int64, pl.Int32]:
                    val = int(val)
                elif col_dtype == pl.Utf8:
                    val = str(val)
                condition &= pl.col(col) == val

        base = base.with_columns(
            pl.when(condition)
            .then(row["candidato_devengo_50_50"])
            .otherwise(pl.col("candidato_devengo_50_50"))
            .alias("candidato_devengo_50_50")
        )

    return base


def cruzar_tasas_cambio(
    base: pl.DataFrame,
    tasas_cambio: pl.DataFrame,
    val_anterior: bool = True,
    bautizo: bool = True,
    liquidacion: bool = True,
) -> pl.DataFrame:
    # Crea la parte opcional del query para cruzar otras tasas
    tasa_bool = [val_anterior, bautizo, liquidacion]
    tasa_col = [
        ", COALESCE(tfval_ant.tasa_cambio, 1) AS tasa_cambio_fecha_valoracion_anterior",
        ", COALESCE(tconst.tasa_cambio, 1) AS tasa_cambio_fecha_constitucion",
        ", COALESCE(tliquid.tasa_cambio, 1) AS tasa_cambio_fecha_liquidacion",
    ]
    tasa_join = [
        """LEFT JOIN tasas_cambio AS tfval_ant
            ON base.fecha_valoracion_anterior = tfval_ant.fecha
            AND base.moneda = tfval_ant.moneda_origen""",
        """LEFT JOIN tasas_cambio AS tconst
            ON base.fecha_constitucion = tconst.fecha
            AND base.moneda = tconst.moneda_origen""",
        """LEFT JOIN tasas_cambio AS tliquid
           ON base.fecha_fin_devengo = tliquid.fecha
           AND base.moneda = tliquid.moneda_origen""",
    ]
    cols_query, joins_query = "", ""
    for tb, tc, tj in zip(tasa_bool, tasa_col, tasa_join):
        if tb:
            cols_query += "\n" + tc
            joins_query += "\n" + tj

    return duckdb.sql(
        f"""
        SELECT
            base.*
            , COALESCE(tfval.tasa_cambio, 1) AS tasa_cambio_fecha_valoracion
            {cols_query}
        FROM base
        LEFT JOIN tasas_cambio AS tfval
            ON base.fecha_valoracion = tfval.fecha
            AND base.moneda = tfval.moneda_origen
            {joins_query}
        """
    ).pl()
