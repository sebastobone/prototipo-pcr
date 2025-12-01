"""
Integra todos los pasos del proceso de cálculo de la PCR CP y realiza los cálculos, exporta el output
"""

import src.parametros as p
import src.aux_tools as aux_tools
import src.prep_insumo as prep_data
import src.devenga as devg
import src.fluctuacion as fluc
import src.deterioro as det
import src.mapeo_contable as mapcont
import polars as pl
import glob


# lee los parámetros e insumos relevantes
FECHA_VALORACION = p.FECHA_VALORACION
FECHA_VALORACION_ANTERIOR = p.FECHA_VALORACION_ANTERIOR
FECHA_TRANSICION = p.FECHA_TRANSICION


def run_pcr():
    # Lectura de insumos
    # Insumos transversales
    param_contab = pl.read_excel(
        p.RUTA_INSUMOS, sheet_name=p.HOJA_PARAMETROS_CONTAB
    ).filter(
        pl.col("estado_insumo") == 1
    )  # Solo se usan las configuraciones activas (1)
    excepciones = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_EXCEPCIONES_50_50)
    gasto = pl.read_excel(p.RUTA_GASTOS)
    tasa_cambio = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_MONEDA)
    descuentos = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_DESCUENTO)
    # El diccionario o tabla de correspondencia de outputs con entradas contables
    input_map_bts = pl.read_excel(p.RUTA_REL_BT, infer_schema_length=2000).filter(
        ~pl.col("clasificacion_adicional").is_in(["MAT", "REC"])
    )
    input_tipo_seguro = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_TIPO_SEGURO)
    tabla_nomenclatura = pl.read_excel(p.RUTA_NOMENCLATURA, sheet_name="V2")
    # Insumos de recibos contabilizados en SAP
    produccion_dir = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_PDN)
    cesion_rea = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_CESION)
    comision_rea = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_COMISION_REA)
    costo_contrato_rea = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_COSTO_CONTRATO)
    seguimiento_rea = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_SEGUIMIENTO_REA)
    produccion_arl = pl.read_excel(p.RUTA_PRODUCCION_ARL)
    costo_contrato_arl = pl.read_excel(p.RUTA_COSTO_CONTRATO_ARL)
    # Insumos de onerosidad leidos desde el datalake
    onerosidad = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_ONEROSIDAD)
    recup_onerosidad = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_RECUP_ONEROSIDAD)
    # Insumos de riesgo de credito cargado por equipo de riesgo financiero
    riesgo_credito = pl.read_excel(p.RUTA_RIESGO_CREDITO)
    # Insumos no devengables
    cartera = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_CARTERA)
    cartera_arl = pl.read_excel(p.RUTA_CARTERA_ARL)
    cuenta_corriente = pl.read_excel(p.RUTA_CUENTA_CORRIENTE)
    cuenta_corriente_arl = pl.read_excel(p.RUTA_CUENTA_CORRIENTE_ARL)

    produccion_arl_prep = prep_data.prep_input_produccion_arl(produccion_arl)
    produccion_dir = pl.concat(
        [produccion_dir, produccion_arl_prep], how="diagonal_relaxed"
    )

    costo_contrato_rea = pl.concat(
        [costo_contrato_rea, costo_contrato_arl], how="diagonal_relaxed"
    )

    cartera = pl.concat(
        [
            cartera,
            cartera_arl.with_columns(
                fecha_expedicion_poliza=pl.col("mes_cotizacion").dt.month_start()
            ),
        ],
        how="diagonal_relaxed",
    )

    cuenta_corriente = pl.concat(
        [cuenta_corriente, cuenta_corriente_arl], how="diagonal_relaxed"
    )

    # Prepara cada insumo para entrar a devengo
    insumos_devengo = [
        # prepara insumos seguro directo
        prep_data.prep_input_prima_directo(
            produccion_dir, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_dcto_directo(
            produccion_dir, param_contab, excepciones, descuentos, FECHA_VALORACION
        ),
        prep_data.prep_input_gasto_directo(
            produccion_dir, param_contab, excepciones, gasto, FECHA_VALORACION
        ),
        prep_data.prep_input_onerosidad(onerosidad, param_contab, FECHA_VALORACION),
        # prepara insumos reaseguro
        prep_data.prep_input_prima_rea(
            cesion_rea, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_dcto_rea(
            cesion_rea, param_contab, excepciones, descuentos, FECHA_VALORACION
        ),
        prep_data.prep_input_gasto_rea(
            cesion_rea, param_contab, excepciones, gasto, FECHA_VALORACION
        ),
        prep_data.prep_input_comi_rea(
            comision_rea, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_costo_con(
            costo_contrato_rea,
            seguimiento_rea,
            param_contab,
            excepciones,
            FECHA_VALORACION,
        ),
        prep_data.prep_input_recup_onerosidad_pp(
            onerosidad, cesion_rea, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_recup_onerosidad_np(
            recup_onerosidad,
            seguimiento_rea,
            param_contab,
            excepciones,
            FECHA_VALORACION,
        ),
    ]

    input_consolidado = pl.concat(
        aux_tools.alinear_esquemas(insumos_devengo), how="diagonal"
    )
    # devuelve la base ya devengada, con las columnas de movimientos saldos y de fluctuación
    output_devengo_fluct = (
        devg.devengar(input_consolidado, FECHA_VALORACION)
        .pipe(fluc.calc_fluctuacion, tasa_cambio)
        .pipe(det.calc_deterioro, riesgo_credito, FECHA_VALORACION)
    )
    output_devengo_fluct.write_excel(p.RUTA_SALIDA_DEVENGO)

    # Insumos no devengables
    insumos_no_devengo = [
        prep_data.prep_input_cartera(cartera, param_contab, FECHA_VALORACION),
        prep_data.prep_input_cartera(cuenta_corriente, param_contab, FECHA_VALORACION),
    ]

    # convierte a output contable
    output_contable = mapcont.gen_output_contable(
        output_devengo_fluct,
        input_map_bts,
        input_tipo_seguro,
        tabla_nomenclatura,
        insumos_no_devengo,
    )
    output_contable.write_excel(p.RUTA_SALIDA_CONTABLE)

    return output_devengo_fluct, output_contable

def input_directo(ramo: str):
    df = pl.read_parquet(f"C:/Users/samuarta/Seguros Suramericana, S.A/EGVFAM - ifrs17_col/salidasdll/pruebas_gestion_tecnica/data/input_directo/{ramo}_202501.parquet")
    mapping_sociedad = {'1000': '01', '2000': '02'}
    df = (df.select(
        ["tipo_insumo", "tipo_negocio", "npoliza", "fecha_expedicion_poliza", "nrecibo", "cdgarantia", 
         "cdsubgarantia", "ncertificado", "sociedad", "cdramo_contable", "canal", "cdsubramo_recibo", 
         "operacion", "moneda_documento", "fecha_contable_documento", "feini_vigencia_recibo", 
         "fefin_vigencia_recibo", "feini_vigencia_cobertura", "fefin_vigencia_cobertura", 
         "importe_moneda_documento_dist"])
        .with_columns(df['sociedad'].replace(mapping_sociedad).alias('compania'))
        .drop('sociedad')
        .rename({"npoliza": "poliza", "nrecibo": "recibo", "cdgarantia": "amparo",
                 "ncertificado": "poliza_certificado", "cdramo_contable": "ramo_sura",
                 "cdsubramo_recibo": "producto", "operacion": "tipo_op",
                 "moneda_documento": "moneda", "fecha_contable_documento": "fecha_contabilizacion_recibo", 
                 "feini_vigencia_recibo": "fecha_inicio_vigencia_recibo",
                 "fefin_vigencia_recibo": "fecha_fin_vigencia_recibo",
                 "feini_vigencia_cobertura": "fecha_inicio_vigencia_cobertura",
                 "fefin_vigencia_cobertura": "fecha_fin_vigencia_cobertura",
                 "importe_moneda_documento_dist": "valor_prima_emitida"})
        )
    
    return df

def input_dcto_directo(ramo: str):
    df = pl.read_parquet(f"C:/Users/samuarta/Seguros Suramericana, S.A/EGVFAM - ifrs17_col/salidasdll/pruebas_gestion_tecnica/data/input_directo/{ramo}_202501.parquet")
    mapping_sociedad = {'1000': '01', '2000': '02'}
    df = (df.select([
        "sociedad", "cdramo_contable", "npoliza", "nrecibo", "cdgarantia", "cdsubgarantia", 
        "ncertificado", "podescuento_tecnico", "podescuento_comercial"
        ])
            .with_columns([
                df['sociedad'].replace(mapping_sociedad).alias('compania'),
                pl.lit(None).alias('recibo_rea'),
                pl.lit(0.0).alias('podto_tecnico_rea'),
                pl.lit(0.0).alias('podto_comercial_rea')
            ])
            .drop('sociedad')
            .rename({"cdramo_contable": "ramo_sura", "npoliza": "poliza", "nrecibo": "recibo", 
                     "cdgarantia": "amparo", "ncertificado": "poliza_certificado", 
                     "podescuento_tecnico": "podto_tecnico", "podescuento_comercial": "podto_comercial"})
        )
    return df

def output_tecnologia(ramo: str):
    ruta = r"C:/Users/samuarta/Seguros Suramericana, S.A/EGVFAM - ifrs17_col/salidasdll/pruebas_gestion_tecnica/data/output/*.parquet"
    files = glob.glob(ruta)

    lfs = []
    for f in files:
        lf_file = (
            pl.scan_parquet(f)
            .with_columns(pl.col("dias_devengados").cast(pl.Float64))
        )
        lfs.append(lf_file)

    lf = (
        pl.concat(lfs, how="vertical_relaxed")
        .filter(pl.col("ramo_sura") == ramo)
    )
    
    df = lf.collect()

    return df

def outer_join(
    df_left: pl.DataFrame, df_right: pl.DataFrame, cols_left_keep: list[str], 
    cols_right_keep: list[str], exclude_cols: list[str], col_mapping: dict[str, str], 
    suffix_right="_right"
) -> pl.DataFrame:
    # 1. Reducir dataframes
    df_left = df_left.select(cols_left_keep)
    df_right = df_right.select(cols_right_keep)

    # 2. Renombrar df_right en columnas excluidas
    df_right = (
        df_right.rename(col_mapping)
        .rename({c: c + suffix_right for c in exclude_cols})
    )

    # 4. Columnas del join = todas menos excluidas
    join_cols = [c for c in cols_left_keep if c not in exclude_cols]
    
    # 5. Hacer el join
    df_joined = df_left.join(
        df_right,
        on=join_cols,
        how="full",
        nulls_equal=True,
        validate="1:1",
    )

    # 6. Construir lista final de columnas
    final_cols = []

    for col in cols_left_keep:
        if col in exclude_cols:
            # columna original del left
            final_cols.append(col)
            # versión renombrada del right
            final_cols.append(col + suffix_right)
        else:
            final_cols.append(col)

    return df_joined.select(final_cols)


def comparar_pcr(ramo: str):
    # Lectura de insumos
    RUTA_INSUMOS = "C:/Users/samuarta/Proyectos/prototipo-pcr/prototipo_pcr/inputs/insumos - tests.xlsx"
    RUTA_GASTOS = "C:/Users/samuarta/Proyectos/prototipo-pcr/prototipo_pcr/inputs/gastos - tests.xlsx"
    
    # Insumos transversales
    param_contab = pl.read_excel(
        RUTA_INSUMOS, sheet_name=p.HOJA_PARAMETROS_CONTAB
    ).filter(
        pl.col("estado_insumo") == 1
    )  # Solo se usan las configuraciones activas (1)
    excepciones = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_EXCEPCIONES_50_50)
    gasto = pl.read_excel(RUTA_GASTOS)
    tasa_cambio = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_MONEDA)
    descuentos = input_dcto_directo(ramo=ramo)
    # El diccionario o tabla de correspondencia de outputs con entradas contables
    input_map_bts = pl.read_excel(p.RUTA_REL_BT, infer_schema_length=2000).filter(
        ~pl.col("clasificacion_adicional").is_in(["MAT", "REC"])
    )
    input_tipo_seguro = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_TIPO_SEGURO)
    tabla_nomenclatura = pl.read_excel(p.RUTA_NOMENCLATURA, sheet_name="V2")
    # Insumos de recibos contabilizados en SAP
    produccion_dir = input_directo(ramo=ramo)
    print("Insumo Producción Directo leido exitosamente")
    cesion_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_CESION)
    comision_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_COMISION_REA)
    costo_contrato_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_COSTO_CONTRATO)
    seguimiento_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_SEGUIMIENTO_REA)
    produccion_arl = pl.read_excel(p.RUTA_PRODUCCION_ARL)
    costo_contrato_arl = pl.read_excel(p.RUTA_COSTO_CONTRATO_ARL)
    # Insumos de onerosidad leidos desde el datalake
    onerosidad = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_ONEROSIDAD)
    recup_onerosidad = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_RECUP_ONEROSIDAD)
    # Insumos de riesgo de credito cargado por equipo de riesgo financiero
    riesgo_credito = pl.read_excel(p.RUTA_RIESGO_CREDITO)
    # Insumos no devengables
    cartera = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_CARTERA)
    cartera_arl = pl.read_excel(p.RUTA_CARTERA_ARL)
    cuenta_corriente = pl.read_excel(p.RUTA_CUENTA_CORRIENTE)
    cuenta_corriente_arl = pl.read_excel(p.RUTA_CUENTA_CORRIENTE_ARL)

    produccion_arl_prep = prep_data.prep_input_produccion_arl(produccion_arl)
    produccion_dir = pl.concat(
        [produccion_dir, produccion_arl_prep], how="diagonal_relaxed"
    )

    costo_contrato_rea = pl.concat(
        [costo_contrato_rea, costo_contrato_arl], how="diagonal_relaxed"
    )

    cartera = pl.concat(
        [
            cartera,
            cartera_arl.with_columns(
                fecha_expedicion_poliza=pl.col("mes_cotizacion").dt.month_start()
            ),
        ],
        how="diagonal_relaxed",
    )

    cuenta_corriente = pl.concat(
        [cuenta_corriente, cuenta_corriente_arl], how="diagonal_relaxed"
    )

    # Prepara cada insumo para entrar a devengo
    insumos_devengo = [
        # prepara insumos seguro directo
        prep_data.prep_input_prima_directo(
            produccion_dir, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_dcto_directo(
            produccion_dir, param_contab, excepciones, descuentos, FECHA_VALORACION
        ),
        prep_data.prep_input_gasto_directo(
            produccion_dir, param_contab, excepciones, gasto, FECHA_VALORACION
        ),
        prep_data.prep_input_onerosidad(onerosidad, param_contab, FECHA_VALORACION),
        # prepara insumos reaseguro
        prep_data.prep_input_prima_rea(
            cesion_rea, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_dcto_rea(
            cesion_rea, param_contab, excepciones, descuentos, FECHA_VALORACION
        ),
        prep_data.prep_input_gasto_rea(
            cesion_rea, param_contab, excepciones, gasto, FECHA_VALORACION
        ),
        prep_data.prep_input_comi_rea(
            comision_rea, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_costo_con(
            costo_contrato_rea,
            seguimiento_rea,
            param_contab,
            excepciones,
            FECHA_VALORACION,
        ),
        prep_data.prep_input_recup_onerosidad_pp(
            onerosidad, cesion_rea, param_contab, excepciones, FECHA_VALORACION
        ),
        prep_data.prep_input_recup_onerosidad_np(
            recup_onerosidad,
            seguimiento_rea,
            param_contab,
            excepciones,
            FECHA_VALORACION,
        ),
    ]

    input_consolidado = pl.concat(
        aux_tools.alinear_esquemas(insumos_devengo), how="diagonal"
    )
    # devuelve la base ya devengada, con las columnas de movimientos saldos y de fluctuación
    output_devengo_fluct = (
        devg.devengar(input_consolidado, FECHA_VALORACION)
        .pipe(fluc.calc_fluctuacion, tasa_cambio)
        .pipe(det.calc_deterioro, riesgo_credito, FECHA_VALORACION)
    )
    #output_devengo_fluct.write_excel(p.RUTA_SALIDA_DEVENGO)

    # Insumos no devengables
    insumos_no_devengo = [
        prep_data.prep_input_cartera(cartera, param_contab, FECHA_VALORACION),
        prep_data.prep_input_cartera(cuenta_corriente, param_contab, FECHA_VALORACION),
    ]

    # convierte a output contable
    excluir = ["-1", "" ,"124324091"]
    output_contable = mapcont.gen_output_contable(
        output_devengo_fluct,
        input_map_bts,
        input_tipo_seguro,
        tabla_nomenclatura,
        insumos_no_devengo,
    ).filter(
        (~pl.col("poliza").cast(pl.Utf8).is_in(excluir)) & pl.col("poliza").is_not_null()
    )
    #path_contable = p.RUTA_SALIDA_CONTABLE
    #output_contable.write_excel(path_contable
    #                            .with_name(f"{path_contable.stem}_{ramo}{path_contable.suffix}"))
    df_tecnologia = output_tecnologia(ramo=ramo)
    cols_left_keep = [
        'tipo_insumo', 'tipo_negocio', 'poliza', 'fecha_expedicion_poliza', 'recibo', 
        'amparo', 'cdsubgarantia', 'poliza_certificado', 'ramo_sura', 'canal', 'producto', 'moneda',
        'tipo_op', 'fecha_contabilizacion_recibo', 'fecha_inicio_vigencia_recibo', 
        'fecha_fin_vigencia_recibo', 'fecha_inicio_vigencia_cobertura', 
        'fecha_fin_vigencia_cobertura', 'valor_md', 'valor_ml', 'tipo_movimiento_codigo', 
        'indicativo_periodo_movimiento_codigo', 'concepto_codigo', 
        'clasificacion_adicional_codigo', 'tipo_negocio_codigo', 'tipo_reaseguro_codigo', 
        'tipo_reasegurador_codigo', 'tipo_contabilidad_codigo', 'transicion_codigo', 
        'naturaleza', 'bt', 'descripcion_bt'
    ]

    cols_right_keep = [
        'tipo_insumo', 'tipo_negocio', 'poliza', 'fecha_expedicion_poliza', 'recibo', 'garantia', 
        'subgarantia', 'poliza_certificado', 'ramo_sura', 'canal', 'producto', 'moneda_documento', 
        'tipo_op', 'fecha_contabilizacion_recibo', 'fecha_inicio_vigencia_recibo', 
        'fecha_fin_vigencia_recibo', 'fecha_inicio_vigencia_cobertura', 
        'fecha_fin_vigencia_cobertura', 'valor_md', 'valor_ml', 'tipo_movimiento_codigo', 
        'indicativo_periodo_movimiento_codigo', 'concepto_codigo', 'clasificacion_adicional_codigo',
        'tipo_negocio_codigo', 'tipo_reaseguro_codigo', 'tipo_reasegurador_codigo', 'tipo_contabilidad', 
        'transicion', 'naturaleza', 'bt', 'descripcion_bt'
    ]

    col_mapping = {
        "tipo_contabilidad": "tipo_contabilidad_codigo",
        "transicion": "transicion_codigo",
        "garantia": "amparo",
        "subgarantia": "cdsubgarantia",
        "moneda_documento": "moneda"
    }

    exclude_cols = ['valor_md', 'valor_ml']

    df_join = outer_join(
        output_contable, df_tecnologia, cols_left_keep, 
        cols_right_keep, exclude_cols, col_mapping,
        suffix_right="_Tecnología"
    )
    df_result = (
        df_join
        .with_columns([
            (pl.col("valor_md") - pl.col("valor_md_Tecnología")).alias("diff_md"),
            (pl.col("valor_ml") - pl.col("valor_ml_Tecnología")).alias("diff_ml"),
            ( (pl.col("valor_md") - pl.col("valor_md_Tecnología")).abs() > 1e-3 )
            .cast(pl.Int8)
            .alias("alerta_md"),
            ( (pl.col("valor_ml") - pl.col("valor_ml_Tecnología")).abs() > 1e-3 )
            .cast(pl.Int8)
            .alias("alerta_ml"),
            ])
    )
    resumen = df_result.select(
        pl.col("alerta_md").mean().alias("Tasa_alerta_md"),
        pl.col("alerta_ml").mean().alias("Tasa_alerta_ml")
    )
    print(resumen)
    return output_devengo_fluct, output_contable

if __name__ == "__main__":
    comparar_pcr("139")