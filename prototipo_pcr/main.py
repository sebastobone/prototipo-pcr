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
from math import ceil
import gc


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
    df = (pl.read_parquet(f"C:/Users/samuarta/Seguros Suramericana, S.A/EGVFAM - ifrs17_col/salidasdll/pruebitas_gestion_tecnica/data/input_directo/{ramo}_202501.parquet")
          .with_columns(pl.col('npoliza').cast(pl.Utf8))
    
    )
    mapping_sociedad = {'1000': '01', '2000': '02'}
    df = (df.select(
        ["tipo_insumo", "tipo_negocio", "npoliza", "fecha_expedicion_poliza", "nrecibo", "cdgarantia", 
         "cdsubgarantia", "ncertificado", "numero_documento_contable", "sociedad", "cdramo_contable", 
         "canal", "cdsubramo_recibo", "operacion", "moneda_documento", "fecha_contable_documento", 
         "feini_vigencia_recibo", "fefin_vigencia_recibo", "feini_vigencia_cobertura",
         "fefin_vigencia_cobertura", "importe_moneda_documento_dist"])
        .with_columns(df['sociedad'].replace(mapping_sociedad).alias('compania'))
        .drop('sociedad')
        .rename({"npoliza": "poliza", "nrecibo": "recibo", "cdgarantia": "amparo",
                 "ncertificado": "poliza_certificado", "cdramo_contable": "ramo_sura",
                 "cdsubramo_recibo": "producto", "operacion": "tipo_op", 'numero_documento_contable': 'numero_documento_sap',
                 "moneda_documento": "moneda", "fecha_contable_documento": "fecha_contabilizacion_recibo", 
                 "feini_vigencia_recibo": "fecha_inicio_vigencia_recibo",
                 "fefin_vigencia_recibo": "fecha_fin_vigencia_recibo",
                 "feini_vigencia_cobertura": "fecha_inicio_vigencia_cobertura",
                 "fefin_vigencia_cobertura": "fecha_fin_vigencia_cobertura",
                 "importe_moneda_documento_dist": "valor_prima_emitida"})
        )
    
    return df

def input_dcto_directo(ramo: str):
    df = pl.read_parquet(f"C:/Users/samuarta/Seguros Suramericana, S.A/EGVFAM - ifrs17_col/salidasdll/pruebitas_gestion_tecnica/data/input_directo/{ramo}_202501.parquet")
    mapping_sociedad = {'1000': '01', '2000': '02'}
    df = (df.select([
        "sociedad", "cdramo_contable", "npoliza", "nrecibo", "cdgarantia", "cdsubgarantia", 
        "ncertificado", "numero_documento_contable", "canal", "cdsubramo_recibo", "operacion",
        "podescuento_tecnico", "podescuento_comercial"
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
                     "numero_documento_contable": "numero_documento_sap", "cdsubramo_recibo": "producto",
                     "operacion": "tipo_op", "podescuento_tecnico": "podto_tecnico", 
                     "podescuento_comercial": "podto_comercial"})
        )
    return df

def output_tecnologia(ramo: str, polizas: list[str], recibos: list[str]):
    ruta = fr"C:/Users/samuarta/Seguros Suramericana, S.A/EGVFAM - ifrs17_col/salidasdll/pruebitas_gestion_tecnica/data/output/{ramo}_*202501.parquet"
    files = glob.glob(ruta)

    lfs = []
    for f in files:
        lf_file = (
            pl.scan_parquet(f)
            .filter(pl.col("ramo_sura") == ramo)
            .filter((pl.col("poliza").is_in(polizas.implode())) & (pl.col('recibo').is_in(recibos.implode())))
            .with_columns(pl.col("dias_devengados").cast(pl.Float64))
        )
        lfs.append(lf_file)

    lf = pl.concat(lfs, how="vertical_relaxed")
    df = lf.collect()
    return df


def outer_join(
    df_left: pl.DataFrame, df_right: pl.DataFrame, cols_left_keep: list[str], 
    cols_right_keep: list[str], exclude_cols: list[str], col_mapping: dict[str, str], 
    suffix_right="_right", type="full"
) -> pl.DataFrame:
    # 1. Reducir dataframes
    df_left = df_left.select(cols_left_keep)
    df_right = df_right.select(cols_right_keep)

    # 2. Renombrar df_right en columnas excluidas
    df_right = (
        df_right.rename(col_mapping)
        .rename({c: c + suffix_right for c in exclude_cols})
    )

    # 3. Columnas del join = todas menos excluidas
    join_cols = [c for c in cols_left_keep if c not in exclude_cols]
    
    # 4. Hacer el join
    df_joined = df_left.join(
        df_right,
        on=join_cols,
        how=type,
        nulls_equal=True,
    )

    # 5. Construir lista final de columnas
    final_cols = []

    for col in cols_left_keep:
        # columna original del left
        final_cols.append(col)
        if col in exclude_cols:
            # versión renombrada del right
            final_cols.append(col + suffix_right)

    return df_joined.fill_null(0)

def comparar_pcr_chunked(
        ramo: str, chunk_size: int, produccion_dir: pl.DataFrame, descuentos: pl.DataFrame, param_contab,
        excepciones, gasto, onerosidad, cesion_rea, comision_rea, costo_contrato_rea, seguimiento_rea,
        recup_onerosidad, cartera, cuenta_corriente, tasa_cambio, riesgo_credito, input_map_bts, 
        input_tipo_seguro, tabla_nomenclatura, FECHA_VALORACION: str
):
    total_registros = produccion_dir.height
    num_chunks = ceil(total_registros / chunk_size)
    print(f"Procesando {total_registros} registros del Ramo {ramo}, en {num_chunks} chunks de {chunk_size} filas...")

    cols_left_keep = [
        'tipo_insumo', 'tipo_negocio', 'poliza','fecha_expedicion_poliza', 
        'recibo', 'amparo', 'cdsubgarantia', 'poliza_certificado', 'ramo_sura', 'canal', 'producto', 
        'moneda', 'tipo_op', 'numero_documento_sap', 'fecha_inicio_vigencia_recibo', 'fecha_contabilizacion_recibo',
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
        'tipo_op', 'numero_documento_sap', 'fecha_inicio_vigencia_recibo', 'fecha_contabilizacion_recibo',
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

    exclude_cols = ['valor_md', 'valor_ml', 'tipo_negocio', "tipo_negocio_codigo"]
    registros_output = 0
    resultados_duplicados = []
    resultados_join = []
    resumenes = []

    for i in range(num_chunks):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, total_registros)
        print(f"\n Chunk {i+1}/{num_chunks}: filas {start} a {end} ")

        # Filtrar bloque de producción
        chunk_prod = produccion_dir.slice(start, end - start)

        # Filtrar descuentos por polizas del bloque
        polizas_chunk = chunk_prod.get_column("poliza").unique()
        recibos_chunk = chunk_prod.get_column("recibo").unique()
        chunk_desc = descuentos.filter((pl.col("poliza").is_in(polizas_chunk.implode())) & (pl.col('recibo').is_in(recibos_chunk.implode())))

        # --- Prepara insumos para devengo ---
        insumos_devengo = [
            prep_data.prep_input_prima_directo(chunk_prod, param_contab, excepciones, FECHA_VALORACION),
            prep_data.prep_input_dcto_directo(chunk_prod, param_contab, excepciones, chunk_desc, FECHA_VALORACION),
            prep_data.prep_input_gasto_directo(chunk_prod, param_contab, excepciones, gasto, FECHA_VALORACION),
        ]
        
        input_consolidado = pl.concat(aux_tools.alinear_esquemas(insumos_devengo), how="diagonal")

        # --- Devengo + fluctuación ---
        output_devengo_fluct = (
            devg.devengar(input_consolidado, FECHA_VALORACION)
            .pipe(fluc.calc_fluctuacion, tasa_cambio)
        )

        # --- Insumos no devengables ---
        insumos_no_devengo = [
            prep_data.prep_input_cartera(cartera, param_contab, FECHA_VALORACION),
            prep_data.prep_input_cartera(cuenta_corriente, param_contab, FECHA_VALORACION),
        ]

        # --- Output contable ---
        excluir = ["-1", "", "124324091"]
        output_contable = mapcont.gen_output_contable(
            output_devengo_fluct,
            input_map_bts,
            input_tipo_seguro,
            tabla_nomenclatura,
            insumos_no_devengo,
        ).filter(
            (~pl.col("poliza").cast(pl.Utf8).is_in(excluir)) & pl.col("poliza").is_not_null()
        )
        #output_contable.write_clipboard()
        registros_output += len(output_contable)
        print(output_contable.is_duplicated().sum())
        #resultados_contables.append(output_contable)

        # --- Join con tecnología ---
        df_tecnologia = output_tecnologia(ramo=ramo, polizas=polizas_chunk, recibos=recibos_chunk)
        print(df_tecnologia.is_duplicated().sum(), len(df_tecnologia))
        resultados_duplicados.append(df_tecnologia.filter(df_tecnologia.is_duplicated()))

        df_join = outer_join(
            output_contable, df_tecnologia, cols_left_keep, 
            cols_right_keep, exclude_cols, col_mapping,
            suffix_right="_Tecnología", type="full"
        )

        df_result = (
            df_join
            .with_columns([
                (pl.col("valor_md") - pl.col("valor_md_Tecnología")).alias("diff_md"),
                (pl.col("valor_ml") - pl.col("valor_ml_Tecnología")).alias("diff_ml"),
                ((pl.col("valor_md") - pl.col("valor_md_Tecnología")).abs() > 1e-6).cast(pl.Int8).alias("alerta_md"),
                ((pl.col("valor_ml") - pl.col("valor_ml_Tecnología")).abs() > 1e-6).cast(pl.Int8).alias("alerta_ml"),
            ])
        )

        resumen = df_result.select(
            pl.col("alerta_md").mean().alias("tasa_alerta_md"),
            pl.col("alerta_md").sum().alias("cantidad_alerta_md"),
            pl.col("alerta_ml").mean().alias("tasa_alerta_ml"),
            pl.col("alerta_ml").sum().alias("cantidad_alerta_ml")
        )
        resumenes.append(resumen)
        resultados_join.append(df_result)#.filter((pl.col('alerta_md') > 0) | (pl.col('alerta_ml') > 0)))
        
        del chunk_prod, chunk_desc, input_consolidado, output_devengo_fluct, 
        del output_contable, df_tecnologia, df_join, df_result, resumen
        gc.collect()


    # Concatenar resultados finales
    #output_contable_final = pl.concat(resultados_contables, how="diagonal_relaxed")
    df_join_final = pl.concat(resultados_join, how="diagonal_relaxed")
    resumen_final = pl.concat(resumenes, how="diagonal_relaxed")
    print(resumen_final.to_pandas())
    print(registros_output)
    return resumen_final, df_join_final

def comparar_pcr(ramo: str, chunk_size: int = 50000):
    # --- Lectura de insumos ---
    RUTA_INSUMOS = "C:/Users/samuarta/Proyectos/prototipo-pcr/prototipo_pcr/inputs/insumos - tests.xlsx"
    RUTA_GASTOS = "C:/Users/samuarta/Proyectos/prototipo-pcr/prototipo_pcr/inputs/gastos - tests.xlsx"

    param_contab = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_PARAMETROS_CONTAB).filter(pl.col("estado_insumo") == 1)
    excepciones = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_EXCEPCIONES_50_50)

    gasto = pl.read_excel(RUTA_GASTOS, infer_schema_length=5000)
    tasa_cambio = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_MONEDA)
    
    input_map_bts = pl.read_excel(p.RUTA_REL_BT, infer_schema_length=2000).filter(~pl.col("clasificacion_adicional").is_in(["MAT", "REC"]))
    input_tipo_seguro = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_TIPO_SEGURO)
    tabla_nomenclatura = pl.read_excel(p.RUTA_NOMENCLATURA, sheet_name="V2")
    produccion_dir = input_directo(ramo=ramo)
    descuentos = input_dcto_directo(ramo=ramo)

    cesion_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_CESION)
    comision_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_COMISION_REA)
    costo_contrato_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_COSTO_CONTRATO)
    seguimiento_rea = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_SEGUIMIENTO_REA)
    onerosidad = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_ONEROSIDAD)
    recup_onerosidad = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_RECUP_ONEROSIDAD)
    riesgo_credito = pl.read_excel(p.RUTA_RIESGO_CREDITO)
    cartera = pl.read_excel(RUTA_INSUMOS, sheet_name=p.HOJA_CARTERA)
    cuenta_corriente = pl.read_excel(p.RUTA_CUENTA_CORRIENTE)

    # Llamada al procesamiento por bloques (chunked) 
    return comparar_pcr_chunked(
        ramo=ramo, chunk_size=chunk_size, produccion_dir=produccion_dir, descuentos=descuentos,
        param_contab=param_contab, excepciones=excepciones, gasto=gasto, onerosidad=onerosidad,
        cesion_rea=cesion_rea, comision_rea=comision_rea, costo_contrato_rea=costo_contrato_rea,
        seguimiento_rea=seguimiento_rea, recup_onerosidad=recup_onerosidad, cartera=cartera,
        cuenta_corriente=cuenta_corriente, tasa_cambio=tasa_cambio, riesgo_credito=riesgo_credito, 
        input_map_bts=input_map_bts, input_tipo_seguro=input_tipo_seguro, 
        tabla_nomenclatura=tabla_nomenclatura, FECHA_VALORACION=FECHA_VALORACION
    )

if __name__ == "__main__":
    # Procesar un ramo por bloques (chunks)
    
    tasa_alertas, registros_diferencia = comparar_pcr("183", chunk_size=30000)
    #registros_diferencia.write_clipboard()
    """
    import pandas as pd 
    import polars as pl
    ramos_no_procesables = [
        '025', '040', '081', '093', '190', '191', '193', '196' 
        ]
    # Procesar todos los ramos
    ramos_pequenos_25000 = [
        '003', '006', '007', '009', '010', '011', '015', '017', '019', '020', '021',
        '024', '032', '033', '034', '039', '069', '085', '092', '095', '096', '109', 
        '132', '133', '134', '139', '181'
        ]
    ramos_grandes = [
        '012', '013', '028', '030', '031', '041', '083', '084', '086', '090', '091', 
        '128', '130', '183', 'AAV'
        ]
    resumen_list = []
    diferencias_dict = {}

    for ramo in ramos_grandes:
        try:
            tasa_alertas, registros_diferencia = comparar_pcr(ramo, chunk_size=40000)

            # Agregar columna ramo
            tasa_alertas = tasa_alertas.with_columns(pl.lit(ramo).alias("ramo"))
            resumen_list.append(tasa_alertas)

            # Guardar primeros N registros
            diferencias_dict[ramo] = registros_diferencia.filter(pl.col('alerta_md') == 1).head(30000)

        except Exception as e:
            print(f"Error en ramo {ramo}: {e}")

        # Concatenar todos los tasa_alertas
        resumen_df = pl.concat(resumen_list)

        # Exportar a Excel
        with pd.ExcelWriter(r"C:/Users/samuarta/Proyectos/prototipo-pcr/prototipo_pcr/output/Resultados_Comparacion_Ramos_Grandes.xlsx", engine="xlsxwriter") as writer:
            resumen_df.to_pandas().to_excel(writer, sheet_name="Resumen", index=False)

            for ramo, df in diferencias_dict.items():
                df.to_pandas().to_excel(writer, sheet_name=str(ramo)[:31], index=False)
    """