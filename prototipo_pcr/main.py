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


# lee los parámetros e insumos relevantes
FECHA_VALORACION = p.FECHA_VALORACION
FECHA_VALORACION_ANTERIOR = p.FECHA_VALORACION_ANTERIOR
FECHA_TRANSICION = p.FECHA_TRANSICION


def run_pcr(fe_valoracion):
    # Lectura de insumos
    # Insumos transversales
    param_contab = pl.read_excel(
        p.RUTA_INSUMOS, sheet_name=p.HOJA_PARAMETROS_CONTAB
    ).filter(
        pl.col("estado_insumo") == 1
    )  # Solo se usan las configuraciones activas (1)
    excepciones = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_EXCEPCIONES_50_50)
    gasto = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_GASTO)
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
    output_contable.write_clipboard()
    output_contable.write_excel(p.RUTA_SALIDA_CONTABLE)

    return output_devengo_fluct, output_contable


if __name__ == "__main__":
    run_pcr(FECHA_VALORACION)
