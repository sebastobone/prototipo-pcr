import polars as pl
from devenga import conftest as dev_cf
import conftest as cf

from src import aux_tools
from src import prep_insumo as prep_data
from src import devenga as devg
from src import fluctuacion as fluc
from src import mapeo_contable as mapcont
from src import parametros as p

import pytest


@pytest.mark.parametrize(
    # 2026-01-08: Por ahora no ensayamos reaseguro por dudas con las BTs existentes.
    "transicion, bts_niif17l, bts_niif17c",
    [
        (
            True,
            {
                "ZT1096",  # Saldo de primas matematica
                "ZT3105",  # Saldo de gasto comisiones matematica
                "ZT3106",  # Saldo de gasto otros matematica
            },
            {
                "ZT1099",  # Saldo de primas
                "ZT3100",  # Saldo de gasto comisiones
                "ZT3101",  # Saldo de gasto otros
            },
        ),
        (
            False,
            {
                "ZC1096",  # Constitucion de primas matematica
                "ZC3105",  # Constitucion de gasto comisiones matematica
                "ZC3106",  # Constitucion de gasto otros matematica
                "ZD1096",  # Devengo de primas matematica
                "ZD3105",  # Devengo de gasto comisiones matematica
                "ZD3106",  # Devengo de gasto otros matematica
            },
            {
                "ZC1099",  # Constitucion de primas
                "ZC3100",  # Constitucion de gasto comisiones
                "ZC3101",  # Constitucion de gasto otros
                "ZD1099",  # Devengo de primas ano actual
                "ZD3100",  # Devengo de gasto comisiones ano actual
                "ZD3101",  # Devengo de gasto otros ano actual
            },
        ),
    ],
)
def test_ramos_matematica(
    param_contabilidad: pl.DataFrame,
    excepciones_df: pl.DataFrame,
    transicion: bool,
    bts_niif17l: set[str],
    bts_niif17c: set[str],
):
    fecha_valoracion = (
        p.FECHA_TRANSICION if transicion else cf.sumar_meses(p.FECHA_TRANSICION, 1)
    )

    fechas = dev_cf.Fechas(
        fecha_valoracion=fecha_valoracion,
        fecha_expedicion_poliza=fecha_valoracion,
        fecha_contabilizacion_recibo=fecha_valoracion,
        fecha_inicio_vigencia_recibo=fecha_valoracion,
        fecha_fin_vigencia_recibo=cf.sumar_meses(fecha_valoracion, 12),
        fecha_inicio_vigencia_cobertura=fecha_valoracion,
        fecha_fin_vigencia_cobertura=cf.sumar_meses(fecha_valoracion, 12),
    )

    produccion_dir = pl.DataFrame(
        {
            "tipo_insumo": ["matematica_local_produccion_directo"],
            "tipo_negocio": ["directo"],
            "compania": ["02"],
            "ramo_sura": ["095"],
            "canal": ["*"],
            "producto": ["*"],
            "tipo_op": ["01"],
            "poliza": ["095001"],
            "poliza_certificado": ["095001"],
            "recibo": ["01"],
            "amparo": ["BASICO"],
            "cdsubgarantia": ["BASICO"],
            "numero_documento_sap": ["01"],
            "tipo_reasegurador": ["no_aplica"],
            "fecha_expedicion_poliza": [fechas.fecha_expedicion_poliza],
            "fecha_contabilizacion_recibo": [fechas.fecha_contabilizacion_recibo],
            "fecha_inicio_vigencia_recibo": [fechas.fecha_inicio_vigencia_recibo],
            "fecha_fin_vigencia_recibo": [fechas.fecha_fin_vigencia_recibo],
            "fecha_inicio_vigencia_cobertura": [fechas.fecha_inicio_vigencia_cobertura],
            "fecha_fin_vigencia_cobertura": [fechas.fecha_fin_vigencia_cobertura],
            "fecha_valoracion": [fechas.fecha_valoracion],
            "moneda": ["COP"],
            "valor_prima_emitida": [1200],
        }
    )

    # cesion_rea = pl.DataFrame(
    #     {
    #         "tipo_insumo": ["matematica_local_cesion_rea_prop"],
    #         "tipo_negocio": ["mantenido"],
    #         "compania": ["02"],
    #         "ramo_sura": ["095"],
    #         "canal": ["*"],
    #         "producto": ["*"],
    #         "tipo_op": ["01"],
    #         "poliza": ["095001"],
    #         "poliza_certificado": ["095001"],
    #         "recibo": ["01"],
    #         "amparo": ["BASICO"],
    #         "cdsubgarantia": ["BASICO"],
    #         "numero_documento_sap": ["01"],
    #         "tipo_reasegurador": ["ext"],
    #         "fecha_expedicion_poliza": [fechas.fecha_expedicion_poliza],
    #         "fecha_contabilizacion_recibo": [fechas.fecha_contabilizacion_recibo],
    #         "fecha_inicio_vigencia_recibo": [fechas.fecha_inicio_vigencia_recibo],
    #         "fecha_fin_vigencia_recibo": [fechas.fecha_fin_vigencia_recibo],
    #         "fecha_inicio_vigencia_cobertura": [fechas.fecha_inicio_vigencia_cobertura],
    #         "fecha_fin_vigencia_cobertura": [fechas.fecha_fin_vigencia_cobertura],
    #         "fecha_valoracion": [fechas.fecha_valoracion],
    #         "moneda": ["COP"],
    #         "valor_prima_cedida": [600],
    #     }
    # )

    gasto = pl.DataFrame(
        {
            "fecha_clave": [fecha_valoracion, fecha_valoracion],
            "fecha_inicio": [fecha_valoracion, fecha_valoracion],
            "fecha_fin": [
                cf.sumar_meses(fecha_valoracion, 12),
                cf.sumar_meses(fecha_valoracion, 12),
            ],
            "periodo": ["*", "*"],
            "compania": ["02", "02"],
            "ramo_sura": ["095", "095"],
            "canal": ["*", "*"],
            "producto": ["*", "*"],
            "tipo_gasto": ["expedicion_otros", "expedicion_comisiones"],
            "real_estimado": ["estimado", "estimado"],
            "porc_gasto": [0.1, 0.1],
        }
    ).join(
        pl.DataFrame(
            {"tipo_contabilidad": ["ifrs4", "ifrs17_local", "ifrs17_corporativo"]}
        ),
        how="cross",
    )

    # Prepara cada insumo para entrar a devengo
    insumos_devengo = [
        # prepara insumos seguro directo
        prep_data.prep_input_prima_directo(
            produccion_dir, param_contabilidad, excepciones_df, fechas.fecha_valoracion
        ),
        prep_data.prep_input_gasto_directo(
            produccion_dir,
            param_contabilidad,
            excepciones_df,
            gasto,
            fechas.fecha_valoracion,
        ),
        # prepara insumos reaseguro
        # prep_data.prep_input_prima_rea(
        #     cesion_rea, param_contabilidad, excepciones_df, fechas.fecha_valoracion
        # ),
        # prep_data.prep_input_gasto_rea(
        #     cesion_rea,
        #     param_contabilidad,
        #     excepciones_df,
        #     gasto,
        #     fechas.fecha_valoracion,
        # ),
    ]

    input_consolidado = pl.concat(
        aux_tools.alinear_esquemas(insumos_devengo), how="diagonal"
    )

    tasa_cambio = pl.DataFrame(
        {
            "fecha": [cf.sumar_meses(fecha_valoracion, -1), fecha_valoracion],
            "moneda_origen": ["USD", "USD"],
            "moneda_destino": ["COP", "COP"],
            "tasa_cambio": [4000, 4100],
        }
    )

    output_devengo_fluct = devg.devengar(
        input_consolidado, fechas.fecha_valoracion
    ).pipe(fluc.calc_fluctuacion, tasa_cambio)

    input_map_bts = pl.read_excel(p.RUTA_REL_BT, infer_schema_length=2000)
    input_tipo_seguro = pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_TIPO_SEGURO)
    tabla_nomenclatura = pl.read_excel(p.RUTA_NOMENCLATURA, sheet_name="V2")

    output_contable = mapcont.gen_output_contable(
        output_devengo_fluct,
        input_map_bts,
        input_tipo_seguro,
        tabla_nomenclatura,
        [pl.DataFrame()],
    ).filter(pl.col("bt").is_not_null())

    niif17c = output_contable.filter(
        pl.col("tipo_contabilidad") == "ifrs17_corporativo"
    )
    niif17l = output_contable.filter(pl.col("tipo_contabilidad") == "ifrs17_local")

    assert set(niif17c.get_column("bt").to_list()) == bts_niif17c
    assert set(niif17l.get_column("bt").to_list()) == bts_niif17l
