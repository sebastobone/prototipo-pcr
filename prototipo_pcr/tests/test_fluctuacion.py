from datetime import date
from tests.devenga import conftest as cf
from src import devenga, prep_insumo, fluctuacion
import polars as pl
import pytest


@pytest.mark.parametrize("fecha_valoracion", [date(2025, 1, 31), date(2025, 2, 28)])
def test_fluctuacion(
    param_contabilidad: pl.DataFrame,
    excepciones_df: pl.DataFrame,
    fecha_valoracion: date,
):
    fechas = cf.Fechas(
        fecha_valoracion=fecha_valoracion,
        fecha_expedicion_poliza=date(2025, 1, 1),
        fecha_contabilizacion_recibo=date(2025, 1, 15),
        fecha_inicio_vigencia_recibo=date(2025, 1, 1),
        fecha_fin_vigencia_recibo=date(2025, 12, 31),
        fecha_inicio_vigencia_cobertura=date(2025, 1, 1),
        fecha_fin_vigencia_cobertura=date(2025, 12, 31),
    )
    prima = 1200

    df = cf.crear_input_devengo(
        fechas, "produccion_directo", "directo", prima, "USD"
    ).pipe(
        prep_insumo.prep_input_prima_directo,
        param_contabilidad,
        excepciones_df,
        fechas.fecha_valoracion,
    )

    tasas_cambio = pl.DataFrame(
        {
            "fecha": [
                date(2024, 12, 31),
                date(2025, 1, 15),
                date(2025, 1, 31),
                date(2025, 2, 28),
            ],
            "moneda_origen": ["USD", "USD", "USD", "USD"],
            "moneda_destino": ["COP", "COP", "COP", "COP"],
            "tasa_cambio": [3900, 4050, 4100, 4200],
        }
    )

    if fecha_valoracion.month == 1:
        delta_tasa = 4100 - 4050
        tasa_fecha_valoracion = 4100
    else:
        delta_tasa = 4200 - 4100
        tasa_fecha_valoracion = 4200

    df_resultado = devenga.devengar(df, fechas.fecha_valoracion).pipe(
        fluctuacion.calc_fluctuacion, tasas_cambio
    )

    saldo = df_resultado.get_column("saldo").item(0)
    liberacion = df_resultado.get_column("valor_liberacion").item(0)

    constitucion_esperada = saldo * delta_tasa / tasa_fecha_valoracion
    constitucion_real = df_resultado.get_column("fluctuacion_constitucion").item(0)

    liberacion_esperada = -liberacion * delta_tasa / tasa_fecha_valoracion
    liberacion_real = df_resultado.get_column("fluctuacion_liberacion").item(0)

    assert constitucion_esperada == constitucion_real
    assert liberacion_esperada == liberacion_real
