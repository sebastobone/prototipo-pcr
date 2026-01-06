from datetime import date

import polars as pl
from src import mapeo_contable as mapcont
import pytest


@pytest.mark.parametrize(
    "fe_valoracion, marcas_esperadas",
    [
        (date(2024, 1, 31), ["OO", "NO"]),
        (date(2024, 2, 29), ["OO", "NO"]),
        (date(2024, 3, 31), ["NO", "NO"]),
    ],
)
def test_agregar_marca_onerosidad(fe_valoracion: date, marcas_esperadas: list[pl.Expr]):
    output_contable_simplificado = pl.DataFrame(
        {
            "compania": ["02", "02"],
            "ramo_sura": ["083", "084"],
            "poliza": ["083001", "084001"],
        }
    )
    onerosidad = pl.DataFrame(
        {
            "compania": ["02", "02", "02"],
            "ramo_sura": ["083", "083", "083"],
            "poliza": ["083001", "083001", "083001"],
            "fecha_calculo_onerosidad": [
                date(2024, 1, 31),
                date(2024, 2, 29),
                date(2024, 3, 31),
            ],
            "valor_prima_emitida": [1200, 600, -1800],
        }
    )

    resultado = mapcont.agregar_marca_onerosidad(
        output_contable_simplificado, onerosidad, fe_valoracion
    )

    resultado_esperado = output_contable_simplificado.with_columns(
        pl.Series("onerosidad", marcas_esperadas)
    )

    assert resultado.equals(resultado_esperado)
