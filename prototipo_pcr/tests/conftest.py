from datetime import date
import calendar
import sys
import os
import pytest
import polars as pl
from prototipo_pcr.src import parametros as p

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../prototipo_pcr"))
)


@pytest.fixture
def param_contabilidad() -> pl.DataFrame:
    return pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_PARAMETROS_CONTAB).filter(
        pl.col("estado_insumo") == 1
    )


@pytest.fixture
def excepciones_df() -> pl.DataFrame:
    return pl.read_excel(p.RUTA_INSUMOS, sheet_name=p.HOJA_EXCEPCIONES_50_50)


def sumar_meses(fecha: date, numero_meses: int) -> date:
    meses_totales = fecha.year * 12 + fecha.month - 1 + numero_meses

    anno = meses_totales // 12
    mes = meses_totales % 12 + 1

    ultimo_dia = calendar.monthrange(anno, mes)[1]
    dia = min(fecha.day, ultimo_dia)

    return date(anno, mes, dia)
