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
