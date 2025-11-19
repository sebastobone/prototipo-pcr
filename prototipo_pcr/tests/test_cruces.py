import polars as pl
from src import cruces
import pytest


@pytest.mark.parametrize(
    "canal, producto, porcentajes_esperados",
    [
        ("sucursal", "plan_basico", [0.1, 0.9]),
        ("sucursal", "*", [0.2, 0.8]),
        ("*", "*", [0.3, 0.7]),
    ],
)
def test_cruzar_gastos_expedicion(
    canal: str, producto: str, porcentajes_esperados: list[float]
):
    produccion = pl.DataFrame(
        {
            "tipo_insumo": ["produccion_directo"],
            "tipo_negocio": ["directo"],
            "poliza": [1],
            "fecha_expedicion_poliza": ["2023-01-30"],
            "recibo": [1],
            "amparo": ["DANOS"],
            "poliza_certificado": [1],
            "compania": ["01"],
            "ramo_sura": ["040"],
            "canal": ["sucursal"],
            "producto": ["plan_basico"],
            "tipo_op": ["emision"],
            "moneda": ["COP"],
            "fecha_contabilizacion": ["2023-01-30"],
            "fecha_inicio_vigencia_recibo": ["2023-01-30"],
            "fecha_fin_vigencia_recibo": ["2023-12-31"],
            "fecha_inicio_vigencia_cobertura": ["2023-01-30"],
            "fecha_fin_vigencia_cobertura": ["2023-12-31"],
            "valor_prima_emitida": [1200],
        }
    )
    gastos = pl.DataFrame(
        {
            "fecha_clave": ["2023-12-31", "2023-12-31"],
            "fecha_inicio": ["2023-01-01", "2023-01-01"],
            "fecha_fin": ["2023-12-31", "2023-12-31"],
            "compania": ["01", "01"],
            "ramo_sura": ["040", "040"],
            "canal": [canal, canal],
            "producto": [producto, producto],
            "tipo_gasto": ["expedicion_comisiones", "expedicion_otros"],
            "real_estimado": ["estimado", "estimado"],
            "porc_gasto": porcentajes_esperados,
        }
    )

    resultado = cruces.cruzar_gastos_expedicion(produccion, gastos)
    assert resultado.shape[0] == 2
    assert sorted(resultado.get_column("porc_gasto").to_list()) == porcentajes_esperados
