import polars as pl

df = pl.read_parquet("prototipo_pcr/tests/data/CS_090_202501.parquet").sort("nrecibo")

dias_vigencia = (
    pl.col("fefin_vigencia_cobertura") - pl.col("feini_vigencia_cobertura")
).dt.total_days() + 1

# Produccion anticipada
df.filter(
    (pl.col("fecont_pdcion") < pl.col("feini_vigencia_cobertura"))
    & (dias_vigencia > 32)
    & (pl.col("ptcobro_cobertura") != 0)
    & (pl.col("fecont_pdcion").dt.year() != 1900)
    & (
        (pl.col("feini_vigencia_cobertura") - pl.col("fecont_pdcion")).dt.total_days()
        + 1
        > 30
    )
).slice(0, 1).write_parquet(
    "prototipo_pcr/tests/data/pdn_anticipada_no_mensual.parquet"
)

# Produccion anticipada mensual
df.filter(
    (pl.col("fecont_pdcion").dt.year() == pl.col("feini_vigencia_cobertura").dt.year())
    & (
        pl.col("fecont_pdcion").dt.month()
        < pl.col("feini_vigencia_cobertura").dt.month()
    )
    & (dias_vigencia.is_between(15, 32))
    & (pl.col("ptcobro_cobertura") != 0)
).slice(0, 1).write_parquet("prototipo_pcr/tests/data/pdn_anticipada_mensual.parquet")

# Produccion vencida
df.filter(
    (pl.col("fecont_pdcion") > pl.col("fefin_vigencia_cobertura"))
    & (dias_vigencia > 32)
    & (pl.col("ptcobro_cobertura") != 0)
).slice(0, 1).write_parquet("prototipo_pcr/tests/data/pdn_vencida_no_mensual.parquet")

# # Produccion vencida mensual
df.filter(
    (pl.col("fecont_pdcion").dt.year() == pl.col("fefin_vigencia_cobertura").dt.year())
    & (
        pl.col("fecont_pdcion").dt.month()
        > pl.col("fefin_vigencia_cobertura").dt.month()
    )
    & (dias_vigencia.is_between(15, 32))
    & (pl.col("ptcobro_cobertura") != 0)
).slice(0, 1).write_parquet("prototipo_pcr/tests/data/pdn_vencida_mensual.parquet")
