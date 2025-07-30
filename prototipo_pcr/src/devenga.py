"""
Módulo de funciones de devengamiento de un componente durante una vigencia determinada
"""
import polars as pl
import datetime as dt
import src.parametros as params
import src.aux_tools as aux_tools



def deveng_diario(input_deveng:pl.DataFrame) -> pl.DataFrame:
    """
    Recibe un input preprocesado de devengamiento y devuelve el devengo uniforme diario
    """
    output_deveng_diario = input_deveng.with_columns(
        (pl.when(pl.col("fecha_valoracion") < pl.col("fecha_inicio_devengo"))
        .then(pl.lit("no_iniciado"))
        .otherwise(
            pl.when(
                # si ya inicio a devengar o le quedan dias por devengar esta en curso
                (pl.col("fecha_inicio_devengo") <= pl.col("fecha_valoracion")) &
                (pl.col("fecha_inicio_periodo") <= pl.col("fecha_fin_devengo"))
            )
            .then(pl.lit("en_curso"))
            .when(pl.col('fecha_constitucion') > pl.col('fecha_fin_devengo'))
            .then(pl.lit('entra_devengado'))
            .otherwise(pl.lit("finalizado"))
        )).alias("estado_devengo")
    ).with_columns(
        pl.when(pl.col('estado_devengo') == 'finalizado')
        .then(0)    # no se constituye si ya finalizó la vigencia
        # en cualquier otro caso, se constituye por el total de la vigencia
        .otherwise(aux_tools.calcular_dias_diferencia(pl.col('fecha_fin_devengo'),  pl.col('fecha_inicio_vigencia')))
        .alias('dias_constitucion')
    ).with_columns(
        # dias que ya se devengaron, depende del estado
        pl.when(pl.col("estado_devengo") == "no_iniciado")
        .then(pl.lit(0))
        .when((pl.col("estado_devengo") == "en_curso") | (pl.col("estado_devengo") == "entra_devengado"))
        .then(aux_tools.calcular_dias_diferencia(pl.col('fecha_valoracion'), pl.col('fecha_inicio_vigencia')))
        .when(pl.col("estado_devengo") == "finalizado")
        .then(pl.col("dias_constitucion"))
        .alias("dias_devengados")
    ).with_columns(
        # dias aun no devengados segun el estado
        pl.when(pl.col("estado_devengo") == "no_iniciado")
        .then(pl.col("dias_constitucion"))
        .when(pl.col("estado_devengo") == "en_curso")
        .then(
            # no se incluye extremo para no doble-contar el dia de valoracion
            aux_tools.calcular_dias_diferencia(
                pl.col("fecha_fin_devengo"), 
                pl.col("fecha_valoracion"), 
                incluir_extremos=False))
        .when((pl.col("estado_devengo") == "finalizado") | (pl.col("estado_devengo") == "entra_devengado"))
        .then(pl.lit(0))
        .alias("dias_no_devengados")
    ).with_columns(
        # validacion del calculo de dias
        (pl.col("dias_devengados") + pl.col("dias_no_devengados") == pl.col("dias_constitucion"))
        .alias("control_suma_dias")
    ).with_columns(
        # valor diario devengo (prima diaria en sap)
        pl.when(pl.col('dias_constitucion') != 0)
        .then(pl.col('valor_base_devengo') / pl.col('dias_constitucion'))
        .otherwise(0.0)
        .alias('valor_devengo_diario')
    ).with_columns(
        (# el valor que falta por devengarse es el saldo
        pl.col('valor_devengo_diario') * pl.col('dias_no_devengados')
        ).alias('saldo')
    ).with_columns(
        pl.when(
                    (pl.col("fecha_constitucion") <= pl.col("fecha_valoracion")) &
                    (pl.col("fecha_inicio_periodo") <= pl.col("fecha_constitucion"))
                )
        .then(pl.col('dias_constitucion') * pl.col('valor_devengo_diario'))
        .otherwise(pl.lit(0.0))
        .alias('valor_constitucion')
    ).with_columns(
    # Queremos que si entra devengado, libere todo
    pl.when(pl.col('estado_devengo') == 'entra_devengado')
    .then(pl.col('dias_constitucion'))
    .when(pl.col('fecha_inicio_periodo') <= pl.col('fecha_fin_devengo'))
    .then(
        aux_tools.calcular_dias_diferencia(
            pl.min_horizontal([pl.col('fecha_valoracion'), pl.col('fecha_fin_devengo')]),
            pl.when(pl.col("fecha_constitucion") > pl.col("fecha_inicio_periodo"))
            .then(pl.col('fecha_inicio_vigencia')).otherwise(pl.col('fecha_inicio_periodo'))
        )
    )
    .otherwise(pl.lit(0))
    .alias('dias_liberacion')
).with_columns(
        ( pl.col('dias_liberacion') * pl.col('valor_devengo_diario') ).alias('valor_liberacion')
    ).with_columns(
        ( pl.col('dias_devengados') * pl.col('valor_devengo_diario') ).alias('valor_liberacion_acum')
    )

    return output_deveng_diario


def deveng_cincuenta(input_deveng_cinq:pl.DataFrame, fe_valoracion:dt.date) -> pl.DataFrame:
    """
    Recibe un input preprocesado para devengo y devuelve el devengamiento segun las reglas del 50/50
    """
    
    # se constituye solo si el mes de constitucion es menor o igual al mes de fin devengo
    mes_constitucion = aux_tools.yyyymm(pl.col('fecha_constitucion'))
    mes_fin_devengo = aux_tools.yyyymm(pl.col('fecha_fin_devengo'))
    es_periodo_constit = ((pl.col("fecha_constitucion") <= pl.col("fecha_valoracion")) &
                            (pl.col("fecha_inicio_periodo") <= pl.col("fecha_constitucion")))
    # aplica las condiciones para constituir
    output_deveng_cinq = input_deveng_cinq.with_columns(
         pl.when((mes_constitucion <= mes_fin_devengo) & es_periodo_constit)
        .then(pl.col('valor_base_devengo'))
        .otherwise(0.0)
        .alias('valor_constitucion')
    ).with_columns(
        mes_constitucion.alias('mes_constitucion')
    )

    # libera solo a cierre de mes -> si no es cierre la norma me obliga a mantener el 50%
    es_cierre_mes = pl.lit(aux_tools.es_ultimo_dia_mes(fe_valoracion))
    # primera liberacion ocurre el max mes entre mes constitucion y mes inicio devengo
    aux_fe_primera_lib = pl.max_horizontal([pl.col('fecha_constitucion'), pl.col('fecha_inicio_devengo')])
    mes_primera_lib = aux_tools.yyyymm(aux_fe_primera_lib)
    mes_segunda_lib = aux_tools.yyyymm(aux_fe_primera_lib.dt.offset_by("1mo"))
    mes_valoracion = aux_tools.yyyymm(pl.col('fecha_valoracion'))
    cierre_anterior = pl.col('fecha_valoracion').dt.offset_by('-1mo').dt.month_end()
    mes_anterior = aux_tools.yyyymm(cierre_anterior)
    # calcula la liberacion por meses
    lib_mes_actual = pl.when(
            ((mes_valoracion == mes_primera_lib) | (mes_valoracion == mes_segunda_lib)) & es_cierre_mes
        ).then(pl.col('valor_base_devengo') * 0.5).otherwise(0.0)
    lib_mes_anterior = pl.when(
            ((mes_anterior == mes_primera_lib) | (mes_anterior == mes_segunda_lib)) & es_cierre_mes
        ).then(pl.col('valor_base_devengo') * 0.5).otherwise(0.0)
    # para conocer el saldo debo acumular la liberacion que solo se puede dar en max 2 periodos
    lib_acumulada = lib_mes_actual + lib_mes_anterior
    # el estado segun las fechas
    devengo_no_iniciado = (pl.col('fecha_valoracion') < pl.col('fecha_constitucion'))
    devengo_en_curso = (mes_primera_lib <= mes_valoracion) & (mes_valoracion <= mes_segunda_lib)
    devengo_finalizado = mes_valoracion > mes_segunda_lib
    output_deveng_cinq = output_deveng_cinq.with_columns(
        pl.when(devengo_no_iniciado).then(pl.lit('no_iniciado'))
        .when(devengo_en_curso).then(pl.lit('en_curso'))
        .when(devengo_finalizado).then(pl.lit('finalizado'))
        .otherwise(pl.lit('revisar_estado'))
        .alias('estado_devengo')
    ).with_columns(
        # El saldo es el 100% si no ha empezado
        pl.when(devengo_no_iniciado).then(pl.col('valor_base_devengo'))
        # el 100% menos la lib acumulada si está liberando
        .when(devengo_en_curso).then(pl.col('valor_base_devengo') - lib_acumulada)
        # sino, ya acabó y el saldo es cero
        .otherwise(0)
        .alias('saldo')
    ).with_columns(
        lib_mes_actual.alias('valor_liberacion')
    ).with_columns(
        mes_primera_lib.alias('mes_ini_liberacion')
    ).with_columns(
        mes_segunda_lib.alias('mes_fin_liberacion')
    ).with_columns(
        lib_acumulada.alias('valor_liberacion_acum')
    )

    return output_deveng_cinq


def devengo_diario_vs_limite(input_costo: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica el devengo del costo de contrato de RA no prop tomando el máximo entre devengo diario 
    y consumo del límite agregado del contrato
    """
    # Expresiones de saldo segun si es recibo nuevo o ya viene devengandose
    saldo_anterior = pl.col('saldo_anterior') if 'saldo_anterior' in input_costo.columns else pl.col('valor_base_devengo')

    # Devengo diario es la base
    output_devengo_costo = deveng_diario(input_costo).with_columns(
        # % consumo del límite en el mes
        (pl.col('valor_siniestros_incurridos_mes') / pl.col('limite_agregado_valor_instalado'))
        .alias('porc_consumo_limite')
    ).with_columns(
        # valor de liberación por límite
        (pl.col('valor_base_devengo') * pl.col('porc_consumo_limite')).alias('valor_liberacion_limite')
    ).with_columns(
        # elegir el método de liberación: diario o límite
        pl.when(aux_tools.yyyymm(pl.col('fecha_valoracion')) == aux_tools.yyyymm(pl.col('fecha_fin_devengo')))  # último mes debe liberar todo el saldo
        .then(pl.lit('saldo_restante'))
        .when(pl.col('valor_liberacion_limite') > pl.col('valor_liberacion'))
        .then(pl.lit('consumo_limite'))
        .otherwise(pl.lit('diario'))
        .alias('regla_devengo')
    ).with_columns(
        # aplicar el método de liberación elegido
        pl.when(pl.col('regla_devengo') == 'saldo_restante')
        .then(saldo_anterior)
        .when(pl.col('regla_devengo') == 'consumo_limite')
        .then(pl.col('valor_liberacion_limite'))
        .otherwise(pl.col('valor_liberacion'))
        .alias('valor_liberacion')
    ).with_columns(
        # acumulado de liberación recalculado
        ((pl.col('valor_base_devengo') - saldo_anterior) + pl.col('valor_liberacion')).alias('valor_liberacion_acum')
    ).with_columns(
        # nuevo saldo después de la liberación (esto es antes de que se apliquen signos de reserva)
        (saldo_anterior - pl.col('valor_liberacion')).alias('saldo')
    )

    return output_devengo_costo



def agregar_cohorte_dinamico(df: pl.DataFrame) -> pl.DataFrame:
    """
    La cohorte depende del tipo de contrato, 
    por lo cual debe usar columnas distintas que deben aparecer en el insumo
    """
    columnas = df.columns
    tiene_col_directo = 'fecha_expedicion_poliza' in columnas
    tiene_col_rea = 'fe_ini_vig_contrato_reaseguro' in columnas
    if tiene_col_directo and tiene_col_rea:
        cohorte_directo = pl.col('fecha_expedicion_poliza').dt.year()
        cohorte_rea = pl.col('fe_ini_vig_contrato_reaseguro').dt.year()
        expr = pl.when(pl.col('tipo_contrato') == 'directo').then(cohorte_directo).otherwise(cohorte_rea)
    elif tiene_col_directo:
        expr = pl.col('fecha_expedicion_poliza').dt.year()
    elif tiene_col_rea:
        expr = pl.col('fe_ini_vig_contrato_reaseguro').dt.year()
    else:
        raise ValueError("No existen columnas de fecha válidas para definir la cohorte.")

    return df.with_columns(expr.alias('cohorte'))


def etiquetar_resultado_devengo(data_devengo:pl.DataFrame) -> pl.DataFrame:
    """
    calcula campos necesarios según los inputs y outputs del devengo
    y organiza el output con estas columnas incluidas
    """
    # condiciones para definir los niveles de agregacion
    devengo_en_curso = pl.col("estado_devengo") == "en_curso"
    es_anio_actual = pl.col("fecha_constitucion").dt.year() == pl.col("fecha_valoracion").dt.year()
    es_transicion = pl.col("fecha_valoracion") == params.FECHA_TRANSICION
    # aplica condiciones
    data_devengo_out = data_devengo.pipe(
        agregar_cohorte_dinamico
    ).with_columns(
        # define si la liberacion es del año actual o anterior o ninguno porque no se libera
        pl.when(devengo_en_curso)
        .then(
                pl.when(es_anio_actual)
                .then(pl.lit("anio_actual"))
                .otherwise(pl.lit("anio_anterior"))
        )
        .otherwise(pl.lit("no_aplica"))
        .alias("anio_liberacion")
    ).with_columns(
        pl.when(es_transicion).then(1).otherwise(0).alias('transicion')
    )

    return data_devengo_out


def devengar(input_deveng:pl.DataFrame, fe_valoracion:dt.date) -> pl.DataFrame:
    """
    Recibe cualquier input preprocesado para devengamiento
    devuelve el output de devengo consolidado y organizado relativo a la fecha de valoración
    integra las distintas reglas de devengo, las aplica según corresponda y consolida el resultado
    """

    # parametros generales segun la fecha de valoracion
    input_devengo = input_deveng.with_columns(
        pl.lit(fe_valoracion).alias('fecha_valoracion')
    ).with_columns(
        # por defecto los mov se calculan para el periodo desde el inicio del mes de la fe valoracion
        pl.col('fecha_valoracion').dt.month_start().alias('fecha_inicio_periodo')
    ).with_columns(
        # define fecha cierre anterior para el delta
        pl.col('fecha_valoracion').dt.offset_by('-1mo').alias('fecha_valoracion_anterior')
    )
    # define si aplica devengo de costo contrato
    aplica_costo_contrato = pl.col('tipo_insumo') == 'costo_contrato_rea_noprop'
    # define si aplica 5050 y hace la particion del insumo
    dias_devengo = aux_tools.calcular_dias_diferencia(pl.col('fecha_fin_devengo'), pl.col('fecha_inicio_devengo'))
    aplica_5050 = (dias_devengo <= 32) & (pl.col('candidato_devengo_50_50') == 1)
    input_devengo = input_devengo.with_columns(
        pl.when(aplica_5050).then(pl.lit('regla_50_50')).otherwise(pl.lit('diario')).alias('regla_devengo')
    )
    input_devengo_5050 = input_devengo.filter(aplica_5050 & (~aplica_costo_contrato))
    input_devengo_costcon = input_devengo.filter(aplica_costo_contrato & (~aplica_5050))
    input_devengo_diario = input_devengo.filter((~aplica_5050) & (~aplica_costo_contrato))

    # se inicializan outputs vacío y campos output como copia de la lista (porque si no modifica la original)
    outputs, campos_output = [], params.CAMPOS_OUTPUT_CONTABLE.copy()
    # aplica el devengamiento a cada particion solo si tiene datos de entrada
    if input_devengo_diario.height > 0:
        outputs.append(deveng_diario(input_devengo_diario))
    if input_devengo_5050.height > 0:
        outputs.append(deveng_cincuenta(input_devengo_5050, fe_valoracion=fe_valoracion))
        campos_output.extend(params.CAMPOS_OUTPUT_5050)
    if input_devengo_costcon.height > 0:
        outputs.append(devengo_diario_vs_limite(input_devengo_costcon))
        campos_output.extend(params.CAMPOS_OUTPUT_LIMITE)
    if not outputs:
        return pl.DataFrame()
    
    # retorna un consolidado tipo union all de los outputs
    output_devengo_consolidado = pl.concat(outputs, how="diagonal").with_columns([
        # aplica el signo de reserva segun tipo insumo y movimientos
            (pl.col('valor_constitucion') * pl.col('signo_constitucion')).alias('valor_constitucion'),
            (pl.col('valor_liberacion') * -1 * pl.col('signo_constitucion')).alias('valor_liberacion'),
            (pl.col('valor_liberacion_acum') * -1 * pl.col('signo_constitucion')).alias('valor_liberacion_acum'),
            (pl.col('saldo') * pl.col('signo_constitucion')).alias('saldo'),
            (pl.col('saldo') - pl.col('valor_constitucion') - pl.col('valor_liberacion')).alias('saldo_anterior')
        ]).pipe(
            etiquetar_resultado_devengo
        )
    
    campos_output.extend(params.CAMPOS_OUTPUT_CALCULADO)
    campos_output = list(dict.fromkeys(campos_output))
    campos_input = [ col for col in output_devengo_consolidado.columns if col not in campos_output ]

    return output_devengo_consolidado.select(campos_input + campos_output)