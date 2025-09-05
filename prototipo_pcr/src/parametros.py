from datetime import date
from dateutil.relativedelta import relativedelta
from pathlib import Path

base_dir = Path(__file__).resolve().parent


# saltar una fila al leer los insumos que se usa como paralelo de los nombres de campos en las HU
SKIP_ROWS = 1

# Ubicación de los insumos relativa al script de ejecución
RUTA_INSUMOS = base_dir.parent / "inputs" / "insumos.xlsx"
RUTA_REL_BT = base_dir.parent / "inputs" / "relacion_bt.xlsx"

# Insumos transversales
HOJA_PARAMETROS_CONTAB = "param_contabilidad"
HOJA_EXCEPCIONES_50_50 = "excepciones_50_50"
HOJA_GASTO = "gastos"
HOJA_MONEDA = "tasa_cambio"
HOJA_DESCUENTO = "porc_descuento"
HOJA_TIPO_SEGURO = "tipo_seguro"
HOJA_CUENTAS_REA = "cuentas_reaseguro"
RUTA_RIESGO_CREDITO = base_dir.parent / "inputs" / "riesgo_credito.xlsx"

# Insumos de recibos contabilizados
HOJA_PDN = "produccion_directo"
HOJA_CESION = "produccion_rea"
HOJA_COMISION_REA = "comision_rea"
HOJA_COSTO_CONTRATO = "costo_rea_noprop"
HOJA_SEGUIMIENTO_REA = "seguimiento_rea_noprop"

# Insumos de onerosidad
HOJA_ONEROSIDAD = "onerosidad"
HOJA_RECUP_ONEROSIDAD = "recup_onerosidad"


# Fechas relevantes para cada ejecución
FECHA_VALORACION = date(2024, 12, 31)
FECHA_TRANSICION = date(2024, 9, 30)
FECHA_VALORACION_ANTERIOR = FECHA_VALORACION + relativedelta(months=-1)

# Rutas salidas
RUTA_SALIDA_DEVENGO = (
    base_dir.parent
    / "output"
    / f"output_devengo_fluc_{FECHA_VALORACION.strftime('%d%m%Y')}.xlsx"
)
RUTA_SALIDA_CONTABLE = (
    base_dir.parent
    / "output"
    / f"output_contable_{FECHA_VALORACION.strftime('%d%m%Y')}.xlsx"
)

# Parametros generales
NIVELES_DETALLE = ["recibo", "cobertura"]
MONEDA_DESTINO = "COP"

# Ayudan a controlar campos de salida, orden y nombres
CAMPOS_OUTPUT_CONTABLE = [
    # Campos necesarios para contabilizar, deben aparecer en el output con estos nombres
    "componente",
    "tipo_contabilidad",
    "regla_devengo",  # nivel de agregacion y parametros
    "tipo_contrato",
    "cohorte",
    "anio_liberacion",
    "transicion",  # nivel de agregacion
    "fecha_inicio_periodo",
    "fecha_valoracion",  # fecha de inicio y fin del periodo de valoracion
    "fecha_constitucion",
    "fecha_inicio_devengo",
    "fecha_fin_devengo",
    "valor_base_devengo",  # parametros devengo
    "dias_devengados",
    "dias_no_devengados",
    "control_suma_dias",  # solo estan para devengo diario
    "dias_constitucion",
    "dias_liberacion",
    "valor_devengo_diario",  # solo estan para devengo diario
]
CAMPOS_OUTPUT_5050 = [
    "mes_constitucion",
    "mes_ini_liberacion",
    "mes_fin_liberacion",  # solo estan para devengo 5050
]
CAMPOS_OUTPUT_LIMITE = [
    "porc_consumo_limite",  # solo estan para devengo por consumo del limite
]
CAMPOS_OUTPUT_CALCULADO = [
    "estado_devengo",
    "valor_constitucion",
    "valor_liberacion",
    "valor_liberacion_acum",
    "saldo",  # movimientos y saldo
]

# a qué tipos de contabilidad aplica deterioro
APLICA_DETERIORO = ["ifrs17_local", "ifrs_17_corporativo"]


# Las columnas que se deben pivotear para el output contable
COLUMNAS_CALCULO = [
    "valor_constitucion",
    "valor_liberacion",
    "saldo",
    "fluctuacion_liberacion",
    "fluctuacion_constitucion",
    "constitucion_deterioro",
    "liberacion_deterioro",
]
