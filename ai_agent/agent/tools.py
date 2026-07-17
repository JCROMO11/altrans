import json
from db import queries

_CONDUCTOR_TOOL_NAMES = {
    "listar_manifiestos",
    "consultar_manifiesto",
    "resumen_periodo",
    "manifiestos_pendientes_pago",
    "manifiestos_sin_factura",
    "manifiestos_con_novedad",
    "conductor_info",
}

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "listar_manifiestos",
            "description": "Lista todos los manifiestos de un conductor, opcionalmente filtrados por mes y año. Úsala cuando el conductor pregunte por 'mis manifiestos', 'mis viajes' o quiera un resumen general.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mes":  {"type": "string", "description": "Mes en mayúsculas (opcional): ENERO, FEBRERO, etc."},
                    "anio": {"type": "string", "description": "Año (opcional), ej: 2025"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "consultar_manifiesto",
            "description": "Obtiene todos los datos de un manifiesto específico: conductor, ruta, cliente, pagos y facturación.",
            "parameters": {
                "type": "object",
                "properties": {
                    "numero": {"type": "string", "description": "Número del manifiesto (5-7 dígitos)"},
                },
                "required": ["numero"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "resumen_periodo",
            "description": (
                "KPIs y totales de un período: cantidad de manifiestos, fletes, remesas y pendiente de pago. "
                "Úsala para: un mes específico (mes+anio), un año completo (solo anio, sin mes), "
                "o comparar trimestres. Si el conductor pregunta por todo un año (ej: '¿cuánto gané en 2024?'), "
                "llama con solo anio y sin mes."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "mes":  {"type": "string", "description": "Mes en mayúsculas (opcional): ENERO, FEBRERO, etc. Omitir para resumen anual."},
                    "anio": {"type": "string", "description": "Año (opcional), ej: 2025. Omitir solo si se quieren todos los registros."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "manifiestos_pendientes_pago",
            "description": "Lista manifiestos cuyo pago al conductor aún no está completado (no PAGADO ni ANULADO).",
            "parameters": {
                "type": "object",
                "properties": {
                    "mes": {"type": "string", "description": "Mes en mayúsculas (opcional)"},
                    "anio": {"type": "string", "description": "Año (opcional), ej: 2025"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "manifiestos_sin_factura",
            "description": "Lista manifiestos que aún no tienen número de factura asignado.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mes": {"type": "string", "description": "Mes en mayúsculas (opcional)"},
                    "anio": {"type": "string", "description": "Año (opcional), ej: 2025"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "top_conductores",
            "description": "Conductores con más manifiestos en un período, ordenados de mayor a menor.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mes":    {"type": "string", "description": "Mes en mayúsculas (opcional)"},
                    "anio":   {"type": "string", "description": "Año (opcional), ej: 2025"},
                    "limite": {"type": "string", "description": "Cantidad de resultados, por defecto 10"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "top_clientes",
            "description": "Clientes con más manifiestos, mayor valor de remesas y facturación en un período.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mes":    {"type": "string", "description": "Mes en mayúsculas (opcional)"},
                    "anio":   {"type": "string", "description": "Año (opcional), ej: 2025"},
                    "limite": {"type": "string", "description": "Cantidad de resultados, por defecto 10"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "top_rutas",
            "description": "Rutas origen-destino más frecuentes en un período.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mes":    {"type": "string", "description": "Mes en mayúsculas (opcional)"},
                    "anio":   {"type": "string", "description": "Año (opcional), ej: 2025"},
                    "limite": {"type": "string", "description": "Cantidad de resultados, por defecto 10"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "manifiestos_con_novedad",
            "description": "Lista manifiestos que tienen novedades pendientes registradas.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mes":  {"type": "string", "description": "Mes en mayúsculas (opcional)"},
                    "anio": {"type": "string", "description": "Año (opcional), ej: 2025"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "conductor_info",
            "description": "Busca un conductor por nombre o cédula y muestra su información y total de manifiestos.",
            "parameters": {
                "type": "object",
                "properties": {
                    "nombre": {"type": "string", "description": "Nombre o parte del nombre del conductor"},
                    "cedula": {"type": "string", "description": "Número de cédula exacto del conductor"},
                },
                "required": [],
            },
        },
    },
]

TOOLS_CONDUCTOR = [t for t in TOOLS if t["function"]["name"] in _CONDUCTOR_TOOL_NAMES]


def _anio(args) -> int | None:
    return int(args["anio"]) if args.get("anio") else None

def _limite(args) -> int:
    return int(args["limite"]) if args.get("limite") else 10

def _ccedula(args) -> str | None:
    return args.get("_conductor_cedula")

def _placa(args) -> str | None:
    return args.get("_placa")

_TOOL_MAP = {
    "listar_manifiestos":          lambda args: queries.listar_manifiestos(_ccedula(args), args.get("mes"), _anio(args), placa=_placa(args)),
    "consultar_manifiesto":        lambda args: queries.consultar_manifiesto(int(args["numero"]), _ccedula(args), placa=_placa(args)),
    "resumen_periodo":             lambda args: queries.resumen_periodo(args.get("mes"), _anio(args), _ccedula(args), placa=_placa(args)),
    "manifiestos_pendientes_pago": lambda args: queries.manifiestos_pendientes_pago(args.get("mes"), _anio(args), _ccedula(args), placa=_placa(args)),
    "manifiestos_sin_factura":     lambda args: queries.manifiestos_sin_factura(args.get("mes"), _anio(args), _ccedula(args), placa=_placa(args)),
    "top_conductores":             lambda args: queries.top_conductores(args.get("mes"), _anio(args), _limite(args)),
    "top_clientes":                lambda args: queries.top_clientes(args.get("mes"), _anio(args), _limite(args)),
    "top_rutas":                   lambda args: queries.top_rutas(args.get("mes"), _anio(args), _limite(args)),
    "manifiestos_con_novedad":     lambda args: queries.manifiestos_con_novedad(args.get("mes"), _anio(args), _ccedula(args), placa=_placa(args)),
    "conductor_info":              lambda args: queries.conductor_info(
                                       args.get("nombre"), args.get("cedula"),
                                       cedula_auth=_ccedula(args),
                                   ),
}


async def execute(tool_name: str, args: dict) -> str:
    fn = _TOOL_MAP.get(tool_name)
    if not fn:
        return f"Tool desconocida: {tool_name}"
    try:
        result = await fn(args)
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        return f"Error ejecutando {tool_name}: {e}"