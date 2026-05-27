import json
import logging
import os
from groq import Groq
from openai import OpenAI
from config import get_settings
from agent.prompts import build_system_prompt
from agent import tools as tool_executor

logger = logging.getLogger(__name__)

_cfg = get_settings()

# Agente principal: DeepSeek v4 Flash (OpenAI-compatible API)
_client = OpenAI(api_key=os.environ["DEEPSEEK_API_KEY"], base_url="https://api.deepseek.com")
MODEL   = "deepseek-v4-flash"

# Moderación: llama-prompt-guard-2-86m — clasificador especializado en jailbreaks (~50ms, 14.4K RPD free)
_mod_client    = Groq()
MODEL_MODERATE = "meta-llama/llama-prompt-guard-2-86m"

MAX_TOOL_ITERS = 6  # tope duro a los ciclos del agente


def run(
    mensaje: str,
    historial: list[dict] = None,
    conductor_nombre: str = None,
    conductor_cedula: str = None,
    placa: str = None,
    nombre: str = None,
    tipo_usuario: str = None,
) -> tuple[str, bool]:
    """Ejecuta el agente. Filtra por cédula (conductor) o placa (propietario).

    Devuelve (respuesta, tools_called). `tools_called` indica si el agente
    invocó al menos una herramienta para esta respuesta — el webhook lo usa
    para descontar del límite de consultas solo los mensajes con tool call.

    Compatibilidad: si se pasa `conductor_cedula`, se trata como conductor
    autenticado (modo legacy). Para propietarios usar `placa` + `nombre`.
    """
    # Normalizar parámetros — soportar firma legacy y nueva.
    if conductor_cedula and not tipo_usuario:
        tipo_usuario = "conductor"
        nombre = nombre or conductor_nombre

    autenticado = bool(conductor_cedula or placa)
    system_prompt = build_system_prompt(
        nombre=nombre or conductor_nombre,
        cedula=conductor_cedula,
        placa=placa,
        tipo_usuario=tipo_usuario,
    )
    messages = [{"role": "system", "content": system_prompt}]
    if historial:
        messages.extend(historial)
    messages.append({"role": "user", "content": mensaje})

    active_tools = tool_executor.TOOLS_CONDUCTOR if autenticado else tool_executor.TOOLS

    tools_called = False

    for _ in range(MAX_TOOL_ITERS):
        response = _client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=active_tools,
            tool_choice="auto",
            max_tokens=8192,
            temperature=0.2,
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            content = msg.content
            if not content:
                # DeepSeek ocasionalmente devuelve content=None sin tool_calls.
                # Reintentamos sin tools para forzar una respuesta de texto.
                logger.warning("empty_response_retry", extra={"mensaje": mensaje[:100]})
                recovery = _client.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    max_tokens=8192,
                    temperature=0.3,
                )
                content = recovery.choices[0].message.content or "Lo siento, no pude procesar tu consulta. Intenta de nuevo."
            return content, tools_called

        tools_called = True
        messages.append(msg)
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError as e:
                logger.warning("json_parse_error", extra={
                    "tool": tc.function.name, "args_raw": tc.function.arguments[:200], "error": str(e),
                })
                messages.append({
                    "role":         "tool",
                    "tool_call_id": tc.id,
                    "content":      json.dumps({"error": f"Argumentos mal formados: {e}"}),
                })
                continue
            if conductor_cedula:
                args["_conductor_cedula"] = conductor_cedula
            if placa:
                args["_placa"] = placa
            result = tool_executor.execute(tc.function.name, args)
            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      result,
            })

    # Si el modelo entra en bucle, forzar respuesta sin más tools
    response = _client.chat.completions.create(
        model=MODEL,
        messages=messages,
        max_tokens=8192,
        temperature=0.2,
    )
    content = response.choices[0].message.content or "No pude completar tu consulta. Intenta reformularla."
    return content, tools_called


# ── Moderación: capa 2 anti-jailbreak ────────────────────────────────────────
# llama-prompt-guard-2-86m: clasificador especializado en prompt injection y jailbreaks.
# Responde SAFE / UNSAFE. Latencia <50ms. No necesita prompt elaborado.

def moderate(texto: str) -> bool:
    """Devuelve True si el mensaje es un intento de jailbreak."""
    response = _mod_client.chat.completions.create(
        model=MODEL_MODERATE,
        messages=[{"role": "user", "content": texto[:500]}],
        max_completion_tokens=8,
        temperature=0.0,
    )
    out = (response.choices[0].message.content or "").strip().upper()
    return out.startswith("UNSAFE")
