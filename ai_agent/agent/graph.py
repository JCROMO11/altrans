import json
from groq import Groq
from config import get_settings
from agent.prompts import build_system_prompt
from agent import tools as tool_executor

# ── Cliente Groq ──────────────────────────────────────────────────────────────

_cfg = get_settings()
_client = Groq()  # lee GROQ_API_KEY del entorno automáticamente
MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"

# ── Cliente Claude (producción) ───────────────────────────────────────────────
# import anthropic
# _claude = anthropic.Anthropic(api_key=_cfg["anthropic_api_key"])
# CLAUDE_MODEL = "claude-sonnet-4-6"


def run(
    mensaje: str,
    historial: list[dict] = None,
    conductor_nombre: str = None,
    conductor_cedula: str = None,
) -> str:
    """
    Ejecuta el agente con el mensaje del usuario.
    conductor_nombre / conductor_cedula: si se pasan, el prompt y los filtros
    restringen todas las respuestas a ese conductor.
    """
    system_prompt = build_system_prompt(conductor_nombre, conductor_cedula)
    messages = [{"role": "system", "content": system_prompt}]
    if historial:
        messages.extend(historial)
    messages.append({"role": "user", "content": mensaje})

    # Agentic loop: el modelo puede llamar tools varias veces antes de responder
    while True:
        active_tools = tool_executor.TOOLS_CONDUCTOR if conductor_cedula else tool_executor.TOOLS
        response = _client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=active_tools,
            tool_choice="auto",
            max_completion_tokens=1024,
            temperature=0.2,
        )

        msg = response.choices[0].message

        # Sin tool calls → respuesta final
        if not msg.tool_calls:
            return msg.content

        # Ejecutar cada tool call y agregar resultados al historial
        messages.append(msg)
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            if conductor_cedula:
                args["_conductor_cedula"] = conductor_cedula
            result = tool_executor.execute(tc.function.name, args)
            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      result,
            })

    # ── Versión Claude (producción) ───────────────────────────────────────────
    # La estructura es diferente: Claude devuelve content blocks, no tool_calls.
    #
    # response = _claude.messages.create(
    #     model=CLAUDE_MODEL,
    #     system=SYSTEM_PROMPT,
    #     messages=messages,
    #     tools=tool_executor.CLAUDE_TOOLS,
    #     max_tokens=2048,
    # )
    # while response.stop_reason == "tool_use":
    #     tool_results = []
    #     for block in response.content:
    #         if block.type == "tool_use":
    #             result = tool_executor.execute(block.name, block.input)
    #             tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result})
    #     messages.append({"role": "assistant", "content": response.content})
    #     messages.append({"role": "user", "content": tool_results})
    #     response = _claude.messages.create(model=CLAUDE_MODEL, system=SYSTEM_PROMPT, messages=messages, tools=tool_executor.CLAUDE_TOOLS, max_tokens=2048)
    # return response.content[0].text
