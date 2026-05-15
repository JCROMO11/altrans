"""
Runners de agente para múltiples modelos (Groq, DeepSeek, Gemini, Claude).

Cada runner toma (mensaje, system_prompt, cedula_conductor) y devuelve
(respuesta, metricas) donde metricas = {tokens_in, tokens_out, tool_calls, tool_names}.

Para producción el chatbot usa solo `agent.graph.run` (DeepSeek).
Estos runners son para `scripts/test_agent.py --modelos a,b,c,d` (A/B testing).

Los clientes se instancian lazy — solo se conectan cuando se invoca su runner.
"""
import json
import os
from typing import Callable

from agent.prompts import build_system_prompt
from agent import tools as tool_executor

MAX_TOOL_ITERS = 6

# Identificadores de modelo y nombres completos
MODELS = {
    "groq":     "meta-llama/llama-4-scout-17b-16e-instruct",
    "deepseek": "deepseek-v4-flash",
    "gemini":   "gemini-2.5-flash",
    "claude":   "claude-haiku-4-5-20251001",
}


# ── Clientes lazy (no instancian hasta que se invocan) ────────────────────────

_clients: dict = {}


def _get_groq():
    if "groq" not in _clients:
        from groq import Groq
        _clients["groq"] = Groq()
    return _clients["groq"]


def _get_deepseek():
    if "deepseek" not in _clients:
        from openai import OpenAI
        key = os.environ.get("DEEPSEEK_API_KEY")
        if not key:
            raise RuntimeError("DEEPSEEK_API_KEY no configurada")
        _clients["deepseek"] = OpenAI(api_key=key, base_url="https://api.deepseek.com")
    return _clients["deepseek"]


def _get_gemini():
    if "gemini" not in _clients:
        from google import genai
        key = os.environ.get("GEMINI_API_KEY")
        if not key:
            raise RuntimeError("GEMINI_API_KEY no configurada")
        _clients["gemini"] = genai.Client(api_key=key)
    return _clients["gemini"]


def _get_claude():
    if "claude" not in _clients:
        import anthropic
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY no configurada")
        _clients["claude"] = anthropic.Anthropic(api_key=key)
    return _clients["claude"]


def _exec_tool(name: str, args: dict, cedula: str | None) -> str:
    if cedula:
        args["_conductor_cedula"] = cedula
    return tool_executor.execute(name, args)


def _empty_metrics() -> dict:
    return {"tokens_in": 0, "tokens_out": 0, "tool_calls": 0, "tool_names": []}


def _tools_for(cedula: str | None) -> list:
    return tool_executor.TOOLS_CONDUCTOR if cedula else tool_executor.TOOLS


# ── Runner común para APIs OpenAI-compatibles (Groq + DeepSeek) ───────────────

def _run_openai_like(client, model: str, mensaje: str, system_prompt: str,
                     cedula: str | None, max_tokens_kw: str) -> tuple[str, dict]:
    messages = [{"role": "system", "content": system_prompt},
                {"role": "user",   "content": mensaje}]
    m = _empty_metrics()
    tools = _tools_for(cedula)

    for _ in range(MAX_TOOL_ITERS):
        kwargs = {"model": model, "messages": messages, "tools": tools,
                  "tool_choice": "auto", "temperature": 0.2,
                  max_tokens_kw: 1000}
        resp = client.chat.completions.create(**kwargs)
        if resp.usage:
            m["tokens_in"]  += resp.usage.prompt_tokens or 0
            m["tokens_out"] += resp.usage.completion_tokens or 0
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return msg.content or "", m
        messages.append(msg)
        for tc in msg.tool_calls:
            m["tool_calls"] += 1
            m["tool_names"].append(tc.function.name)
            args = json.loads(tc.function.arguments)
            messages.append({"role": "tool", "tool_call_id": tc.id,
                             "content": _exec_tool(tc.function.name, args, cedula)})

    # Salida forzada sin más tools tras el tope
    resp = client.chat.completions.create(
        model=model, messages=messages, temperature=0.2,
        **{max_tokens_kw: 1000})
    if resp.usage:
        m["tokens_in"]  += resp.usage.prompt_tokens or 0
        m["tokens_out"] += resp.usage.completion_tokens or 0
    return resp.choices[0].message.content or "", m


def run_groq(mensaje: str, nombre: str | None, cedula: str | None) -> tuple[str, dict]:
    sp = build_system_prompt(nombre, cedula)
    return _run_openai_like(_get_groq(), MODELS["groq"], mensaje, sp, cedula,
                            "max_completion_tokens")


def run_deepseek(mensaje: str, nombre: str | None, cedula: str | None) -> tuple[str, dict]:
    sp = build_system_prompt(nombre, cedula)
    return _run_openai_like(_get_deepseek(), MODELS["deepseek"], mensaje, sp, cedula,
                            "max_tokens")


# ── Runner: Gemini ────────────────────────────────────────────────────────────

def run_gemini(mensaje: str, nombre: str | None, cedula: str | None) -> tuple[str, dict]:
    from google.genai import types as gtypes
    client = _get_gemini()
    sp = build_system_prompt(nombre, cedula)

    tools_def = _tools_for(cedula)
    gemini_tools = gtypes.Tool(function_declarations=[
        {
            "name": t["function"]["name"],
            "description": t["function"]["description"],
            "parameters": t["function"]["parameters"],
        }
        for t in tools_def
    ])

    config_base = lambda with_tools=True: gtypes.GenerateContentConfig(
        system_instruction=sp,
        tools=[gemini_tools] if with_tools else None,
        temperature=0.2,
        max_output_tokens=1000,
    )

    contents = [gtypes.Content(role="user", parts=[gtypes.Part(text=mensaje)])]
    m = _empty_metrics()

    for _ in range(MAX_TOOL_ITERS):
        resp = client.models.generate_content(
            model=MODELS["gemini"], contents=contents, config=config_base(True))
        if resp.usage_metadata:
            m["tokens_in"]  += resp.usage_metadata.prompt_token_count or 0
            m["tokens_out"] += resp.usage_metadata.candidates_token_count or 0

        parts = resp.candidates[0].content.parts
        fn_calls   = [p for p in parts if getattr(p, "function_call", None) and p.function_call.name]
        text_parts = [p for p in parts if getattr(p, "text", None)]

        if not fn_calls:
            return (text_parts[0].text if text_parts else ""), m

        contents.append(resp.candidates[0].content)
        result_parts = []
        for p in fn_calls:
            m["tool_calls"] += 1
            m["tool_names"].append(p.function_call.name)
            args = dict(p.function_call.args)
            result_str = _exec_tool(p.function_call.name, args, cedula)
            result_parts.append(gtypes.Part(
                function_response=gtypes.FunctionResponse(
                    name=p.function_call.name,
                    response={"output": result_str})
            ))
        contents.append(gtypes.Content(role="user", parts=result_parts))

    resp = client.models.generate_content(
        model=MODELS["gemini"], contents=contents, config=config_base(False))
    if resp.usage_metadata:
        m["tokens_in"]  += resp.usage_metadata.prompt_token_count or 0
        m["tokens_out"] += resp.usage_metadata.candidates_token_count or 0
    text_parts = [p for p in resp.candidates[0].content.parts if getattr(p, "text", None)]
    return (text_parts[0].text if text_parts else ""), m


# ── Runner: Claude ────────────────────────────────────────────────────────────

def run_claude(mensaje: str, nombre: str | None, cedula: str | None) -> tuple[str, dict]:
    client = _get_claude()
    sp = build_system_prompt(nombre, cedula)

    tools_def = _tools_for(cedula)
    claude_tools = [
        {
            "name": t["function"]["name"],
            "description": t["function"]["description"],
            "input_schema": t["function"]["parameters"],
        }
        for t in tools_def
    ]

    messages = [{"role": "user", "content": mensaje}]
    m = _empty_metrics()

    for _ in range(MAX_TOOL_ITERS):
        resp = client.messages.create(
            model=MODELS["claude"], system=sp, messages=messages,
            tools=claude_tools, max_tokens=1000, temperature=0.2,
        )
        m["tokens_in"]  += resp.usage.input_tokens
        m["tokens_out"] += resp.usage.output_tokens
        tool_uses   = [b for b in resp.content if b.type == "tool_use"]
        text_blocks = [b for b in resp.content if b.type == "text"]
        if not tool_uses:
            return (text_blocks[0].text if text_blocks else ""), m
        messages.append({"role": "assistant", "content": resp.content})
        results = []
        for tu in tool_uses:
            m["tool_calls"] += 1
            m["tool_names"].append(tu.name)
            args = dict(tu.input)
            results.append({"type": "tool_result", "tool_use_id": tu.id,
                            "content": _exec_tool(tu.name, args, cedula)})
        messages.append({"role": "user", "content": results})

    resp = client.messages.create(
        model=MODELS["claude"], system=sp, messages=messages,
        max_tokens=1000, temperature=0.2)
    m["tokens_in"]  += resp.usage.input_tokens
    m["tokens_out"] += resp.usage.output_tokens
    text_blocks = [b for b in resp.content if b.type == "text"]
    return (text_blocks[0].text if text_blocks else ""), m


# ── Registry ─────────────────────────────────────────────────────────────────

RUNNERS: dict[str, Callable] = {
    "groq":     run_groq,
    "deepseek": run_deepseek,
    "gemini":   run_gemini,
    "claude":   run_claude,
}
