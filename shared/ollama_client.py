"""
shared/ollama_client.py
========================
Client Ollama partagé — utilisé par Pays (actuel) et futurs champs LLM.
"""
from __future__ import annotations
import json, re, time
from typing import Optional

def call_llm_batch(values: list, system_prompt: str,
                   valid_results: set, cfg: dict) -> dict:
    """
    Envoie un batch à Ollama, retourne {valeur: résultat_ou_None}.
    Paramètres LLM lus depuis cfg["llm"]: model, num_ctx, max_retry, retry_wait.
    """
    import ollama
    llm        = cfg.get("llm", {})
    model      = llm.get("model",      "qwen2.5:14b")
    num_ctx    = llm.get("num_ctx",    3500)
    max_retry  = llm.get("max_retry",  3)
    retry_wait = llm.get("retry_wait", 2)

    user_content = json.dumps(
        {"values": [{"input": str(v)} for v in values]}, ensure_ascii=False)

    for attempt in range(1, max_retry + 1):
        try:
            response = ollama.chat(
                model=model,
                messages=[{"role": "system", "content": system_prompt},
                           {"role": "user",   "content": user_content}],
                format="json",
                options={"temperature": 0, "num_ctx": num_ctx},
            )
            raw   = re.sub(r"```(?:json)?|```", "", response["message"]["content"]).strip()
            items = json.loads(raw).get("results", [])
            mapping = {}
            for item in items:
                inp    = item.get("input", "")
                result = item.get("iso2") or item.get("result") or item.get("value")
                mapping[inp] = (str(result).upper()
                                if result and str(result).upper() in valid_results
                                else None)
            return mapping
        except Exception:
            if attempt < max_retry:
                time.sleep(retry_wait * attempt)
    return {v: None for v in values}
