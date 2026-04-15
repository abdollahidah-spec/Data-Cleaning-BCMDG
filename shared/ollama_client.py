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


def call_llm_nateco_batch(
    batch:          list[str],
    system_prompt:  str,
    few_shot:       list[dict],
    all_labels_set: set,
    liste_labels_prompt: str,
    cfg:            dict,
) -> list:
    """
    LLM fallback pour NatureEconomique — format numéroté avec few-shot.
    Retourne une liste de labels (ou None si non résolu), dans le même ordre que batch.
 
    Format réponse attendu :
        1. LABEL_A
        2. LABEL_B
        ...
    """
    import ollama
 
    llm             = cfg.get("llm", {})
    model           = llm.get("model",           "qwen2.5:14b")
    num_ctx         = llm.get("num_ctx",         3800)
    tokens_par_item = cfg.get("tokens_par_ligne", 35)
    n               = len(batch)
 
    items       = "\n".join(f"{i+1}. {v}" for i, v in enumerate(batch))
    user_prompt = (
        f"Labels disponibles :\n{liste_labels_prompt}\n\n"
        f"Classe ces libellés (format N. LABEL) :\n{items}"
    )
 
    try:
        resp = ollama.chat(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                *few_shot,
                {"role": "user",   "content": user_prompt},
            ],
            options={
                "temperature": 0,
                "num_predict": n * tokens_par_item,
                "num_ctx":     num_ctx,
                "stop":        ["\n\n", "---", "Note", "Exemple", "Explanation"],
            },
        )
        reponse = resp["message"]["content"].strip()
    except Exception as e:
        print(f"[LLM NATECO ERROR] {e}")
        return [None] * n
 
    resultats = [None] * n
    pattern   = re.compile(r"^(\d+)[.\)]\s*(.+)$")
    for ligne in reponse.split("\n"):
        m = pattern.match(ligne.strip())
        if m:
            idx = int(m.group(1)) - 1
            # Normaliser : accents + majuscules + espaces
            import unicodedata
            nfkd = unicodedata.normalize("NFKD", m.group(2))
            val  = "".join(c for c in nfkd if not unicodedata.combining(c)).upper()
            val  = re.sub(r"[^A-Z0-9\s]", " ", val).strip()
            val  = " ".join(w for w in val.split() if len(w) >= 2)
            if 0 <= idx < n and val in all_labels_set:
                resultats[idx] = val
    return resultats