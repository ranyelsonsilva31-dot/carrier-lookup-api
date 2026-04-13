"""
Carrier Lookup Engine
Consulta operadora via ABR Telecom (ANATEL).
- 50 workers assíncronos paralelos
- Retry com backoff exponencial + jitter
- Logging estruturado com rotação de arquivo (máx 5 MB × 3 arquivos)
- Endpoint /debug para inspecionar HTML bruto retornado pelo site
"""

import asyncio
import logging
import random
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────
# Logging com rotação – não estoura memória
# ──────────────────────────────────────────────
LOG_DIR = Path("/tmp/carrier_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

_file_handler = RotatingFileHandler(
    LOG_DIR / "carrier.log",
    maxBytes=5 * 1024 * 1024,   # 5 MB por arquivo
    backupCount=3,               # máximo 3 arquivos = 15 MB total
    encoding="utf-8",
)
_file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(_file_handler)

# ──────────────────────────────────────────────
# Configurações da consulta
# ──────────────────────────────────────────────
ABR_URL = (
    "https://consultanumero.abrtelecom.com.br"
    "/consultanumero/consulta/consultaSituacaoAtualCtg"
)

HEADERS_BASE = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://consultanumero.abrtelecom.com.br",
    "Referer": "https://consultanumero.abrtelecom.com.br/",
    "Content-Type": "application/x-www-form-urlencoded",
}

# Palavras-chave de operadoras para fallback de parsing
CARRIER_KEYWORDS = {
    "claro": "Claro",
    "vivo": "Vivo",
    "tim": "TIM",
    " oi ": "Oi",
    "oi s.a": "Oi",
    "nextel": "Nextel",
    "algar": "Algar",
    "sercomtel": "Sercomtel",
    "brt": "BRT",
    "sky": "Sky",
    "porto seguro": "Porto Seguro",
    "surf": "Surf Telecom",
    "winity": "Winity",
    "ligga": "Ligga",
    "telemar": "Oi",
    "brasil telecom": "Oi",
    "tim celular": "TIM",
    "telefonica": "Vivo",
    "claro s.a": "Claro",
    "embratel": "Claro",
    "net": "Claro",
}

# ──────────────────────────────────────────────
# Normalização
# ──────────────────────────────────────────────
def normalize_number(raw: str) -> str:
    digits = re.sub(r"\D", "", str(raw))
    if digits.startswith("55") and len(digits) in (12, 13):
        digits = digits[2:]
    return digits


def is_valid_number(number: str) -> bool:
    return len(number) in (10, 11) and number.isdigit()


# ──────────────────────────────────────────────
# Parsing do HTML do ABR Telecom
# ──────────────────────────────────────────────
def parse_html(html: str, number: str) -> dict:
    """
    Extrai operadora do HTML retornado pelo ABR Telecom.
    Usa 4 estratégias em ordem. Se nenhuma funcionar,
    retorna 'parse_inconclusivo' com html_preview para debug.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        full_text_lower = soup.get_text(" ").lower()

        # ── Estratégia 1: seletores CSS por ID/classe comuns ──
        for sel_attr, sel_val in [
            ("id", "resultado"), ("id", "resposta"), ("id", "operadora"),
            ("id", "prestadora"), ("class", "resultado"), ("class", "operadora"),
            ("class", "prestadora"), ("class", "detalhes"), ("class", "info"),
        ]:
            tag = soup.find(True, {sel_attr: sel_val})
            if tag:
                text = tag.get_text(" ", strip=True)
                if text and len(text) > 2:
                    logger.debug("[%s] Parser estratégia 1 (selector %s=%s): %s", number, sel_attr, sel_val, text[:80])
                    return _build_result(number, text, f"selector:{sel_attr}={sel_val}")

        # ── Estratégia 2: tabelas – linha com label de operadora ──
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for i, row in enumerate(rows):
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                header = " ".join(cells).lower()
                if any(k in header for k in ("operadora", "prestadora", "empresa", "detentor")):
                    if len(cells) >= 2:
                        valor = cells[-1] if cells[-1] else cells[1]
                        if valor:
                            logger.debug("[%s] Parser estratégia 2 (tabela inline): %s", number, valor[:80])
                            return _build_result(number, valor, "table_inline")
                    if i + 1 < len(rows):
                        next_cells = [td.get_text(strip=True) for td in rows[i + 1].find_all(["td", "th"])]
                        if next_cells and next_cells[0]:
                            logger.debug("[%s] Parser estratégia 2 (tabela próx linha): %s", number, next_cells[0][:80])
                            return _build_result(number, next_cells[0], "table_next_row")

        # ── Estratégia 3: busca por div/span com texto de operadora ──
        for tag in soup.find_all(["div", "span", "p", "td"]):
            text = tag.get_text(strip=True).lower()
            if any(k in text for k in ("claro", "vivo", "tim", "nextel", "algar")):
                raw = tag.get_text(strip=True)
                if 2 < len(raw) < 80:
                    logger.debug("[%s] Parser estratégia 3 (tag scan): %s", number, raw[:80])
                    return _build_result(number, raw, "tag_scan")

        # ── Estratégia 4: keyword no texto completo ──
        for keyword, carrier_name in CARRIER_KEYWORDS.items():
            if keyword in full_text_lower:
                logger.debug("[%s] Parser estratégia 4 (keyword '%s'): %s", number, keyword, carrier_name)
                return _build_result(number, carrier_name, "keyword_match")

        # ── Número não cadastrado ──
        not_found_phrases = [
            "não encontrado", "nao encontrado", "inválido", "invalido",
            "inexistente", "não cadastrado", "nao cadastrado", "not found",
            "sem resultado", "nenhum resultado",
        ]
        if any(phrase in full_text_lower for phrase in not_found_phrases):
            logger.info("[%s] Número não encontrado na base ANATEL", number)
            return {"numero": number, "operadora": "Não encontrado", "status": "nao_encontrado", "metodo": "not_found_phrase"}

        # ── Fallback: retorna preview para inspeção ──
        preview = soup.get_text(" ", strip=True)[:500]
        logger.warning("[%s] Parse inconclusivo. Preview: %s", number, preview[:200])
        return {
            "numero": number,
            "operadora": "NÃO_IDENTIFICADO",
            "status": "parse_inconclusivo",
            "metodo": "fallback",
            "html_preview": preview,
        }

    except Exception as exc:
        logger.error("[%s] Exceção no parsing: %s", number, exc, exc_info=True)
        return {"numero": number, "operadora": "ERRO_PARSE", "status": str(exc)[:120], "metodo": "exception"}


def _build_result(number: str, carrier: str, method: str) -> dict:
    carrier_clean = carrier.strip()
    # Normaliza pelo dicionário de palavras-chave
    for keyword, canonical in CARRIER_KEYWORDS.items():
        if keyword in carrier_clean.lower():
            carrier_clean = canonical
            break
    return {
        "numero": number,
        "operadora": carrier_clean,
        "status": "sucesso",
        "metodo": method,
    }


# ──────────────────────────────────────────────
# Consulta individual com retry
# ──────────────────────────────────────────────
async def fetch_carrier(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    number: str,
    max_retries: int = 4,
) -> dict:
    async with semaphore:
        for attempt in range(max_retries):
            try:
                jitter = random.uniform(0.05, 0.30)
                await asyncio.sleep(jitter)

                logger.debug("[%s] Requisição ao ABR Telecom (tentativa %d/%d)", number, attempt + 1, max_retries)

                response = await client.post(
                    ABR_URL,
                    data={"telefone": number},
                    headers=HEADERS_BASE,
                    timeout=30,
                    follow_redirects=True,
                )

                logger.debug("[%s] HTTP %s | %d bytes", number, response.status_code, len(response.content))

                if response.status_code == 200:
                    result = parse_html(response.text, number)
                    logger.info("[%s] ✓ operadora=%s status=%s método=%s",
                                number, result.get("operadora"), result.get("status"), result.get("metodo"))
                    return result

                if response.status_code in (429, 503, 502, 500):
                    wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                    logger.warning("[%s] HTTP %s – aguardando %.1fs antes de retry", number, response.status_code, wait)
                    await asyncio.sleep(wait)
                    continue

                logger.error("[%s] HTTP %s inesperado – abandonando", number, response.status_code)
                return {"numero": number, "operadora": "ERRO_HTTP", "status": f"http_{response.status_code}", "metodo": "error"}

            except httpx.TimeoutException:
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.warning("[%s] Timeout (tentativa %d/%d) – retry em %.1fs", number, attempt + 1, max_retries, wait)
                await asyncio.sleep(wait)

            except httpx.RequestError as exc:
                logger.error("[%s] Erro de rede: %s", number, exc)
                if attempt == max_retries - 1:
                    return {"numero": number, "operadora": "ERRO_REDE", "status": str(exc)[:120], "metodo": "network_error"}
                await asyncio.sleep(2 ** attempt)

        logger.error("[%s] Esgotou %d tentativas", number, max_retries)
        return {"numero": number, "operadora": "FALHA", "status": "max_tentativas_excedido", "metodo": "max_retries"}


# ──────────────────────────────────────────────
# Consulta de debug (HTML bruto para inspeção)
# ──────────────────────────────────────────────
async def fetch_raw_html(number: str) -> dict:
    """Retorna o HTML bruto do ABR Telecom para um número. Usado no endpoint /debug."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.post(ABR_URL, data={"telefone": number}, headers=HEADERS_BASE)
            soup = BeautifulSoup(response.text, "html.parser")
            return {
                "numero": number,
                "http_status": response.status_code,
                "html_bruto": response.text[:3000],
                "texto_extraido": soup.get_text(" ", strip=True)[:1000],
                "parse_result": parse_html(response.text, number),
            }
    except Exception as exc:
        return {"numero": number, "erro": str(exc)}


# ──────────────────────────────────────────────
# Processamento em lote
# ──────────────────────────────────────────────
async def process_batch(
    numbers: list[str],
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
) -> list[dict]:
    tasks = [fetch_carrier(client, semaphore, n) for n in numbers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output = []
    for number, result in zip(numbers, results):
        if isinstance(result, Exception):
            logger.error("[%s] Exceção não capturada no gather: %s", number, result)
            output.append({"numero": number, "operadora": "ERRO", "status": str(result)[:120], "metodo": "gather_exception"})
        else:
            output.append(result)
    return output
