"""
Carrier Lookup Engine
Consulta operadora via ABR Telecom (ANATEL) com suporte a concorrência e retry.
"""

import asyncio
import logging
import random
import re

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Configurações
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
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://consultanumero.abrtelecom.com.br",
    "Referer": "https://consultanumero.abrtelecom.com.br/",
    "Content-Type": "application/x-www-form-urlencoded",
}

# Operadoras conhecidas para fallback de parsing
CARRIER_KEYWORDS = {
    "claro": "Claro",
    "vivo": "Vivo",
    "tim": "TIM",
    " oi ": "Oi",
    "nextel": "Nextel",
    "algar": "Algar",
    "sercomtel": "Sercomtel",
    "brt": "BRT",
    "sky": "Sky",
    "porto seguro": "Porto Seguro",
    "surf": "Surf Telecom",
    "winity": "Winity",
    "ligga": "Ligga",
}


# ──────────────────────────────────────────────
# Normalização
# ──────────────────────────────────────────────
def normalize_number(raw: str) -> str:
    """Remove tudo que não for dígito e descarta o +55 do início."""
    digits = re.sub(r"\D", "", str(raw))
    if digits.startswith("55") and len(digits) in (12, 13):
        digits = digits[2:]
    return digits


def is_valid_number(number: str) -> bool:
    return len(number) in (10, 11) and number.isdigit()


# ──────────────────────────────────────────────
# Parsing da resposta HTML
# ──────────────────────────────────────────────
def parse_html(html: str, number: str) -> dict:
    """
    Extrai o nome da operadora do HTML retornado pelo ABR Telecom.
    Usa múltiplas estratégias em ordem de confiabilidade.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Estratégia 1: buscar por IDs/classes comuns em portais de consulta
        selectors = [
            {"id": "resultado"},
            {"id": "resposta"},
            {"id": "operadora"},
            {"class_": "resultado"},
            {"class_": "operadora"},
            {"class_": "prestadora"},
        ]
        for sel in selectors:
            tag = soup.find(True, sel) if "class_" not in sel else soup.find(
                True, {"class": sel["class_"]}
            )
            if tag:
                text = tag.get_text(" ", strip=True)
                if text:
                    return _build_result(number, text, "selector")

        # Estratégia 2: percorrer tabelas buscando linha com "operadora"/"prestadora"
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for i, row in enumerate(rows):
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                header = " ".join(cells).lower()
                if any(k in header for k in ("operadora", "prestadora", "empresa")):
                    # O valor provavelmente está na célula ao lado ou na próxima linha
                    if len(cells) >= 2:
                        return _build_result(number, cells[-1], "table_inline")
                    if i + 1 < len(rows):
                        next_cells = [
                            td.get_text(strip=True)
                            for td in rows[i + 1].find_all(["td", "th"])
                        ]
                        if next_cells:
                            return _build_result(number, next_cells[0], "table_next_row")

        # Estratégia 3: buscar por palavras-chave de operadoras no texto completo
        full_text = soup.get_text(" ").lower()
        for keyword, carrier_name in CARRIER_KEYWORDS.items():
            if keyword in full_text:
                return _build_result(number, carrier_name, "keyword_match")

        # Estratégia 4: número não encontrado / inválido
        if any(
            phrase in full_text
            for phrase in ("não encontrado", "inválido", "inexistente", "não cadastrado")
        ):
            return {
                "numero": number,
                "operadora": "Não encontrado",
                "status": "nao_encontrado",
            }

        # Fallback: retornar trecho do HTML para depuração
        preview = soup.get_text(" ", strip=True)[:300]
        logger.warning("Parse inconclusivo para %s | preview: %s", number, preview)
        return {
            "numero": number,
            "operadora": "NÃO_IDENTIFICADO",
            "status": "parse_inconclusivo",
            "html_preview": preview,
        }

    except Exception as exc:
        logger.error("Erro ao fazer parse do número %s: %s", number, exc)
        return {"numero": number, "operadora": "ERRO_PARSE", "status": str(exc)[:120]}


def _build_result(number: str, carrier: str, method: str) -> dict:
    return {"numero": number, "operadora": carrier.strip(), "status": "sucesso", "metodo": method}


# ──────────────────────────────────────────────
# Consulta individual (async)
# ──────────────────────────────────────────────
async def fetch_carrier(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    number: str,
    max_retries: int = 4,
) -> dict:
    """
    Consulta a operadora de um único número com retry e backoff exponencial.
    """
    async with semaphore:
        for attempt in range(max_retries):
            try:
                # Pequeno delay aleatório entre 50-300ms para não parecer bot
                await asyncio.sleep(random.uniform(0.05, 0.30))

                response = await client.post(
                    ABR_URL,
                    data={"telefone": number},
                    headers=HEADERS_BASE,
                    timeout=30,
                    follow_redirects=True,
                )

                if response.status_code == 200:
                    return parse_html(response.text, number)

                if response.status_code in (429, 503, 502):
                    wait = (2**attempt) + random.uniform(0.5, 2.0)
                    logger.warning(
                        "Rate limit HTTP %s p/ %s | tentativa %d | aguardando %.1fs",
                        response.status_code, number, attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    continue

                logger.error("HTTP %s inesperado para %s", response.status_code, number)

            except httpx.TimeoutException:
                wait = (2**attempt) + random.uniform(0, 1)
                logger.warning("Timeout %s | tentativa %d | aguardando %.1fs", number, attempt + 1, wait)
                await asyncio.sleep(wait)

            except httpx.RequestError as exc:
                logger.error("Erro de rede %s: %s", number, exc)
                if attempt == max_retries - 1:
                    return {"numero": number, "operadora": "ERRO_REDE", "status": str(exc)[:120]}
                await asyncio.sleep(2**attempt)

        return {"numero": number, "operadora": "FALHA", "status": "max_tentativas_excedido"}


# ──────────────────────────────────────────────
# Processamento em lote
# ──────────────────────────────────────────────
async def process_batch(
    numbers: list[str],
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
) -> list[dict]:
    """Processa uma lista de números de forma concorrente."""
    tasks = [fetch_carrier(client, semaphore, n) for n in numbers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output = []
    for number, result in zip(numbers, results):
        if isinstance(result, Exception):
            output.append({"numero": number, "operadora": "ERRO", "status": str(result)[:120]})
        else:
            output.append(result)
    return output
