"""
Carrier Lookup API – FastAPI
Endpoints:
  POST /process         → sobe Excel, inicia job em background
  GET  /status/{id}    → progresso do job
  GET  /download/{id}  → baixa Excel de resultado
  GET  /debug/{numero} → testa 1 número e mostra HTML bruto do ABR Telecom
  GET  /logs           → últimas linhas do log para monitoramento
  GET  /health         → healthcheck para n8n/Coolify
"""
import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, PlainTextResponse

from carrier_lookup import process_batch, fetch_raw_html
from excel_handler import read_numbers_from_excel, write_results_to_excel

# ──────────────────────────────────────────────
# Logging com rotação – máx 15 MB total em disco
# ──────────────────────────────────────────────
LOG_DIR = Path("/tmp/carrier_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "carrier.log"

_rotating_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5 MB por arquivo
    backupCount=3,              # 3 arquivos = 15 MB máximo total
    encoding="utf-8",
)
_rotating_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
)

# Configura o root logger para capturar tudo
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(_rotating_handler)
root_logger.addHandler(_console_handler)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# App
# ──────────────────────────────────────────────
app = FastAPI(
    title="Carrier Lookup API",
    description="Consulta em massa de operadoras via ABR Telecom / ANATEL",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────
# Diretórios
# ──────────────────────────────────────────────
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", "/tmp/carrier_checkpoints"))
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/tmp/carrier_outputs"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JOBS_DIR = Path(os.getenv("JOBS_DIR", "/tmp/carrier_jobs"))
JOBS_DIR.mkdir(parents=True, exist_ok=True)

MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
JOB_TTL_HOURS = int(os.getenv("JOB_TTL_HOURS", "24"))

# ──────────────────────────────────────────────
# Storage de jobs (memória + disco)
# ──────────────────────────────────────────────
JOBS: dict[str, dict] = {}


def _job_meta_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.meta.json"


def _save_job_meta(job_id: str):
    try:
        with open(_job_meta_path(job_id), "w") as f:
            json.dump(JOBS[job_id], f)
    except Exception as exc:
        logger.error("Falha ao salvar meta do job %s: %s", job_id, exc)


def _load_all_jobs():
    loaded, discarded = 0, 0
    cutoff = datetime.now() - timedelta(hours=JOB_TTL_HOURS)
    for meta_file in JOBS_DIR.glob("*.meta.json"):
        job_id = meta_file.stem.replace(".meta", "")
        try:
            with open(meta_file) as f:
                job = json.load(f)
            terminal = job.get("status") in ("completed", "cancelled", "error")
            created_str = job.get("created_at", "")
            if terminal and created_str:
                if datetime.fromisoformat(created_str) < cutoff:
                    _purge_job_files(job_id, job)
                    discarded += 1
                    continue
            JOBS[job_id] = job
            loaded += 1
        except Exception as exc:
            logger.warning("Falha ao carregar meta do job %s: %s", job_id, exc)
    logger.info("Jobs restaurados: %d | expirados descartados: %d", loaded, discarded)


def _purge_job_files(job_id: str, job: dict | None = None):
    _job_meta_path(job_id).unlink(missing_ok=True)
    _checkpoint_path(job_id).unlink(missing_ok=True)
    output = Path(job["output_path"]) if job and job.get("output_path") else OUTPUT_DIR / f"{job_id}.xlsx"
    output.unlink(missing_ok=True)


# ──────────────────────────────────────────────
# Limpeza periódica a cada hora
# ──────────────────────────────────────────────
async def _cleanup_loop():
    while True:
        await asyncio.sleep(3600)
        cutoff = datetime.now() - timedelta(hours=JOB_TTL_HOURS)
        to_remove = [
            jid for jid, j in JOBS.items()
            if j.get("status") in ("completed", "cancelled", "error")
            and j.get("created_at")
            and datetime.fromisoformat(j["created_at"]) < cutoff
        ]
        for job_id in to_remove:
            job = JOBS.pop(job_id, None)
            _purge_job_files(job_id, job)
        if to_remove:
            logger.info("Limpeza automática: %d job(s) removido(s)", len(to_remove))


@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("API iniciando – MAX_CONCURRENT=%d BATCH_SIZE=%d", MAX_CONCURRENT, BATCH_SIZE)
    _load_all_jobs()
    asyncio.create_task(_cleanup_loop())
    logger.info("API pronta.")


# ──────────────────────────────────────────────
# Checkpoint de resultados
# ──────────────────────────────────────────────
def _checkpoint_path(job_id: str) -> Path:
    return CHECKPOINT_DIR / f"{job_id}.json"


def _load_checkpoint(job_id: str) -> dict:
    path = _checkpoint_path(job_id)
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as exc:
            logger.warning("Falha ao carregar checkpoint %s: %s", job_id, exc)
    return {}


def _save_checkpoint(job_id: str, results: dict):
    try:
        with open(_checkpoint_path(job_id), "w") as f:
            json.dump(results, f)
    except Exception as exc:
        logger.error("Falha ao salvar checkpoint %s: %s", job_id, exc)


def _delete_checkpoint(job_id: str):
    _checkpoint_path(job_id).unlink(missing_ok=True)


# ──────────────────────────────────────────────
# Worker principal
# ──────────────────────────────────────────────
async def _run_job(job_id: str, numbers: list[str], invalid: list[str]):
    job = JOBS[job_id]
    job["status"] = "processing"
    job["started_at"] = datetime.now().isoformat()
    _save_job_meta(job_id)

    results: dict[str, dict] = _load_checkpoint(job_id)
    pending = [n for n in numbers if n not in results]

    logger.info("Job %s | total=%d | já feitos=%d | pendentes=%d",
                job_id[:8], len(numbers), len(results), len(pending))

    job["processed"] = len(results)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    try:
        async with httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=MAX_CONCURRENT + 20,
                max_keepalive_connections=MAX_CONCURRENT,
                keepalive_expiry=30,
            ),
            timeout=httpx.Timeout(connect=10, read=30, write=10, pool=5),
        ) as client:
            for batch_start in range(0, len(pending), BATCH_SIZE):
                if job.get("cancelled"):
                    logger.info("Job %s cancelado", job_id[:8])
                    job["status"] = "cancelled"
                    _save_job_meta(job_id)
                    return

                batch = pending[batch_start: batch_start + BATCH_SIZE]
                lote_num = (batch_start // BATCH_SIZE) + 1
                total_lotes = (len(pending) + BATCH_SIZE - 1) // BATCH_SIZE
                logger.info("Job %s | lote %d/%d (%d números)", job_id[:8], lote_num, total_lotes, len(batch))

                batch_results = await process_batch(batch, semaphore, client)

                sucesso = sum(1 for r in batch_results if r.get("status") == "sucesso")
                inconclusivo = sum(1 for r in batch_results if r.get("status") == "parse_inconclusivo")
                erros = sum(1 for r in batch_results if "erro" in r.get("status", "").lower())

                logger.info("Job %s | lote %d resultado: ✓%d inconclusivo:%d erro:%d",
                            job_id[:8], lote_num, sucesso, inconclusivo, erros)

                for res in batch_results:
                    results[res["numero"]] = res

                job["processed"] = len(results)
                _save_checkpoint(job_id, results)
                _save_job_meta(job_id)

    except Exception as exc:
        logger.exception("Erro fatal no job %s", job_id[:8])
        job["status"] = "error"
        job["error"] = str(exc)
        _save_job_meta(job_id)
        return

    # Gerar Excel final
    try:
        logger.info("Job %s | gerando Excel...", job_id[:8])
        xlsx_bytes = write_results_to_excel(numbers, results, invalid)
        output_path = OUTPUT_DIR / f"{job_id}.xlsx"
        output_path.write_bytes(xlsx_bytes)

        stats = {
            "total": len(numbers),
            "sucesso": sum(1 for r in results.values() if r.get("status") == "sucesso"),
            "nao_encontrado": sum(1 for r in results.values() if r.get("status") == "nao_encontrado"),
            "inconclusivo": sum(1 for r in results.values() if r.get("status") == "parse_inconclusivo"),
            "erros": sum(1 for r in results.values() if "erro" in r.get("status", "").lower()),
            "invalidos": len(invalid),
        }

        job["status"] = "completed"
        job["completed_at"] = datetime.now().isoformat()
        job["output_path"] = str(output_path)
        job["stats"] = stats
        _save_job_meta(job_id)
        _delete_checkpoint(job_id)

        logger.info("Job %s | CONCLUÍDO | %s", job_id[:8], stats)

    except Exception as exc:
        logger.exception("Erro ao gerar Excel para job %s", job_id[:8])
        job["status"] = "error"
        job["error"] = f"Falha na geração do Excel: {exc}"
        _save_job_meta(job_id)


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────

@app.get("/health", tags=["Sistema"])
async def health():
    return {
        "status": "ok",
        "jobs_ativos": sum(1 for j in JOBS.values() if j["status"] == "processing"),
        "jobs_total": len(JOBS),
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/logs", tags=["Sistema"], response_class=PlainTextResponse)
async def get_logs(linhas: int = 200):
    """
    Retorna as últimas N linhas do arquivo de log.
    Útil para debug sem precisar de acesso ao servidor.
    Acesse: GET /logs?linhas=500
    """
    try:
        if not LOG_FILE.exists():
            return "Nenhum log disponível ainda."
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        last_lines = all_lines[-linhas:]
        return "".join(last_lines)
    except Exception as exc:
        return f"Erro ao ler logs: {exc}"


@app.get("/debug/{numero}", tags=["Debug"])
async def debug_numero(numero: str):
    """
    Testa a consulta de UM número e retorna o HTML bruto do ABR Telecom.
    Use para verificar se o parsing está funcionando corretamente.
    Exemplo: GET /debug/98991234567
    """
    logger.info("DEBUG solicitado para número: %s", numero)
    result = await fetch_raw_html(numero)
    return result


@app.post("/process", tags=["Processamento"])
async def start_job(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, detail="Apenas arquivos .xlsx ou .xls são aceitos.")

    content = await file.read()
    logger.info("Arquivo recebido: %s (%.1f KB)", file.filename, len(content) / 1024)

    try:
        valid_numbers, invalid_numbers = read_numbers_from_excel(content)
    except Exception as exc:
        logger.error("Erro ao ler Excel: %s", exc)
        raise HTTPException(422, detail=f"Erro ao ler o Excel: {exc}")

    if not valid_numbers:
        raise HTTPException(400, detail="Nenhum número válido encontrado na primeira coluna.")

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "status": "queued",
        "total": len(valid_numbers),
        "processed": 0,
        "invalidos": len(invalid_numbers),
        "created_at": datetime.now().isoformat(),
        "filename": file.filename,
    }
    _save_job_meta(job_id)
    background_tasks.add_task(_run_job, job_id, valid_numbers, invalid_numbers)

    logger.info("Job %s criado | válidos=%d | inválidos=%d", job_id[:8], len(valid_numbers), len(invalid_numbers))

    return {
        "job_id": job_id,
        "total_validos": len(valid_numbers),
        "total_invalidos": len(invalid_numbers),
        "mensagem": "Processamento iniciado.",
    }


@app.get("/status/{job_id}", tags=["Processamento"])
async def job_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, detail="Job não encontrado.")

    job = JOBS[job_id]
    total = job["total"]
    processed = job["processed"]
    progress = round((processed / total) * 100, 1) if total else 0

    eta_seconds: Optional[int] = None
    if job.get("started_at") and job["status"] == "processing" and processed > 0:
        elapsed = (datetime.now() - datetime.fromisoformat(job["started_at"])).total_seconds()
        rate = processed / elapsed
        remaining = total - processed
        eta_seconds = int(remaining / rate) if rate > 0 else None

    return {
        "job_id": job_id,
        "status": job["status"],
        "progresso_percent": progress,
        "processados": processed,
        "total": total,
        "invalidos": job.get("invalidos", 0),
        "eta_segundos": eta_seconds,
        "criado_em": job.get("created_at"),
        "iniciado_em": job.get("started_at"),
        "concluido_em": job.get("completed_at"),
        "stats": job.get("stats"),
        "erro": job.get("error"),
    }


@app.get("/download/{job_id}", tags=["Processamento"])
async def download_result(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, detail="Job não encontrado.")

    job = JOBS[job_id]
    if job["status"] != "completed":
        raise HTTPException(400, detail=f"Job ainda não concluído. Status: {job['status']}")

    output_path = Path(job["output_path"])
    if not output_path.exists():
        raise HTTPException(500, detail="Arquivo de saída não encontrado.")

    xlsx_bytes = output_path.read_bytes()
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="operadoras_{job_id[:8]}.xlsx"'},
    )


@app.delete("/job/{job_id}", tags=["Processamento"])
async def cancel_job(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, detail="Job não encontrado.")
    JOBS[job_id]["cancelled"] = True
    _save_job_meta(job_id)
    return {"mensagem": f"Cancelamento solicitado para o job {job_id[:8]}."}


@app.get("/jobs", tags=["Sistema"])
async def list_jobs():
    return [
        {
            "job_id": jid,
            "status": j["status"],
            "total": j["total"],
            "processados": j["processed"],
            "criado_em": j["created_at"],
            "stats": j.get("stats"),
        }
        for jid, j in JOBS.items()
    ]
