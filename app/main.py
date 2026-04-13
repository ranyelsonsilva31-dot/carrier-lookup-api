"""
Carrier Lookup API – FastAPI
Endpoints:
  POST /process       → sobe Excel, inicia job em background
  GET  /status/{id}  → progresso do job
  GET  /download/{id}→ baixa Excel de resultado
  GET  /health        → healthcheck para n8n/Coolify
"""
import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from carrier_lookup import process_batch
from excel_handler import read_numbers_from_excel, write_results_to_excel

# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Carrier Lookup API",
    description="Consulta em massa de operadoras via ABR Telecom / ANATEL",
    version="1.0.0",
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

# Diretório para persistência dos metadados dos jobs
JOBS_DIR = Path(os.getenv("JOBS_DIR", "/tmp/carrier_jobs"))
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# Concorrência: quantas requisições ao mesmo tempo
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "50"))

# Tamanho do lote para checkpoint
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

# Horas até um job concluído/cancelado/erro ser removido da memória e do disco
JOB_TTL_HOURS = int(os.getenv("JOB_TTL_HOURS", "24"))

# ──────────────────────────────────────────────
# Storage de jobs – PERSISTENTE EM DISCO
# ──────────────────────────────────────────────
JOBS: dict[str, dict] = {}


def _job_meta_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.meta.json"


def _save_job_meta(job_id: str):
    """Persiste os metadados do job em disco."""
    try:
        with open(_job_meta_path(job_id), "w") as f:
            json.dump(JOBS[job_id], f)
    except Exception as exc:
        logger.error("Falha ao salvar meta do job %s: %s", job_id, exc)


def _load_all_jobs():
    """
    Carrega todos os jobs persistidos ao iniciar a aplicação.
    Jobs expirados (mais antigos que JOB_TTL_HOURS) são descartados na carga.
    """
    loaded = 0
    discarded = 0
    cutoff = datetime.now() - timedelta(hours=JOB_TTL_HOURS)

    for meta_file in JOBS_DIR.glob("*.meta.json"):
        job_id = meta_file.stem.replace(".meta", "")
        try:
            with open(meta_file) as f:
                job = json.load(f)

            # Se o job está em estado terminal e passou do TTL, descarta
            terminal = job.get("status") in ("completed", "cancelled", "error")
            created_str = job.get("created_at", "")
            if terminal and created_str:
                created_at = datetime.fromisoformat(created_str)
                if created_at < cutoff:
                    _purge_job_files(job_id, job)
                    discarded += 1
                    continue

            JOBS[job_id] = job
            loaded += 1
        except Exception as exc:
            logger.warning("Falha ao carregar meta do job %s: %s", job_id, exc)

    logger.info(
        "Jobs restaurados do disco: %d | expirados descartados: %d",
        loaded, discarded,
    )


def _purge_job_files(job_id: str, job: dict | None = None):
    """Remove todos os arquivos em disco relacionados a um job."""
    _job_meta_path(job_id).unlink(missing_ok=True)
    _checkpoint_path(job_id).unlink(missing_ok=True)
    if job and job.get("output_path"):
        Path(job["output_path"]).unlink(missing_ok=True)
    else:
        (OUTPUT_DIR / f"{job_id}.xlsx").unlink(missing_ok=True)


# ──────────────────────────────────────────────
# Limpeza periódica (evita crescimento infinito de memória)
# ──────────────────────────────────────────────
async def _cleanup_loop():
    """
    Roda em background a cada hora.
    Remove da memória (e do disco) jobs terminais mais antigos que JOB_TTL_HOURS.
    """
    while True:
        await asyncio.sleep(3600)  # espera 1 hora
        cutoff = datetime.now() - timedelta(hours=JOB_TTL_HOURS)
        to_remove = []

        for job_id, job in JOBS.items():
            terminal = job.get("status") in ("completed", "cancelled", "error")
            created_str = job.get("created_at", "")
            if not (terminal and created_str):
                continue
            try:
                if datetime.fromisoformat(created_str) < cutoff:
                    to_remove.append(job_id)
            except ValueError:
                pass

        for job_id in to_remove:
            job = JOBS.pop(job_id, None)
            _purge_job_files(job_id, job)

        if to_remove:
            logger.info("Limpeza automática: %d job(s) removido(s)", len(to_remove))


@app.on_event("startup")
async def startup_event():
    _load_all_jobs()
    asyncio.create_task(_cleanup_loop())


# ──────────────────────────────────────────────
# Helpers de checkpoint de resultados
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
# Worker principal (roda em background)
# ──────────────────────────────────────────────
async def _run_job(job_id: str, numbers: list[str], invalid: list[str]):
    job = JOBS[job_id]
    job["status"] = "processing"
    job["started_at"] = datetime.now().isoformat()
    _save_job_meta(job_id)

    # Carregar progresso anterior (se houver)
    results: dict[str, dict] = _load_checkpoint(job_id)
    pending = [n for n in numbers if n not in results]
    already_done = len(results)
    job["processed"] = already_done

    logger.info(
        "Job %s | total=%d | checkpoint=%d | pendentes=%d",
        job_id, len(numbers), already_done, len(pending),
    )

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
                    logger.info("Job %s cancelado pelo usuário", job_id)
                    job["status"] = "cancelled"
                    _save_job_meta(job_id)
                    return

                batch = pending[batch_start : batch_start + BATCH_SIZE]
                logger.info(
                    "Job %s | lote %d-%d de %d",
                    job_id, batch_start + 1, batch_start + len(batch), len(pending),
                )
                batch_results = await process_batch(batch, semaphore, client)
                for res in batch_results:
                    results[res["numero"]] = res

                job["processed"] = len(results)
                _save_checkpoint(job_id, results)
                _save_job_meta(job_id)  # persiste progresso intermediário

    except Exception as exc:
        logger.exception("Erro fatal no job %s", job_id)
        job["status"] = "error"
        job["error"] = str(exc)
        _save_job_meta(job_id)
        return

    # Gerar Excel final
    try:
        logger.info("Job %s | gerando Excel de saída...", job_id)
        xlsx_bytes = write_results_to_excel(numbers, results, invalid)
        output_path = OUTPUT_DIR / f"{job_id}.xlsx"
        output_path.write_bytes(xlsx_bytes)

        job["status"] = "completed"
        job["completed_at"] = datetime.now().isoformat()
        job["output_path"] = str(output_path)
        job["stats"] = {
            "total": len(numbers),
            "sucesso": sum(1 for r in results.values() if r.get("status") == "sucesso"),
            "nao_encontrado": sum(1 for r in results.values() if r.get("status") == "nao_encontrado"),
            "erros": sum(1 for r in results.values() if "erro" in r.get("status", "").lower()),
            "invalidos": len(invalid),
        }
        _save_job_meta(job_id)
        _delete_checkpoint(job_id)
        logger.info("Job %s | CONCLUÍDO | stats=%s", job_id, job["stats"])

    except Exception as exc:
        logger.exception("Erro ao gerar Excel para job %s", job_id)
        job["status"] = "error"
        job["error"] = f"Falha na geração do Excel: {exc}"
        _save_job_meta(job_id)


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────
@app.get("/health", tags=["Sistema"])
async def health():
    """Healthcheck – usado pelo Coolify e n8n."""
    return {
        "status": "ok",
        "jobs_ativos": sum(1 for j in JOBS.values() if j["status"] == "processing"),
        "jobs_total": len(JOBS),
        "timestamp": datetime.now().isoformat(),
    }


@app.post("/process", tags=["Processamento"])
async def start_job(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="Planilha Excel com números na primeira coluna"),
):
    """
    Recebe um Excel, lê os números e inicia a consulta em background.
    Retorna um `job_id` para acompanhar o progresso.
    """
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, detail="Apenas arquivos .xlsx ou .xls são aceitos.")

    content = await file.read()
    try:
        valid_numbers, invalid_numbers = read_numbers_from_excel(content)
    except Exception as exc:
        raise HTTPException(422, detail=f"Erro ao ler o Excel: {exc}")

    if not valid_numbers:
        raise HTTPException(
            400,
            detail="Nenhum número válido encontrado. Certifique-se de que os números estão na primeira coluna.",
        )

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "status": "queued",
        "total": len(valid_numbers),
        "processed": 0,
        "invalidos": len(invalid_numbers),
        "created_at": datetime.now().isoformat(),
        "filename": file.filename,
    }
    _save_job_meta(job_id)  # persiste imediatamente ao criar

    background_tasks.add_task(_run_job, job_id, valid_numbers, invalid_numbers)
    logger.info(
        "Job %s criado | arquivo=%s | válidos=%d | inválidos=%d",
        job_id, file.filename, len(valid_numbers), len(invalid_numbers),
    )
    return {
        "job_id": job_id,
        "total_validos": len(valid_numbers),
        "total_invalidos": len(invalid_numbers),
        "mensagem": "Processamento iniciado. Use GET /status/{job_id} para acompanhar.",
    }


@app.get("/status/{job_id}", tags=["Processamento"])
async def job_status(job_id: str):
    """
    Retorna o status e progresso atual do job.
    Quando `status == 'completed'`, faça GET /download/{job_id}.
    """
    if job_id not in JOBS:
        raise HTTPException(404, detail="Job não encontrado.")

    job = JOBS[job_id]
    total = job["total"]
    processed = job["processed"]
    progress = round((processed / total) * 100, 1) if total else 0

    # Estimativa de tempo restante
    eta_seconds: Optional[int] = None
    if job.get("started_at") and job["status"] == "processing" and processed > 0:
        elapsed = (
            datetime.now() - datetime.fromisoformat(job["started_at"])
        ).total_seconds()
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
    """Baixa o Excel de resultado (disponível apenas quando status == 'completed')."""
    if job_id not in JOBS:
        raise HTTPException(404, detail="Job não encontrado.")

    job = JOBS[job_id]
    if job["status"] != "completed":
        raise HTTPException(
            400,
            detail=f"Job ainda não concluído. Status atual: {job['status']}",
        )

    output_path = Path(job["output_path"])
    if not output_path.exists():
        raise HTTPException(500, detail="Arquivo de saída não encontrado no servidor.")

    xlsx_bytes = output_path.read_bytes()
    filename = f"operadoras_{job_id[:8]}.xlsx"
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.delete("/job/{job_id}", tags=["Processamento"])
async def cancel_job(job_id: str):
    """Cancela um job em andamento."""
    if job_id not in JOBS:
        raise HTTPException(404, detail="Job não encontrado.")
    JOBS[job_id]["cancelled"] = True
    _save_job_meta(job_id)
    return {"mensagem": f"Cancelamento solicitado para o job {job_id}."}


@app.get("/jobs", tags=["Sistema"])
async def list_jobs():
    """Lista todos os jobs (útil para debugging)."""
    return [
        {
            "job_id": jid,
            "status": j["status"],
            "total": j["total"],
            "processados": j["processed"],
            "criado_em": j["created_at"],
        }
        for jid, j in JOBS.items()
    ]
