import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("pipeline_daily")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# =========================
# Repo root robusto
# =========================
def find_repo_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(6):  # sube hasta 6 niveles
        if (cur / ".git").exists():
            return cur
        if (cur / "README.md").exists():
            return cur
        if (cur / "pyproject.toml").exists() or (cur / "requirements.txt").exists():
            return cur
        cur = cur.parent
    return start.resolve()


BASE_DIR = find_repo_root(Path(__file__).resolve().parent)
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# Config
# =========================
DEFAULT_ENDPOINT = "https://api.copiloto.ai/wicar-report/report-files/vehicle-records"
DEFAULT_SIGNIN = "https://accounts.copiloto.ai/v1/sign-in"

COPILOTO_ENDPOINT = os.getenv("COPILOTO_ENDPOINT", DEFAULT_ENDPOINT).strip()
COPILOTO_SIGNIN_URL = os.getenv("COPILOTO_SIGNIN_URL", DEFAULT_SIGNIN).strip()

# Si Actions te pasa una variable vacía (""), NO sobreescribir el default
if not COPILOTO_ENDPOINT:
    COPILOTO_ENDPOINT = DEFAULT_ENDPOINT
if not COPILOTO_SIGNIN_URL:
    COPILOTO_SIGNIN_URL = DEFAULT_SIGNIN

COPILOTO_EMAIL = os.getenv("COPILOTO_EMAIL", "").strip()
COPILOTO_PASSWORD = os.getenv("COPILOTO_PASSWORD", "").strip()
COPILOTO_TOKEN_ENV = os.getenv("COPILOTO_TOKEN", "").strip()

TIMEOUT_SEC = int(os.getenv("COPILOTO_TIMEOUT_SEC", "60"))
OUT_PREFIX = os.getenv("COPILOTO_OUT_PREFIX", "vehicles_records").strip() or "vehicles_records"


def validate_url(name: str, url: str):
    if not url or not isinstance(url, str):
        raise RuntimeError(f"{name} está vacío.")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise RuntimeError(f"{name} inválida: '{url}' (debe partir con http/https).")


# =========================
# HTTP helpers
# =========================
def make_session() -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    return sess


def fetch_copiloto_token(email: str, password: str, signin_url: str, timeout: int) -> str:
    """POST /v1/sign-in y devuelve token JWT."""
    validate_url("COPILOTO_SIGNIN_URL", signin_url)

    if not (email and password):
        raise RuntimeError("Faltan COPILOTO_EMAIL/COPILOTO_PASSWORD para login (y no hay COPILOTO_TOKEN).")

    sess = make_session()
    payload = {"email": email, "password": password}
    r = sess.post(signin_url, json=payload, timeout=timeout)

    if r.status_code in (401, 403):
        raise RuntimeError("Credenciales Copiloto inválidas (401/403).")
    if r.status_code >= 400:
        raise RuntimeError(f"Login falló ({r.status_code}): {r.text[:300]}")

    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Respuesta no-JSON del login: {r.text[:300]}")

    token = (
        data.get("accessToken")
        or data.get("access_token")
        or data.get("token")
        or (data.get("data") or {}).get("token")
        or (data.get("data") or {}).get("accessToken")
        or ""
    )

    if not isinstance(token, str) or not token.strip():
        raise RuntimeError(f"No encontré token en login. keys={list(data.keys())}")

    token = token.strip()
    log.info("Token obtenido (len=%d).", len(token))
    return token


def resolve_token() -> str:
    """Si hay COPILOTO_TOKEN en entorno, úsalo. Si no, login con email/pass."""
    if COPILOTO_TOKEN_ENV:
        log.info("Usando COPILOTO_TOKEN desde entorno.")
        return COPILOTO_TOKEN_ENV

    return fetch_copiloto_token(
        COPILOTO_EMAIL,
        COPILOTO_PASSWORD,
        COPILOTO_SIGNIN_URL,
        timeout=TIMEOUT_SEC,
    )


def download_vehicle_records_csv(token: str) -> bytes:
    """Descarga el CSV del endpoint."""
    validate_url("COPILOTO_ENDPOINT", COPILOTO_ENDPOINT)

    sess = make_session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/csv,application/octet-stream,*/*",
    }

    r = sess.get(COPILOTO_ENDPOINT, headers=headers, timeout=TIMEOUT_SEC)

    if r.status_code in (401, 403):
        raise RuntimeError("No autorizado (401/403). Token inválido o expirado.")
    if r.status_code >= 400:
        raise RuntimeError(f"Descarga falló ({r.status_code}): {r.text[:300]}")

    content_type = (r.headers.get("Content-Type") or "").lower()
    log.info("Descarga OK. Content-Type=%s size=%d", content_type, len(r.content))
    return r.content


def main():
    log.info("Repo root: %s", BASE_DIR)
    log.info("DATA_DIR:  %s", DATA_DIR)
    log.info("Endpoint:  %s", COPILOTO_ENDPOINT)
    log.info("Signin:    %s", COPILOTO_SIGNIN_URL)

    token = resolve_token()
    blob = download_vehicle_records_csv(token)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_file = DATA_DIR / f"{OUT_PREFIX}_{ts}.csv"
    out_latest = DATA_DIR / f"{OUT_PREFIX}_latest.csv"

    out_file.write_bytes(blob)
    out_latest.write_bytes(blob)

    log.info("Guardado:   %s", out_file.name)
    log.info("Actualizado:%s", out_latest.name)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("ERROR: %s", e)
        sys.exit(1)
