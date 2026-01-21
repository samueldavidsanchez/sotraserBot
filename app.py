import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import plotly.graph_objects as go

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Conectividad — Cliente", layout="wide")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUTS_DIR = BASE_DIR / "inputs"

# Si NO usas master_ready.csv, déjalo así (no existe y no se usará)
STATUS_FIXED = Path("___no_usar___")
MASTER_XLSX = INPUTS_DIR / "master_Flota.xlsx"

STATUS_PREFIX = "vehicles_records_"

ORDER4 = ["Conectado 0-2", "Intermitente 3-14", "Limitado 15-30+", "Desconectado 31+"]
PROBLEM_LABELS = ["Intermitente 3-14", "Limitado 15-30+", "Desconectado 31+"]

# =========================
# HELPERS
# =========================
def norm_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def ensure_file_exists(p: Path, friendly: str):
    if not p.exists():
        st.error(f"No existe el archivo: {p}\n\nCrea/copia **{friendly}** en esa ruta.")
        st.stop()

def normalize_status_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    if "imei" in df.columns and "IMEI" not in df.columns:
        rename_map["imei"] = "IMEI"
    if "vin" in df.columns and "VIN" not in df.columns:
        rename_map["vin"] = "VIN"
    if "patente" in df.columns and "license_plate" not in df.columns:
        rename_map["patente"] = "license_plate"
    if rename_map:
        df = df.rename(columns=rename_map)
    return df

def latest_csv_by_prefix(folder: Path, prefix: str) -> Path | None:
    files = sorted(folder.glob(f"{prefix}*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def clasificar_4rangos(ts: pd.Series, dias: pd.Series) -> pd.Categorical:
    """
    4 rangos:
      - Conectado 0-2
      - Intermitente 3-14
      - Limitado 15-30+
      - Desconectado 31+ (incluye NaT/Nunca)
    """
    out = pd.Series(index=dias.index, dtype="object")

    is_na_ts = ts.isna()
    out[is_na_ts] = "Desconectado 31+"

    m = ~is_na_ts
    out[m & (dias <= 2)]                           = "Conectado 0-2"
    out[m & dias.between(3, 14, inclusive="both")]  = "Intermitente 3-14"
    out[m & dias.between(15, 30, inclusive="both")] = "Limitado 15-30+"
    out[m & (dias >= 31)]                          = "Desconectado 31+"

    return pd.Categorical(out, categories=ORDER4, ordered=True)

def safe_pct(num, den):
    den = float(den) if den else 0.0
    return round(float(num) / den * 100, 2) if den > 0 else 0.0

def compute_connectivity(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_status_columns(df)

    if "IMEI" not in df.columns:
        raise RuntimeError("El CSV no trae IMEI (ni imei).")

    # timestamps UTC -> naive
    for c in ["can_timestamp", "gps_timestamp", "last_update_utc"]:
        if c in df.columns:
            ts = pd.to_datetime(df[c], errors="coerce", utc=True)
            df[c] = ts.dt.tz_localize(None)

    today = pd.Timestamp.now().normalize()

    # CAN
    if "can_timestamp" in df.columns:
        df["days_can"] = (today - df["can_timestamp"].dt.normalize()).dt.days
    else:
        df["can_timestamp"] = pd.NaT
        df["days_can"] = np.nan
    df["days_can"] = pd.to_numeric(df["days_can"], errors="coerce").astype("Int64")
    df["estado_telemetria"] = clasificar_4rangos(df["can_timestamp"], df["days_can"])

    # GPS global
    if "gps_timestamp" in df.columns:
        df["days_gps"] = (today - df["gps_timestamp"].dt.normalize()).dt.days
    else:
        df["gps_timestamp"] = pd.NaT
        df["days_gps"] = np.nan
    df["days_gps"] = pd.to_numeric(df["days_gps"], errors="coerce").astype("Int64")
    df["gps_status_any"] = clasificar_4rangos(df["gps_timestamp"], df["days_gps"])

    return df

def gauge_card_v2(
    title: str,
    subtitle: str,
    ok_pct: float,
    ok_count: int,
    total: int,
    thresholds=(80, 60),
    bg="#0b1220",
    track="#172033",
    txt="#e5e7eb",
    sub="#94a3b8",
):
    ok_pct = float(ok_pct or 0.0)
    ok_pct = max(0.0, min(100.0, ok_pct))

    total = int(total or 0)
    ok_count = int(ok_count or 0)
    ok_count = max(0, min(total, ok_count))
    offline_count = max(0, total - ok_count)
    offline_pct = round(100.0 - ok_pct, 2) if total > 0 else 0.0

    green, yellow = thresholds
    if ok_pct >= green:
        arc_color = "#22c55e"
    elif ok_pct >= yellow:
        arc_color = "#f59e0b"
    else:
        arc_color = "#ef4444"

    st.markdown(
        f"""
        <div style="background:{bg}; padding:16px 16px 12px 16px; border-radius:16px; border:1px solid #1f2a44;">
          <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <div>
              <div style="color:{txt}; font-weight:800; font-size:18px; line-height:1.1;">{title}</div>
              <div style="color:{sub}; font-size:12px; margin-top:4px;">{subtitle}</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    fig = go.Figure()
    fig.add_trace(go.Pie(
        values=[ok_pct, 100 - ok_pct],
        hole=0.72,
        rotation=180,
        direction="clockwise",
        marker=dict(colors=[arc_color, track]),
        textinfo="none",
        sort=False,
        showlegend=False,
    ))
    fig.update_layout(
        height=260,
        margin=dict(l=10, r=10, t=0, b=0),
        paper_bgcolor=bg,
        plot_bgcolor=bg,
    )
    fig.add_annotation(
        x=0.5, y=0.50,
        text=f"<span style='color:{txt}; font-size:40px; font-weight:800;'>{ok_pct:.2f}%</span>",
        showarrow=False
    )
    fig.add_annotation(x=0.06, y=0.06, text=f"<span style='color:{sub}; font-size:12px;'>0</span>", showarrow=False)
    fig.add_annotation(x=0.94, y=0.06, text=f"<span style='color:{sub}; font-size:12px;'>100</span>", showarrow=False)

    st.plotly_chart(fig, use_container_width=True)

    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1.3, 1.1, 1.1])
    with c1:
        st.markdown(f"<div style='color:{sub}; font-size:12px;'>Total</div><div style='color:{txt}; font-size:20px; font-weight:800;'>{total:,}</div>", unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div style='color:{sub}; font-size:12px;'>Conectadas</div><div style='color:{txt}; font-size:20px; font-weight:800;'>{ok_count:,}</div>", unsafe_allow_html=True)
    with c3:
        st.markdown(f"<div style='color:{sub}; font-size:12px;'>Desconectadas</div><div style='color:{txt}; font-size:20px; font-weight:800;'>{offline_count:,}</div>", unsafe_allow_html=True)
    with c4:
        st.markdown(f"<div style='color:{sub}; font-size:12px;'>% OK</div><div style='color:{txt}; font-size:20px; font-weight:800;'>{ok_pct:.2f}%</div>", unsafe_allow_html=True)
    with c5:
        st.markdown(f"<div style='color:{sub}; font-size:12px;'>% Offline</div><div style='color:{txt}; font-size:20px; font-weight:800;'>{offline_pct:.2f}%</div>", unsafe_allow_html=True)

    bar_bg = "#111827"
    offline_color = "#ef4444" if offline_pct >= 30 else ("#f59e0b" if offline_pct >= 15 else "#22c55e")
    st.markdown(
        f"""
        <div style="margin-top:10px; background:{bar_bg}; border:1px solid #1f2a44; border-radius:12px; padding:10px 12px;">
          <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
            <div style="color:{sub}; font-size:12px;">Dispositivos desconectado</div>
            <div style="color:{txt}; font-size:12px; font-weight:700;">{offline_pct:.2f}%</div>
          </div>
          <div style="height:12px; width:100%; background:{track}; border-radius:999px; overflow:hidden;">
            <div style="height:12px; width:{offline_pct}%; background:{offline_color}; border-radius:999px;"></div>
          </div>
          <div style="display:flex; justify-content:space-between; margin-top:6px;">
            <div style="color:{sub}; font-size:11px;">0%</div>
            <div style="color:{sub}; font-size:11px;">100%</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def hbar_counts(title: str, counts: pd.DataFrame):
    st.markdown(f"### {title}")

    d = counts.reset_index()
    first_col = d.columns[0]
    d = d.rename(columns={first_col: "Estado"})

    if "unidades" not in d.columns:
        value_col = d.columns[1]
        d = d.rename(columns={value_col: "unidades"})

    fig = go.Figure(go.Bar(
        x=d["unidades"],
        y=d["Estado"],
        orientation="h",
        text=d["unidades"],
        textposition="outside"
    ))
    fig.update_layout(
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(autorange="reversed"),
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig, use_container_width=True)

# =========================
# LOADS
# =========================
@st.cache_data(ttl=300)
def load_status_df() -> tuple[pd.DataFrame, Path]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if STATUS_FIXED.exists():
        path = STATUS_FIXED
    else:
        last = latest_csv_by_prefix(DATA_DIR, STATUS_PREFIX)
        if last is None:
            st.error(
                f"No encontré archivos {STATUS_PREFIX}*.csv en {DATA_DIR}.\n\n"
                f"Solución: ejecuta el job diario o descarga un CSV y cópialo a /data"
            )
            st.stop()
        path = last
        st.info(f"Usando último CSV diario: {path.name}")

    df = pd.read_csv(path)
    df = compute_connectivity(df)
    return df, path

@st.cache_data(ttl=300)
def load_master_df() -> pd.DataFrame:
    ensure_file_exists(MASTER_XLSX, "master_Flota.xlsx")

    xls = pd.ExcelFile(MASTER_XLSX)
    sheet = xls.sheet_names[0]
    m = xls.parse(sheet_name=sheet, dtype=str)
    m.columns = [c.strip() for c in m.columns]

    # ✅ Tu master real trae IMEI_master / IMEI_status, etc.
    candidates = [
        "IMEI", "imei", "Imei",
        "IMEI_master", "imei_master",
        "IMEI_status", "imei_status",
    ]
    col_imei = next((c for c in candidates if c in m.columns), None)

    if col_imei is None:
        st.error(
            "El master debe tener columna IMEI.\n\n"
            f"Busqué: {candidates}\n"
            f"Columnas encontradas: {list(m.columns)}"
        )
        st.stop()

    m = m.rename(columns={col_imei: "IMEI"})
    m["IMEI"] = norm_str_series(m["IMEI"])

    # Opcional: normaliza VIN / patente si existen (para búsquedas)
    if "VIN" not in m.columns:
        for c in ["VIN_master", "VIN_status", "vin", "vin_master", "vin_status"]:
            if c in m.columns:
                m = m.rename(columns={c: "VIN"})
                break
    if "license_plate" not in m.columns:
        for c in ["Patente", "patente", "license_plate_status", "license_plate_master"]:
            if c in m.columns:
                m = m.rename(columns={c: "license_plate"})
                break

    return m

# =========================
# MAIN
# =========================
df_status, used_path = load_status_df()
df_master = load_master_df()

# Filtrar por master
df_status["IMEI"] = norm_str_series(df_status["IMEI"])
allowed = set(df_master["IMEI"])
df_f = df_status[df_status["IMEI"].isin(allowed)].copy()

# Sidebar filtros mínimos
st.sidebar.title("Filtros")
q = st.sidebar.text_input("Buscar (IMEI/VIN/Patente)", value="").strip().upper()

if q:
    cols = [c for c in ["IMEI", "VIN", "license_plate"] if c in df_f.columns]
    mask = False
    for c in cols:
        mask = mask | df_f[c].fillna("").astype(str).str.upper().str.contains(q, regex=False)
    df_f = df_f[mask].copy()

# KPIs + Gauges
total = len(df_f)

tele_ok = int(((df_f["can_timestamp"].notna()) & (df_f["days_can"] <= 30)).sum()) if total else 0
tele_pct = safe_pct(tele_ok, total)

gps_ok = int(((df_f["gps_timestamp"].notna()) & (df_f["days_gps"] <= 15)).sum()) if total else 0
gps_pct = safe_pct(gps_ok, total)

tele_counts = df_f["estado_telemetria"].value_counts().reindex(ORDER4).fillna(0).astype(int).to_frame("unidades")
gps_counts = df_f["gps_status_any"].value_counts().reindex(ORDER4).fillna(0).astype(int).to_frame("unidades")

# UI
st.title("Dashboard de Conectividad")

st.caption(
    f"Status usado: {used_path.name} | "
    f"Unidades master: {len(df_master):,} | "
    f"Unidades filtradas: {len(df_f):,}"
)

g1, g2 = st.columns(2)
with g1:
    gauge_card_v2(
        "Conectividad Telemetría Copiloto",
        "Conexión últimos 30 días (CAN)",
        tele_pct, tele_ok, total,
        thresholds=(80, 60)
    )

with g2:
    gauge_card_v2(
        "Conectividad GPS Copiloto",
        "Conexión últimos 15 días (GPS global)",
        gps_pct, gps_ok, total,
        thresholds=(85, 70)
    )

b1, b2 = st.columns(2)
with b1:
    hbar_counts("Estado de conectividad Telemetría", tele_counts)
with b2:
    hbar_counts("Estado de conectividad GPS", gps_counts)

st.markdown("---")
st.subheader("Unidades con problemas (Telemetría o GPS)")

problem_mask = df_f["estado_telemetria"].isin(PROBLEM_LABELS) | df_f["gps_status_any"].isin(PROBLEM_LABELS)
df_probs = df_f[problem_mask].copy()

show_cols = [c for c in [
    "IMEI", "VIN", "license_plate", "device_model", "source",
    "days_can", "estado_telemetria",
    "days_gps", "gps_status_any",
    "last_update_utc"
] if c in df_probs.columns]

df_probs = df_probs.sort_values(["days_can", "days_gps"], ascending=False, na_position="last")
st.dataframe(df_probs[show_cols].head(300), use_container_width=True)
