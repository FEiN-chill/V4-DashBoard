import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import requests
import math
import os
import io
import numpy as np
import uuid
from streamlit_gsheets import GSheetsConnection

# ============================================================
# 1. 基本設定
# ============================================================
JST = timezone(timedelta(hours=+9), "JST")
st.set_page_config(
    page_title="Uniswap V4 LP Dashboard",
    layout="wide", page_icon="⚡",
    initial_sidebar_state="collapsed",
)

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background: #080c12; }
    .block-container { padding: 1.5rem 2rem 2rem 2rem !important; max-width: 1600px; }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0d1117; }
    ::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
    .stButton > button {
        border-radius: 6px !important; font-weight: 600 !important;
        font-size: 0.82rem !important; transition: all 0.2s ease !important;
    }
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #00E676, #00b8d4) !important;
        color: #000 !important; border: none !important;
    }
    .stButton > button[kind="primary"]:hover { opacity: 0.88 !important; transform: translateY(-1px) !important; }
    .stNumberInput input, .stTextInput input {
        background: rgba(255,255,255,0.04) !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 6px !important; color: #e6edf3 !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(255,255,255,0.03) !important; border-radius: 8px !important;
        padding: 4px !important; gap: 2px !important;
        border: 1px solid rgba(255,255,255,0.07) !important;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px !important; color: #8b949e !important;
        font-size: 0.8rem !important; font-weight: 500 !important; padding: 6px 14px !important;
    }
    .stTabs [aria-selected="true"] { background: rgba(0,230,118,0.12) !important; color: #00E676 !important; }
    h1 { font-family: 'Space Mono', monospace !important; }
    h2, h3 { font-family: 'Inter', sans-serif !important; font-weight: 600 !important; }
    .alert-banner {
        padding: 10px 16px; border-radius: 8px; margin-bottom: 12px;
        font-size: 0.82rem; font-weight: 600; display: flex; align-items: center; gap: 8px;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================
# 2. 定数
# ============================================================
API_KEY     = "7a401bdd8853e9f92d3cdd8819a77a73"
SUBGRAPH_ID = "2CB2uQxcDKWDenagn2z17KQVCtfwSx5eXYuvqTciRTJu"
POOL_ID     = "0x3befb3625d0a36cb4bb84ff1c549dc5092fe8e6f527661c9e4825762b5cea727"

NEON_GREEN  = "#00E676"
NEON_BLUE   = "#00B0FF"
NEON_RED    = "#ff4b4b"
NEON_PURPLE = "#D500F9"
NEON_AMBER  = "#FFD600"
NEON_ORANGE = "#FF6D00"
NEON_CYAN   = "#00E5FF"
NEON_PINK   = "#FF4081"

# ポジションごとのカラーパレット（チャート用）
POS_COLORS = [NEON_GREEN, NEON_BLUE, NEON_PURPLE, NEON_CYAN, NEON_PINK, NEON_ORANGE]

conn = st.connection("gsheets", type=GSheetsConnection)

# ============================================================
# 3. データ管理（マルチポジション対応）
# ============================================================

# --- positions ワークシート ---
# 列: pos_id, name, INITIAL_USDC, INITIAL_JPYC, TICK_LOWER, TICK_UPPER,
#     CARRYOVER_PROFIT, CARRYOVER_FEES, PHASE_START_DATE, FEE_TARGET_MONTHLY, active

POSITIONS_COLS = [
    "pos_id", "name", "INITIAL_USDC", "INITIAL_JPYC",
    "TICK_LOWER", "TICK_UPPER", "CARRYOVER_PROFIT", "CARRYOVER_FEES",
    "PHASE_START_DATE", "FEE_TARGET_MONTHLY", "active",
]
POSITIONS_NUMERIC = [
    "INITIAL_USDC", "INITIAL_JPYC", "TICK_LOWER", "TICK_UPPER",
    "CARRYOVER_PROFIT", "CARRYOVER_FEES", "FEE_TARGET_MONTHLY",
]
HISTORY_COLS = [
    "pos_id", "date", "rate", "usdc", "jpyc", "fees",
    "il", "hold_val_usd", "lp_val_usd", "net_profit_usd",
]


def _make_default_position(name: str = "Position 1") -> dict:
    return {
        "pos_id": str(uuid.uuid4())[:8],
        "name": name,
        "INITIAL_USDC": 478.14, "INITIAL_JPYC": 86135.12,
        "TICK_LOWER": 326810, "TICK_UPPER": 327250,
        "CARRYOVER_PROFIT": 0.0, "CARRYOVER_FEES": 0.0,
        "PHASE_START_DATE": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "FEE_TARGET_MONTHLY": 30.0,
        "active": True,
    }


def load_positions() -> pd.DataFrame:
    """positions ワークシートから全ポジション設定を読み込む"""
    try:
        df = conn.read(worksheet="positions", ttl=600)
        if df is None or df.empty:
            return pd.DataFrame(columns=POSITIONS_COLS)
        if "pos_id" not in df.columns:
            return pd.DataFrame(columns=POSITIONS_COLS)
        df = df.dropna(subset=["pos_id"])
        if df.empty:
            return pd.DataFrame(columns=POSITIONS_COLS)
        for c in POSITIONS_NUMERIC:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        if "active" in df.columns:
            df["active"] = df["active"].astype(str).str.lower().isin(["true", "1", "yes"])
        else:
            df["active"] = True
        return df
    except Exception:
        return pd.DataFrame(columns=POSITIONS_COLS)


def save_positions(df: pd.DataFrame):
    try:
        conn.update(worksheet="positions", data=df)
    except Exception:
        # ワークシートが存在しない場合は新規作成
        try:
            conn.create(worksheet="positions", data=df)
        except Exception as e:
            st.error(f"positions ワークシートの書き込みに失敗しました: {e}")
            return
    st.cache_data.clear()


def add_position(pos_dict: dict):
    """新しいポジションを追加"""
    existing = load_positions()
    new_row = pd.DataFrame([pos_dict])
    combined = pd.concat([existing, new_row], ignore_index=True)
    save_positions(combined)


def update_position(pos_id: str, updates: dict):
    """既存ポジションの設定を更新"""
    df = load_positions()
    idx = df.index[df["pos_id"] == pos_id]
    if len(idx) == 0:
        return
    for k, v in updates.items():
        df.at[idx[0], k] = v
    save_positions(df)


def delete_position(pos_id: str):
    """ポジションを削除（設定 + 履歴両方）"""
    df = load_positions()
    df = df[df["pos_id"] != pos_id]
    save_positions(df)
    # 履歴も削除
    hist = load_history_all()
    hist = hist[hist["pos_id"] != pos_id]
    _write_history(hist)


def get_position(pos_id: str) -> dict | None:
    """指定IDのポジション設定をdictで返す"""
    df = load_positions()
    row = df[df["pos_id"] == pos_id]
    if row.empty:
        return None
    return row.iloc[0].to_dict()


# --- history ワークシート ---

def load_history_all() -> pd.DataFrame:
    """全ポジションの履歴を読み込む"""
    try:
        df = conn.read(worksheet="history", ttl=600)
        df = df.dropna(how="all")
        if not df.empty and "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
        # pos_id が無い行は旧データ → "legacy" として扱う
        if "pos_id" not in df.columns:
            df["pos_id"] = "legacy"
        else:
            df["pos_id"] = df["pos_id"].fillna("legacy")
        return df
    except Exception:
        return pd.DataFrame(columns=HISTORY_COLS)


def load_history(pos_id: str) -> pd.DataFrame:
    """特定ポジションの履歴のみ返す"""
    df = load_history_all()
    return df[df["pos_id"] == pos_id].reset_index(drop=True)


def _write_history(df: pd.DataFrame):
    """履歴ワークシートを丸ごと上書き"""
    try:
        conn.update(worksheet="history", data=df)
    except Exception:
        try:
            conn.create(worksheet="history", data=df)
        except Exception as e:
            st.error(f"history ワークシートの書き込みに失敗しました: {e}")
            return
    st.cache_data.clear()


def save_history(new_df: pd.DataFrame):
    """新しい行を履歴に追加"""
    existing = load_history_all()
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    # 同一ポジション内で同一日時は上書き
    combined = combined.drop_duplicates(subset=["pos_id", "date"], keep="last")
    combined = combined.sort_values(["pos_id", "date"]).reset_index(drop=True)
    _write_history(combined)


def reset_history(pos_id: str):
    """指定ポジションの履歴のみクリア"""
    df = load_history_all()
    df = df[df["pos_id"] != pos_id]
    _write_history(df)


def import_history_csv(uploaded_file, pos_id: str) -> tuple[bool, str]:
    try:
        df_new = pd.read_csv(uploaded_file)
        df_new["date"] = pd.to_datetime(df_new["date"])
        required = {"date", "rate", "fees"}
        if not required.issubset(df_new.columns):
            return False, f"必須列が不足: {required - set(df_new.columns)}"
        df_new["pos_id"] = pos_id
        save_history(df_new)
        return True, f"{len(df_new)} 件をインポートしました"
    except Exception as e:
        return False, str(e)


# ============================================================
# 旧データ移行ヘルパー（初回のみ実行）
# ============================================================
def migrate_legacy_data():
    """
    旧 settings ワークシート → positions に変換
    旧 history の pos_id なし行 → legacy ID を付与
    """
    positions = load_positions()
    if not positions.empty:
        return  # 既に移行済み

    # 旧 settings を読む
    old_s = {}
    try:
        df_old = conn.read(worksheet="settings", ttl=5)
        if df_old is not None and not df_old.empty and "key" in df_old.columns:
            df_old = df_old.dropna(subset=["key"])
            old_s = dict(zip(df_old["key"].astype(str), df_old["value"].astype(str)))
    except Exception:
        pass

    if old_s:
        pos = _make_default_position("Position 1 (migrated)")
        pos["pos_id"] = "legacy"
        for k in POSITIONS_NUMERIC:
            if k in old_s:
                try:
                    pos[k] = float(old_s[k])
                except Exception:
                    pass
        if "PHASE_START_DATE" in old_s:
            pos["PHASE_START_DATE"] = old_s["PHASE_START_DATE"]
        if "FEE_TARGET_MONTHLY" in old_s:
            try:
                pos["FEE_TARGET_MONTHLY"] = float(old_s["FEE_TARGET_MONTHLY"])
            except Exception:
                pass
        try:
            add_position(pos)
        except Exception:
            pass

        # history に pos_id を付与
        try:
            hist = load_history_all()
            if not hist.empty:
                hist["pos_id"] = hist["pos_id"].fillna("legacy").replace("", "legacy")
                _write_history(hist)
        except Exception:
            pass


def run_manual_migration():
    """デバッグ用：ステップごとに結果を表示する手動移行"""
    results = []

    # Step 1: settings 読み込み
    results.append("**Step 1:** settings ワークシートを読み込み中...")
    old_s = {}
    try:
        df_old = conn.read(worksheet="settings", ttl=5)
        if df_old is None or df_old.empty:
            results.append(f"⚠️ settings が空です (None={df_old is None})")
        elif "key" not in df_old.columns:
            results.append(f"⚠️ 'key' 列がありません。列: {list(df_old.columns)}")
        else:
            df_old = df_old.dropna(subset=["key"])
            old_s = dict(zip(df_old["key"].astype(str), df_old["value"].astype(str)))
            results.append(f"✅ {len(old_s)} 件の設定を読み込みました: {list(old_s.keys())}")
    except Exception as e:
        results.append(f"❌ settings 読み込み失敗: {e}")

    if not old_s:
        results.append("⛔ 設定データが無いため移行できません")
        return results

    # Step 2: ポジション作成
    results.append("**Step 2:** ポジションデータを作成中...")
    pos = _make_default_position("Position 1 (migrated)")
    pos["pos_id"] = "legacy"
    for k in POSITIONS_NUMERIC:
        if k in old_s:
            try:
                pos[k] = float(old_s[k])
            except Exception:
                pass
    if "PHASE_START_DATE" in old_s:
        pos["PHASE_START_DATE"] = old_s["PHASE_START_DATE"]
    results.append(f"✅ ポジション作成: Tick {int(pos['TICK_LOWER'])}–{int(pos['TICK_UPPER'])}, USDC={pos['INITIAL_USDC']}, JPYC={pos['INITIAL_JPYC']}")

    # Step 3: positions に書き込み
    results.append("**Step 3:** positions ワークシートに書き込み中...")
    df_pos = pd.DataFrame([pos])
    try:
        conn.update(worksheet="positions", data=df_pos)
        results.append("✅ conn.update() 成功")
    except Exception as e1:
        results.append(f"⚠️ conn.update() 失敗: {e1}")
        try:
            conn.create(worksheet="positions", data=df_pos)
            results.append("✅ conn.create() で代替成功")
        except Exception as e2:
            results.append(f"❌ conn.create() も失敗: {e2}")
            return results

    # Step 4: history に pos_id 付与
    results.append("**Step 4:** history に pos_id 列を追加中...")
    try:
        hist = conn.read(worksheet="history", ttl=5)
        if hist is not None and not hist.empty:
            if "pos_id" not in hist.columns:
                hist.insert(0, "pos_id", "legacy")
                results.append(f"✅ pos_id 列を追加 ({len(hist)} 行)")
            else:
                hist["pos_id"] = hist["pos_id"].fillna("legacy")
                results.append(f"✅ 既存 pos_id を補完 ({len(hist)} 行)")
            try:
                conn.update(worksheet="history", data=hist)
                results.append("✅ history 書き込み成功")
            except Exception as e:
                results.append(f"❌ history 書き込み失敗: {e}")
        else:
            results.append("⚠️ history が空です")
    except Exception as e:
        results.append(f"❌ history 読み込み失敗: {e}")

    st.cache_data.clear()
    results.append("**🎉 移行完了！ページをリロードしてください。**")
    return results


# ============================================================
# 4. オンチェーン & 数学エンジン
# ============================================================
@st.cache_data(ttl=30)
def fetch_pool_data() -> float | None:
    url = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}"
    query = f'{{ pool(id: "{POOL_ID}") {{ token1Price }} }}'
    try:
        res = requests.post(url, json={"query": query}, timeout=10).json()
        return float(res["data"]["pool"]["token1Price"])
    except Exception:
        return None


def tick_to_price(tick: int) -> float:
    return math.pow(1.0001, tick) / (10**12)


def price_to_tick(price: float) -> int:
    if price <= 0:
        return 0
    raw = math.log(price * 1e12) / math.log(1.0001)
    return int(round(raw / 10.0) * 10)


def calculate_exact_holdings(curr_rate, tick_lower, tick_upper, init_usdc, init_jpyc):
    d0, d1 = 10**6, 10**18
    X, Y = init_usdc * d0, init_jpyc * d1
    sqrtP_L = math.pow(1.0001, tick_lower / 2)
    sqrtP_U = math.pow(1.0001, tick_upper / 2)
    sqrtP_curr = math.sqrt(max(curr_rate, 1e-12) * (d1 / d0))
    A = X * sqrtP_U
    B = Y - (X * sqrtP_U * sqrtP_L)
    C = -Y * sqrtP_U
    if A == 0:
        return 0.0, 0.0, tick_to_price(tick_lower), tick_to_price(tick_upper)
    disc = max(0, B**2 - 4 * A * C)
    v = (-B + math.sqrt(disc)) / (2 * A)
    L = Y / (v - sqrtP_L)
    if sqrtP_curr <= sqrtP_L:
        amt0 = L * (sqrtP_U - sqrtP_L) / (sqrtP_L * sqrtP_U); amt1 = 0.0
    elif sqrtP_curr >= sqrtP_U:
        amt0 = 0.0; amt1 = L * (sqrtP_U - sqrtP_L)
    else:
        amt0 = L * (sqrtP_U - sqrtP_curr) / (sqrtP_curr * sqrtP_U)
        amt1 = L * (sqrtP_curr - sqrtP_L)
    return amt0 / d0, amt1 / d1, tick_to_price(tick_lower), tick_to_price(tick_upper)


def compute_fee_stats(df_pos_hist: pd.DataFrame, phase_start_date) -> float:
    if df_pos_hist.empty:
        return 0.0
    actual = phase_start_date
    if df_pos_hist.iloc[0]["date"] < phase_start_date:
        actual = df_pos_hist.iloc[0]["date"]
    df_phase = df_pos_hist[df_pos_hist["date"] >= actual]
    if len(df_phase) < 2:
        return 0.0
    p_days = (df_phase.iloc[-1]["date"] - df_phase.iloc[0]["date"]).total_seconds() / 86400
    if p_days < 0.01:
        return 0.0
    return (df_phase.iloc[-1]["fees"] - df_phase.iloc[0]["fees"]) / p_days


def compute_position_snapshot(pos: dict, rate: float, df_hist: pd.DataFrame) -> dict:
    """ポジション1件の全指標をまとめて返す"""
    calc_usdc, calc_jpyc, p_low, p_up = calculate_exact_holdings(
        rate, pos["TICK_LOWER"], pos["TICK_UPPER"],
        pos["INITIAL_USDC"], pos["INITIAL_JPYC"],
    )
    pos_val = calc_usdc + calc_jpyc / rate if rate > 0 else 0
    hold_val = pos["INITIAL_USDC"] + pos["INITIAL_JPYC"] / rate if rate > 0 else 0
    il = pos_val - hold_val
    cumulative_fees = float(df_hist.iloc[-1]["fees"]) if not df_hist.empty else 0.0
    net_profit = il + cumulative_fees + pos.get("CARRYOVER_PROFIT", 0)

    range_pct = ((rate - p_low) / (p_up - p_low) * 100) if p_up > p_low else 0
    range_pct = max(0, min(100, range_pct))

    usdc_ratio = (calc_usdc / pos_val * 100) if pos_val > 0 else 50
    phase_start = pd.to_datetime(pos.get("PHASE_START_DATE", datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")))
    fee_avg_24h = compute_fee_stats(df_hist, phase_start)
    base_cap = pos["INITIAL_USDC"] + pos["INITIAL_JPYC"] / 160
    apr = (fee_avg_24h * 365 / base_cap * 100) if base_cap > 0 else 0

    return {
        "calc_usdc": calc_usdc, "calc_jpyc": calc_jpyc,
        "p_low": p_low, "p_up": p_up,
        "pos_val": pos_val, "hold_val": hold_val,
        "il": il, "cumulative_fees": cumulative_fees,
        "net_profit": net_profit, "range_pct": range_pct,
        "usdc_ratio": usdc_ratio, "jpyc_ratio": 100 - usdc_ratio,
        "fee_avg_24h": fee_avg_24h, "apr": apr,
        "base_capital": base_cap,
    }


# ============================================================
# 5. UI コンポーネント
# ============================================================
def metric_card(title, value, sub="", accent=NEON_GREEN, delta_positive=None):
    ind = ""
    if delta_positive is True:   ind = f"<span style='color:{NEON_GREEN};'>▲</span> "
    elif delta_positive is False: ind = f"<span style='color:{NEON_RED};'>▼</span> "
    return f"""
    <div style="background:linear-gradient(145deg,#111720,#0d1117);border:1px solid rgba(255,255,255,0.08);
        border-top:2px solid {accent};border-radius:10px;padding:18px 16px 14px;min-height:110px;
        display:flex;flex-direction:column;justify-content:space-between;">
        <div style="color:#8b949e;font-size:0.72rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;">{title}</div>
        <div style="color:#f0f6fc;font-size:1.55rem;font-weight:700;font-family:'Space Mono',monospace;line-height:1.1;">{value}</div>
        <div style="color:{accent};font-size:0.78rem;margin-top:6px;">{ind}{sub}</div>
    </div>"""


def metric_card_compact(title, value, sub="", accent=NEON_GREEN):
    """ポートフォリオ一覧用の小型カード"""
    return f"""
    <div style="background:linear-gradient(145deg,#111720,#0d1117);border:1px solid rgba(255,255,255,0.08);
        border-left:3px solid {accent};border-radius:8px;padding:12px 14px;min-height:70px;">
        <div style="color:#8b949e;font-size:0.68rem;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;">{title}</div>
        <div style="color:#f0f6fc;font-size:1.2rem;font-weight:700;font-family:'Space Mono',monospace;">{value}</div>
        <div style="color:{accent};font-size:0.7rem;margin-top:2px;">{sub}</div>
    </div>"""


def section_header(icon, title):
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:8px;margin:1.8rem 0 0.8rem;
        border-bottom:1px solid rgba(255,255,255,0.07);padding-bottom:8px;">
        <span style="font-size:1rem;">{icon}</span>
        <span style="font-size:0.85rem;font-weight:700;letter-spacing:0.1em;color:#8b949e;text-transform:uppercase;">{title}</span>
    </div>""", unsafe_allow_html=True)


def panel_label(text, color="#8b949e"):
    st.markdown(f"<div style='color:{color};font-size:0.7rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;margin-bottom:10px;'>{text}</div>", unsafe_allow_html=True)


def range_meter(range_pct, tick_lower, tick_upper, p_low, p_up, current_rate, is_sim=False, compact=False):
    in_range = 5 <= range_pct <= 95
    warn_zone = range_pct < 10 or range_pct > 90
    dot_color = NEON_AMBER if is_sim else (NEON_RED if not in_range else (NEON_ORANGE if warn_zone else NEON_GREEN))
    if is_sim: status = "SIMULATION"
    elif not in_range: status = "⚠ OUT"
    elif warn_zone: status = "⚡ NEAR"
    else: status = "IN ✓"

    pad = "10px 12px" if compact else "14px 16px"
    st.markdown(f"""
    <div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:{pad};margin-bottom:8px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
            <span style="color:#8b949e;font-size:0.68rem;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;">📍 Range</span>
            <span style="color:{dot_color};font-size:0.72rem;font-weight:700;">{status}</span>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:0.65rem;color:#555f6e;margin-bottom:4px;font-family:'Space Mono',monospace;">
            <span>{p_low:.4f} (T{tick_lower})</span><span style="text-align:right;">{p_up:.4f} (T{tick_upper})</span>
        </div>
        <div style="position:relative;width:100%;height:8px;border-radius:4px;
            background:linear-gradient(to right,{NEON_RED} 0%,{NEON_RED} 5%,{NEON_ORANGE} 10%,{NEON_GREEN} 20%,{NEON_GREEN} 80%,{NEON_ORANGE} 90%,{NEON_RED} 95%,{NEON_RED} 100%);overflow:visible;">
            <div style="width:12px;height:12px;background:{dot_color};border-radius:50%;box-shadow:0 0 10px {dot_color};
                position:absolute;top:50%;left:calc({range_pct:.1f}% - 6px);transform:translateY(-50%);z-index:2;"></div>
        </div>
        <div style="display:flex;justify-content:space-between;margin-top:6px;font-size:0.65rem;color:#555f6e;font-family:'Space Mono',monospace;">
            <span>↓{abs(current_rate - p_low):.4f}</span>
            <span style="color:{dot_color};font-weight:700;">{range_pct:.1f}%</span>
            <span>↑{abs(p_up - current_rate):.4f}</span>
        </div>
    </div>""", unsafe_allow_html=True)


def plotly_dark_layout(**kwargs):
    base = dict(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified",
        font=dict(family="Inter, sans-serif", size=11, color="#8b949e"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", showgrid=True, zeroline=False),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", showgrid=True, zeroline=False),
    )
    base.update(kwargs)
    return base


# ============================================================
# 6. メインアプリ
# ============================================================

# --- Session State 初期化 ---
if "in_rate" not in st.session_state:
    st.session_state.in_rate = 160.0
if "confirm_reset" not in st.session_state:
    st.session_state.confirm_reset = False
if "confirm_delete" not in st.session_state:
    st.session_state.confirm_delete = False

# --- 旧データ移行 ---
migrate_legacy_data()

# --- ポジション一覧読み込み ---
all_positions = load_positions()

# ---- ヘッダー ----
st.markdown("""
    <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:0.5rem;">
        <h1 style="margin:0;font-size:1.6rem;color:#f0f6fc;font-family:'Space Mono',monospace;">⚡ Uniswap V4</h1>
        <span style="color:#00E676;font-size:0.7rem;font-weight:700;letter-spacing:0.15em;
            background:rgba(0,230,118,0.1);border:1px solid rgba(0,230,118,0.3);
            padding:2px 8px;border-radius:20px;font-family:'Space Mono',monospace;">MULTI-LP DASHBOARD</span>
    </div>
    <div style="color:#555f6e;font-size:0.78rem;margin-bottom:1rem;font-family:'Space Mono',monospace;">
        JPYC / USDC · MULTI-POSITION ANALYTICS & STRATEGY
    </div>
""", unsafe_allow_html=True)

col_main, col_right = st.columns([3, 1], gap="large")

# ============================================================
# 右サイドパネル
# ============================================================
with col_right:

    # ---- Auto Fetch ----
    panel_label("⚡ Auto Fetch")
    if st.button("🔄 The Graph から最新取得", use_container_width=True):
        with st.spinner("オンチェーンデータを取得中..."):
            fetched = fetch_pool_data()
        if fetched:
            st.session_state.in_rate = fetched
            st.success(f"✅ {fetched:.4f}")
        else:
            st.error("❌ 取得失敗")
    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    # ---- ポジション選択 ----
    panel_label("📂 Position Selector")

    if all_positions.empty:
        st.info("ポジションがありません。下の「新規追加」から作成してください。")
        selected_pos_id = None
    else:
        active_positions = all_positions[all_positions["active"] == True]
        if active_positions.empty:
            active_positions = all_positions  # fallback

        pos_options = {
            f"{row['name']}  (T{int(row['TICK_LOWER'])}–{int(row['TICK_UPPER'])})": row["pos_id"]
            for _, row in active_positions.iterrows()
        }
        selected_label = st.selectbox(
            "分析対象", options=list(pos_options.keys()),
            label_visibility="collapsed",
        )
        selected_pos_id = pos_options[selected_label]

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    # ---- What-If Simulator ----
    panel_label("🔮 What-If Simulator", color=NEON_AMBER)
    with st.container(border=True):
        sim_mode = st.toggle("シミュレーションモード")
        if sim_mode:
            sim_rate = st.slider("仮定のレート", min_value=130.0, max_value=190.0,
                                 value=float(st.session_state.in_rate), step=0.5)
            st.caption("⚠️ メイン画面がSIM結果になります")
        else:
            sim_rate = st.session_state.in_rate
    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    # ---- Record Data (選択中のポジションに対して) ----
    if selected_pos_id:
        pos_settings = get_position(selected_pos_id)
        if pos_settings:
            panel_label(f"📝 Record: {pos_settings['name']}")
            with st.container(border=True):
                live_rate = st.number_input(
                    "現在レート (JPYC/USDC)", value=float(st.session_state.in_rate), format="%.4f",
                )
                st.session_state.in_rate = live_rate

                now_usdc, now_jpyc, _, _ = calculate_exact_holdings(
                    live_rate, pos_settings["TICK_LOWER"], pos_settings["TICK_UPPER"],
                    pos_settings["INITIAL_USDC"], pos_settings["INITIAL_JPYC"],
                )
                st.markdown(f"""
                <div style="background:rgba(0,230,118,0.06);border:1px solid rgba(0,230,118,0.15);
                    border-radius:8px;padding:10px 12px;margin:4px 0 8px;font-family:'Space Mono',monospace;font-size:0.78rem;">
                    <div style="color:#8b949e;font-size:0.68rem;margin-bottom:4px;">算出枚数</div>
                    <div style="color:#f0f6fc;">{now_usdc:.2f} <span style="color:#8b949e;">USDC</span></div>
                    <div style="color:#f0f6fc;">{now_jpyc:,.0f} <span style="color:#8b949e;">JPYC</span></div>
                </div>""", unsafe_allow_html=True)

                df_pos_hist = load_history(selected_pos_id)
                last_fees = float(df_pos_hist.iloc[-1]["fees"]) if not df_pos_hist.empty else 0.0
                earned_fees = st.number_input("累積回収済手数料 ($)", value=last_fees, step=0.01, format="%.4f")

                if not sim_mode:
                    if st.button("⚡ データを記録", type="primary", use_container_width=True):
                        save_hold = pos_settings["INITIAL_USDC"] + pos_settings["INITIAL_JPYC"] / live_rate
                        save_lp = now_usdc + now_jpyc / live_rate
                        il = save_lp - save_hold
                        net_p = il + earned_fees + pos_settings.get("CARRYOVER_PROFIT", 0)
                        new_row = pd.DataFrame([{
                            "pos_id": selected_pos_id,
                            "date": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
                            "rate": live_rate, "usdc": now_usdc, "jpyc": now_jpyc,
                            "fees": earned_fees, "il": il,
                            "hold_val_usd": save_hold, "lp_val_usd": save_lp,
                            "net_profit_usd": net_p,
                        }])
                        with st.spinner("記録中..."):
                            save_history(new_row)
                        st.toast("✅ 記録しました！", icon="☁️")
                        st.rerun()
                else:
                    st.button("⚡ データを記録", disabled=True, use_container_width=True)

            st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

            # ---- Position Manage ----
            panel_label("⚙️ Position Manage")
            tab_rebuild, tab_setting = st.tabs(["🔄 再構築", "📋 設定変更"])

            with tab_rebuild:
                st.caption("利益確定 → 新Tick")
                r_up   = st.number_input("新 Max Tick", value=int(pos_settings["TICK_UPPER"]), step=10, key="r_up")
                r_low  = st.number_input("新 Min Tick", value=int(pos_settings["TICK_LOWER"]), step=10, key="r_low")
                r_usdc = st.number_input("新 USDC", value=float(now_usdc), format="%.2f", key="r_u")
                r_jpyc = st.number_input("新 JPYC", value=float(now_jpyc), format="%.2f", key="r_j")
                r_profit = st.number_input("確定利益 ($)", value=0.0, format="%.2f", key="r_p")
                if st.button("🔄 再構築を反映", use_container_width=True):
                    new_carry = float(pos_settings.get("CARRYOVER_PROFIT", 0)) + r_profit
                    update_position(selected_pos_id, {
                        "INITIAL_USDC": r_usdc, "INITIAL_JPYC": r_jpyc,
                        "TICK_UPPER": r_up, "TICK_LOWER": r_low,
                        "CARRYOVER_PROFIT": new_carry,
                        "PHASE_START_DATE": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    st.toast("✅ 再構築完了", icon="🔄")
                    st.rerun()

            with tab_setting:
                new_name = st.text_input("ポジション名", value=pos_settings["name"], key="edit_name")
                new_target = st.number_input("月次手数料目標 ($)", value=float(pos_settings.get("FEE_TARGET_MONTHLY", 30)), format="%.2f", key="edit_target")
                if st.button("💾 設定を保存", use_container_width=True):
                    update_position(selected_pos_id, {"name": new_name, "FEE_TARGET_MONTHLY": new_target})
                    st.toast("✅ 保存しました")
                    st.rerun()

                st.markdown("---")
                # ポジション削除
                if not st.session_state.confirm_delete:
                    if st.button("🗑️ このポジションを削除", use_container_width=True):
                        st.session_state.confirm_delete = True
                        st.rerun()
                else:
                    st.error(f"⚠️ 「{pos_settings['name']}」の設定と履歴を完全に削除します")
                    cd1, cd2 = st.columns(2)
                    with cd1:
                        if st.button("✅ 削除する", use_container_width=True):
                            delete_position(selected_pos_id)
                            st.session_state.confirm_delete = False
                            st.rerun()
                    with cd2:
                        if st.button("❌ キャンセル", use_container_width=True):
                            st.session_state.confirm_delete = False
                            st.rerun()

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    # ---- 新規ポジション追加 ----
    panel_label("➕ Add New Position")
    with st.expander("新しいポジションを作成"):
        new_pos_name = st.text_input("ポジション名", value=f"Position {len(all_positions)+1}", key="new_name")
        new_n_up   = st.number_input("Max Tick", value=327250, step=10, key="new_up")
        new_n_low  = st.number_input("Min Tick", value=326810, step=10, key="new_low")
        new_n_usdc = st.number_input("初期 USDC", value=0.0, format="%.2f", key="new_usdc")
        new_n_jpyc = st.number_input("初期 JPYC", value=0.0, format="%.2f", key="new_jpyc")
        new_n_tgt  = st.number_input("月次手数料目標 ($)", value=30.0, format="%.2f", key="new_tgt")
        invalid = new_n_low >= new_n_up
        if invalid:
            st.error("Min Tick < Max Tick")
        if st.button("🚀 ポジションを作成", use_container_width=True, disabled=invalid):
            new_pos = _make_default_position(new_pos_name)
            new_pos.update({
                "INITIAL_USDC": new_n_usdc, "INITIAL_JPYC": new_n_jpyc,
                "TICK_UPPER": new_n_up, "TICK_LOWER": new_n_low,
                "FEE_TARGET_MONTHLY": new_n_tgt,
            })
            add_position(new_pos)
            st.toast(f"✅ {new_pos_name} を作成しました", icon="🆕")
            st.rerun()

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

    # ---- CSVインポート/エクスポート ----
    panel_label("💾 Data Management")
    with st.expander("CSV Import / Export"):
        if selected_pos_id:
            df_export = load_history(selected_pos_id)
            if not df_export.empty:
                csv_buf = io.StringIO()
                df_export.to_csv(csv_buf, index=False)
                st.download_button(
                    "⬇️ 選択中ポジションの履歴",
                    data=csv_buf.getvalue(),
                    file_name=f"lp_{selected_pos_id}_{datetime.now(JST).strftime('%Y%m%d')}.csv",
                    mime="text/csv", use_container_width=True,
                )
            uploaded = st.file_uploader("CSVインポート", type=["csv"], label_visibility="collapsed")
            if uploaded is not None:
                ok, msg = import_history_csv(uploaded, selected_pos_id)
                if ok:
                    st.success(msg); st.rerun()
                else:
                    st.error(f"失敗: {msg}")


# ============================================================
# メインパネル
# ============================================================
with col_main:

    if all_positions.empty:
        st.markdown("""
        <div style="text-align:center;padding:60px 20px;border:1px dashed rgba(255,255,255,0.1);
            border-radius:16px;background:rgba(255,255,255,0.02);">
            <div style="font-size:3rem;margin-bottom:16px;">📊</div>
            <div style="color:#8b949e;font-size:1rem;font-weight:500;">ポジションがありません</div>
            <div style="color:#555f6e;font-size:0.82rem;margin-top:8px;">
                右パネルの「Add New Position」から新規作成するか、下の移行ボタンで旧データを取り込んでください
            </div>
        </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

        with st.expander("🔧 旧データ移行ツール（デバッグ付き）", expanded=True):
            st.caption("settings → positions への移行をステップごとに実行し、各ステップの成功/失敗を表示します。")
            if st.button("▶️ 手動で移行を実行", type="primary", use_container_width=True):
                results = run_manual_migration()
                for r in results:
                    st.markdown(r)
                st.markdown("---")
                st.caption("上記の結果を確認し、問題があればスクリーンショットを共有してください。成功した場合はページをリロードしてください。")
                if st.button("🔄 リロード"):
                    st.rerun()

        st.stop()

    # ==========================================================
    # ポートフォリオ全体サマリー
    # ==========================================================
    active_rate_global = sim_rate if sim_mode else float(st.session_state.in_rate)
    accent_global = NEON_AMBER if sim_mode else NEON_GREEN

    section_header("🏦", "Portfolio Overview" + (" [SIMULATION]" if sim_mode else ""))

    total_val = 0.0; total_il = 0.0; total_fees = 0.0; total_profit = 0.0
    portfolio_data = []  # (pos_id, name, snapshot, df_hist, color)

    for i, (_, pos_row) in enumerate(all_positions.iterrows()):
        pid = pos_row["pos_id"]
        df_h = load_history(pid)
        snap = compute_position_snapshot(pos_row.to_dict(), active_rate_global, df_h)
        color = POS_COLORS[i % len(POS_COLORS)]
        portfolio_data.append((pid, pos_row["name"], snap, df_h, color))
        total_val    += snap["pos_val"]
        total_il     += snap["il"]
        total_fees   += snap["cumulative_fees"]
        total_profit += snap["net_profit"]

    # ポートフォリオカード
    pc1, pc2, pc3, pc4 = st.columns(4)
    with pc1:
        st.markdown(metric_card("Total Position Value", f"${total_val:,.2f}",
                                f"{len(all_positions)} ポジション合計", accent=accent_global), unsafe_allow_html=True)
    with pc2:
        st.markdown(metric_card("Total Net Profit", f"${total_profit:+,.2f}", "IL+手数料+繰越",
                                accent=accent_global, delta_positive=total_profit >= 0), unsafe_allow_html=True)
    with pc3:
        st.markdown(metric_card("Total Fees", f"${total_fees:,.4f}", "全ポジション累計"), unsafe_allow_html=True)
    with pc4:
        st.markdown(metric_card("Total IL", f"${total_il:+,.4f}", "全ポジション合計",
                                accent=NEON_RED if total_il < 0 else accent_global,
                                delta_positive=total_il >= 0), unsafe_allow_html=True)

    # ---- ポジション一覧カード ----
    n_pos = len(portfolio_data)
    if n_pos > 1:
        cols_per_row = min(n_pos, 3)
        card_cols = st.columns(cols_per_row)
        for i, (pid, pname, snap, df_h, color) in enumerate(portfolio_data):
            with card_cols[i % cols_per_row]:
                in_r = 5 <= snap["range_pct"] <= 95
                range_icon = "🟢" if in_r else "🔴"
                st.markdown(f"""
                <div style="background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.08);
                    border-left:3px solid {color};border-radius:8px;padding:12px 14px;margin-bottom:8px;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="color:#f0f6fc;font-weight:700;font-size:0.85rem;">{pname}</span>
                        <span style="font-size:0.7rem;">{range_icon} {snap['range_pct']:.0f}%</span>
                    </div>
                    <div style="display:flex;justify-content:space-between;margin-top:6px;font-family:'Space Mono',monospace;font-size:0.78rem;">
                        <span style="color:#f0f6fc;">${snap['pos_val']:,.2f}</span>
                        <span style="color:{NEON_GREEN if snap['net_profit']>=0 else NEON_RED};">${snap['net_profit']:+.2f}</span>
                    </div>
                    <div style="display:flex;justify-content:space-between;margin-top:4px;font-size:0.68rem;color:#555f6e;">
                        <span>Fee ${snap['cumulative_fees']:.4f}</span>
                        <span>APR {snap['apr']:.1f}%</span>
                        <span>IL ${snap['il']:+.4f}</span>
                    </div>
                </div>""", unsafe_allow_html=True)

    # ==========================================================
    # 選択中のポジション 詳細ビュー
    # ==========================================================
    if selected_pos_id is None:
        st.stop()

    pos_settings = get_position(selected_pos_id)
    if pos_settings is None:
        st.error("ポジションが見つかりません")
        st.stop()

    df_history = load_history(selected_pos_id)
    if df_history.empty:
        st.info(f"「{pos_settings['name']}」にはまだ記録がありません。右パネルからデータを記録してください。")
        st.stop()

    df_history = df_history.sort_values("date").reset_index(drop=True)

    # ---- 個別ポジション計算 ----
    snap = compute_position_snapshot(pos_settings, active_rate_global, df_history)
    accent_color = NEON_AMBER if sim_mode else NEON_GREEN
    phase_start = pd.to_datetime(pos_settings.get("PHASE_START_DATE", datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")))

    ttr_text = "ILなし" if snap["il"] >= 0 else (
        f"回収 {abs(snap['il'])/snap['fee_avg_24h']:.1f}日" if snap["fee_avg_24h"] > 0.001 else "データ不足"
    )

    # ---- アラート ----
    st.markdown(f"---")
    section_header("🔍", f"Detail: {pos_settings['name']}" + (" [SIM]" if sim_mode else ""))

    if not sim_mode:
        if snap["range_pct"] < 5 or snap["range_pct"] > 95:
            st.markdown(f"""<div class="alert-banner" style="background:rgba(255,75,75,0.12);border:1px solid rgba(255,75,75,0.4);color:{NEON_RED};">
                🚨 CRITICAL: レンジ外です！リバランスを検討してください。</div>""", unsafe_allow_html=True)
        elif snap["range_pct"] < 10 or snap["range_pct"] > 90:
            st.markdown(f"""<div class="alert-banner" style="background:rgba(255,109,0,0.10);border:1px solid rgba(255,109,0,0.35);color:{NEON_ORANGE};">
                ⚡ WARNING: レンジ境界に接近中。</div>""", unsafe_allow_html=True)

    range_meter(snap["range_pct"], int(pos_settings["TICK_LOWER"]), int(pos_settings["TICK_UPPER"]),
                snap["p_low"], snap["p_up"], active_rate_global, is_sim=sim_mode)

    # ---- Overview カード ----
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(metric_card("Position Value", f"${snap['pos_val']:,.2f}", f"Rate: {active_rate_global:.4f}", accent=accent_color), unsafe_allow_html=True)
    with c2:
        ra = NEON_RED if (snap["usdc_ratio"] > 85 or snap["usdc_ratio"] < 15) else accent_color
        st.markdown(metric_card("Token Ratio", f"USDC {snap['usdc_ratio']:.0f}%", f"JPYC {snap['jpyc_ratio']:.0f}%", accent=ra), unsafe_allow_html=True)
    with c3: st.markdown(metric_card("Net Profit", f"${snap['net_profit']:.2f}", "IL+手数料+繰越", accent=accent_color, delta_positive=snap["net_profit"]>=0), unsafe_allow_html=True)
    with c4: st.markdown(metric_card("Cumulative Fees", f"${snap['cumulative_fees']:.4f}", "手数料累計"), unsafe_allow_html=True)

    # ---- Performance カード ----
    p1, p2, p3, p4 = st.columns(4)
    with p1: st.markdown(metric_card("IL", f"${snap['il']:,.4f}", ttr_text, accent=NEON_RED if snap["il"]<0 else accent_color, delta_positive=snap["il"]>=0), unsafe_allow_html=True)
    with p2: st.markdown(metric_card("24h Avg Fee", f"${snap['fee_avg_24h']:.4f}", "フェーズ平均"), unsafe_allow_html=True)
    with p3: st.markdown(metric_card("30d Proj.", f"${snap['fee_avg_24h']*30:.2f}", "着地予想"), unsafe_allow_html=True)
    with p4:
        aa = NEON_GREEN if snap["apr"]>=10 else (NEON_BLUE if snap["apr"]>=5 else NEON_RED)
        st.markdown(metric_card("APR", f"{snap['apr']:.1f}%", "年換算", accent=aa), unsafe_allow_html=True)

    # ---- Monthly Progress ----
    fee_target = pos_settings.get("FEE_TARGET_MONTHLY", 30.0)
    if fee_target and fee_target > 0:
        monthly_pace = snap["fee_avg_24h"] * 30
        progress_ratio = min(snap["cumulative_fees"] / fee_target, 1.0)
        pace_ratio = min(monthly_pace / fee_target, 1.5) if fee_target > 0 else 0
        bar_color = NEON_GREEN if pace_ratio >= 1.0 else (NEON_AMBER if pace_ratio >= 0.7 else NEON_RED)
        st.markdown(f"""
        <div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
            border-radius:10px;padding:14px 16px;margin:8px 0;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
                <span style="color:#8b949e;font-size:0.72rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;">🎯 Monthly Fee Progress</span>
                <span style="color:{bar_color};font-size:0.78rem;font-weight:700;">
                    ${snap['cumulative_fees']:.2f} / ${fee_target:.2f} &nbsp;|&nbsp; Pace: ${monthly_pace:.2f}
                </span>
            </div>
            <div style="width:100%;height:8px;background:rgba(255,255,255,0.07);border-radius:4px;overflow:hidden;">
                <div style="width:{progress_ratio*100:.1f}%;height:100%;background:linear-gradient(90deg,{bar_color},{bar_color}aa);border-radius:4px;"></div>
            </div>
            <div style="display:flex;justify-content:space-between;margin-top:6px;font-size:0.68rem;color:#555f6e;">
                <span>達成率 {progress_ratio*100:.1f}%</span>
                <span>残り ${max(fee_target-snap['cumulative_fees'],0):.2f}</span>
            </div>
        </div>""", unsafe_allow_html=True)

    # ---- Analytics ----
    section_header("📊", "Analytics")
    tab_trend, tab_daily, tab_compare, tab_portfolio_chart, tab_oracle, tab_raw = st.tabs([
        "📈 累積トレンド", "📊 手数料モメンタム", "⚖️ LP vs Hold",
        "🏦 ポートフォリオ比較", "🧠 Quant Oracle", "📋 Raw Data"
    ])

    # ===== タブ1: 累積トレンド =====
    with tab_trend:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(
            x=df_history["date"], y=df_history["net_profit_usd"],
            mode="lines+markers", name="実質利益 (IL込)",
            line=dict(color=NEON_GREEN, width=2.5), marker=dict(size=4),
            fill="tozeroy", fillcolor="rgba(0,230,118,0.06)",
        ))
        fig1.add_trace(go.Scatter(
            x=df_history["date"], y=df_history["fees"],
            mode="lines+markers", name="累積手数料",
            line=dict(color=NEON_BLUE, width=2), marker=dict(size=4),
        ))
        if "il" in df_history.columns:
            fig1.add_trace(go.Scatter(
                x=df_history["date"], y=df_history["il"],
                mode="lines", name="IL", line=dict(color=NEON_RED, dash="dot", width=1.8),
            ))
        if fee_target and fee_target > 0:
            fig1.add_hline(y=fee_target, line_color=NEON_AMBER, line_dash="dash",
                           annotation_text=f"月次目標 ${fee_target:.0f}", annotation_position="bottom right",
                           annotation_font_color=NEON_AMBER)
        fig1.add_hline(y=0, line_color="rgba(255,255,255,0.10)", line_dash="dot")
        fig1.update_layout(**plotly_dark_layout(yaxis=dict(tickprefix="$")))
        st.plotly_chart(fig1, use_container_width=True)

    # ===== タブ2: 手数料モメンタム =====
    with tab_daily:
        col_tf, col_interp = st.columns([2, 3])
        with col_tf:
            timeframe = st.radio("集計時間軸", ["6時間", "12時間", "24時間"], horizontal=True, label_visibility="collapsed")
        with col_interp:
            show_interp = st.checkbox("補間データを表示", value=True)
        freq = {"6時間": "6h", "12時間": "12h", "24時間": "24h"}[timeframe]

        df_rs = df_history.copy().set_index("date").resample(freq)["fees"].max()
        if show_interp:
            df_rs = df_rs.interpolate(method="time")
        df_rs = df_rs.reset_index()
        df_rs["period_fee"] = df_rs["fees"].diff().fillna(df_rs["fees"]).clip(lower=0)
        df_rs["24h_pace"] = 0.0
        first_d = df_rs.iloc[0]["date"] if not df_rs.empty else None
        for idx, row in df_rs.iterrows():
            t = row["date"]
            past = df_rs[df_rs["date"] <= t - pd.Timedelta(hours=24)]
            df_rs.at[idx, "24h_pace"] = (
                row["fees"] - past.iloc[-1]["fees"] if not past.empty
                else (row["fees"] / max((t - first_d).total_seconds()/86400, 0.01)) if first_d else 0
            )
        df_rs["ma7"] = df_rs["period_fee"].rolling(7, min_periods=1).mean()

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=df_rs["date"], y=df_rs["period_fee"], name="期間手数料", marker_color=NEON_PURPLE, opacity=0.75))
        fig2.add_trace(go.Scatter(x=df_rs["date"], y=df_rs["ma7"], mode="lines", name="MA(7)", line=dict(color=NEON_AMBER, width=1.5, dash="dash")))
        fig2.add_trace(go.Scatter(x=df_rs["date"], y=df_rs["24h_pace"], mode="lines+markers", name="24h ペース", line=dict(color=NEON_GREEN, width=2)))
        fig2.update_layout(**plotly_dark_layout(yaxis=dict(tickprefix="$")))
        st.plotly_chart(fig2, use_container_width=True)
        if show_interp:
            st.caption("⚠️ 補間ON: 欠損期間は線形補完")

    # ===== タブ3: LP vs Hold =====
    with tab_compare:
        if "hold_val_usd" in df_history.columns and "lp_val_usd" in df_history.columns:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=pd.concat([df_history["date"], df_history["date"].iloc[::-1]]),
                y=pd.concat([df_history["lp_val_usd"], df_history["hold_val_usd"].iloc[::-1]]),
                fill="toself", fillcolor="rgba(0,176,255,0.06)",
                line=dict(color="rgba(0,0,0,0)"), hoverinfo="skip", showlegend=False,
            ))
            fig3.add_trace(go.Scatter(x=df_history["date"], y=df_history["lp_val_usd"],
                mode="lines+markers", name="LP価値", line=dict(color=NEON_GREEN, width=2.5), marker=dict(size=4)))
            fig3.add_trace(go.Scatter(x=df_history["date"], y=df_history["hold_val_usd"],
                mode="lines", name="HOLD", line=dict(color=NEON_BLUE, width=2, dash="dash")))
            fig3.add_hline(y=snap["base_capital"], line_color="rgba(255,255,255,0.15)", line_dash="dot",
                           annotation_text="元本", annotation_position="bottom right")
            diff_s = df_history["lp_val_usd"] - df_history["hold_val_usd"]
            fig3.add_trace(go.Bar(x=df_history["date"], y=diff_s, name="差分",
                marker_color=[NEON_GREEN if v>=0 else NEON_RED for v in diff_s], opacity=0.5, yaxis="y2"))
            fig3.update_layout(**plotly_dark_layout(
                yaxis=dict(title="USD", tickprefix="$"),
                yaxis2=dict(title="差分", overlaying="y", side="right", showgrid=False, tickprefix="$"),
                barmode="overlay",
            ))
            st.plotly_chart(fig3, use_container_width=True)
            avg_o = diff_s.mean()
            st.info(f"📊 平均超過収益: **${avg_o:+.2f}** | LP優位: **{(diff_s>0).sum()}**/{len(diff_s)} 回")
        else:
            st.info("データが2件以上必要です。")

    # ===== タブ4: ポートフォリオ比較 =====
    with tab_portfolio_chart:
        if n_pos < 2:
            st.info("比較にはポジションが2つ以上必要です。")
        else:
            chart_mode = st.radio("比較指標", ["累積手数料", "実質利益", "LP価値", "APR"], horizontal=True, key="pf_mode")
            col_map = {"累積手数料": "fees", "実質利益": "net_profit_usd", "LP価値": "lp_val_usd", "APR": None}
            col_key = col_map[chart_mode]

            fig_pf = go.Figure()
            if col_key:
                for pid, pname, snap_i, df_h, color in portfolio_data:
                    if df_h.empty or col_key not in df_h.columns:
                        continue
                    df_sorted = df_h.sort_values("date")
                    fig_pf.add_trace(go.Scatter(
                        x=df_sorted["date"], y=df_sorted[col_key],
                        mode="lines+markers", name=pname,
                        line=dict(color=color, width=2), marker=dict(size=4),
                    ))
                fig_pf.update_layout(**plotly_dark_layout(
                    title=f"ポジション比較: {chart_mode}",
                    yaxis=dict(tickprefix="$"),
                ))
            else:
                # APR棒グラフ
                names = [pname for _, pname, _, _, _ in portfolio_data]
                aprs  = [snap_i["apr"] for _, _, snap_i, _, _ in portfolio_data]
                colors = [c for _, _, _, _, c in portfolio_data]
                fig_pf.add_trace(go.Bar(x=names, y=aprs, marker_color=colors))
                fig_pf.update_layout(**plotly_dark_layout(
                    title="ポジション別 APR 比較",
                    yaxis=dict(ticksuffix="%"),
                ))
            st.plotly_chart(fig_pf, use_container_width=True)

            # ポートフォリオ構成比
            st.markdown("##### 📊 ポートフォリオ構成比")
            vals = [snap_i["pos_val"] for _, _, snap_i, _, _ in portfolio_data]
            names = [pname for _, pname, _, _, _ in portfolio_data]
            colors_list = [c for _, _, _, _, c in portfolio_data]
            fig_pie = go.Figure(go.Pie(
                labels=names, values=vals,
                marker=dict(colors=colors_list),
                hole=0.45, textinfo="label+percent",
                textfont=dict(size=12, color="#f0f6fc"),
            ))
            fig_pie.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#8b949e"), margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False, height=300,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    # ===== タブ5: Quant Oracle =====
    with tab_oracle:
        st.markdown("#### 🤖 ボラティリティ分析 & 最適レンジ提案")

        # 全ポジションの履歴を結合して分析
        all_hist = load_history_all()
        all_rates = all_hist["rate"].dropna().values if not all_hist.empty else np.array([])

        if len(all_rates) >= 3:
            rate_std  = float(np.std(all_rates)) or 1.5
            rate_mean = float(np.mean(all_rates))
            rate_min  = float(np.min(all_rates))
            rate_max  = float(np.max(all_rates))

            x_range = np.linspace(rate_min * 0.97, rate_max * 1.03, 300)
            gaussian = np.exp(-0.5*((x_range-rate_mean)/rate_std)**2) / (rate_std*np.sqrt(2*np.pi))

            fig_vol = go.Figure()
            fig_vol.add_trace(go.Histogram(x=all_rates, nbinsx=min(30, len(all_rates)),
                name="レート分布", marker_color=NEON_BLUE, opacity=0.6, histnorm="probability density"))
            fig_vol.add_trace(go.Scatter(x=x_range, y=gaussian, mode="lines", name="正規分布",
                line=dict(color=NEON_GREEN, width=2)))
            for sigma, color, label in [(1, NEON_GREEN, "±1σ"), (2.5, NEON_AMBER, "±2.5σ")]:
                for sign in [1, -1]:
                    fig_vol.add_vline(x=rate_mean+sign*sigma*rate_std, line_color=color, line_dash="dash", line_width=1,
                        annotation_text=label if sign==1 else "", annotation_font_color=color, annotation_position="top")
            fig_vol.add_vline(x=active_rate_global, line_color=NEON_RED, line_width=2,
                annotation_text="現在", annotation_font_color=NEON_RED)
            # 各ポジションのレンジも表示
            for pid, pname, snap_i, _, color in portfolio_data:
                p = get_position(pid)
                if p:
                    fig_vol.add_vrect(x0=tick_to_price(int(p["TICK_LOWER"])), x1=tick_to_price(int(p["TICK_UPPER"])),
                        fillcolor=color, opacity=0.08, line_width=0,
                        annotation_text=pname, annotation_position="top left", annotation_font_color=color, annotation_font_size=9)
            fig_vol.update_layout(**plotly_dark_layout(title="過去レート分布 & 各ポジションレンジ",
                xaxis=dict(title="JPYC/USDC"), yaxis=dict(title="確率密度")))
            st.plotly_chart(fig_vol, use_container_width=True)

            col_n, col_w = st.columns(2)
            for col, sigma, label, style, desc in [
                (col_n, 1.0, "🟢 Narrow (±1σ)", "info", "資金効率最大化"),
                (col_w, 2.5, "🛡️ Wide (±2.5σ)", "success", "安定運用"),
            ]:
                with col:
                    lo = max(active_rate_global - sigma*rate_std, 0.001)
                    hi = active_rate_global + sigma*rate_std
                    t_lo, t_hi = price_to_tick(lo), price_to_tick(hi)
                    getattr(st, style)(f"**{label}**\n\nMin: `{t_lo}` ({lo:.3f}) / Max: `{t_hi}` ({hi:.3f})\n\n*{desc}*")
            st.markdown("---")
        else:
            st.info("分析には3件以上の記録が必要です。")

        # リバランス ROI
        st.markdown("#### ⚖️ リバランス ROI")
        col_g1, col_g2 = st.columns(2)
        with col_g1: gas_usd = st.number_input("想定ガス代 (USD)", value=1.5, step=0.1, min_value=0.0)
        with col_g2: exp_fee = st.number_input("想定24h手数料 ($)", value=max(snap["fee_avg_24h"], 0.01), step=0.01, min_value=0.001)

        if exp_fee > 0:
            be_days = gas_usd / exp_fee
            roi_30 = (exp_fee*30-gas_usd)/gas_usd*100 if gas_usd > 0 else 0
            if be_days < 3:   st.success(f"✅ **推奨** — 回収: **{be_days:.1f}日** / 30日ROI: **{roi_30:.0f}%**")
            elif be_days < 7: st.warning(f"⚠️ **要検討** — 回収: **{be_days:.1f}日** / 30日ROI: **{roi_30:.0f}%**")
            else:             st.error(f"❌ **非推奨** — 回収: **{be_days:.1f}日** / 30日ROI: **{roi_30:.0f}%**")

            days_r = np.arange(0, 31, 1)
            fig_roi = go.Figure()
            fig_roi.add_trace(go.Scatter(x=days_r, y=exp_fee*days_r, mode="lines", name="累積収益", line=dict(color=NEON_BLUE, width=2)))
            fig_roi.add_trace(go.Scatter(x=days_r, y=exp_fee*days_r-gas_usd, mode="lines", name="純利益", line=dict(color=NEON_GREEN, width=2), fill="tozeroy", fillcolor="rgba(0,230,118,0.06)"))
            fig_roi.add_hline(y=0, line_color="rgba(255,255,255,0.2)", line_dash="dot")
            fig_roi.add_vline(x=be_days, line_color=NEON_AMBER, line_dash="dash", annotation_text=f"BEP {be_days:.1f}日", annotation_font_color=NEON_AMBER)
            fig_roi.update_layout(**plotly_dark_layout(title="30日シミュレーション", xaxis=dict(title="日数"), yaxis=dict(title="USD", tickprefix="$")))
            st.plotly_chart(fig_roi, use_container_width=True)

    # ===== タブ6: Raw Data =====
    with tab_raw:
        data_scope = st.radio("表示対象", ["選択中のポジション", "全ポジション"], horizontal=True)
        df_show = df_history if data_scope == "選択中のポジション" else load_history_all()

        col_sort, col_dl = st.columns([3, 2])
        with col_sort: sort_asc = st.checkbox("昇順で表示", value=False)
        with col_dl:
            st.download_button("⬇️ CSV DL", data=df_show.to_csv(index=False),
                file_name=f"lp_history_{datetime.now(JST).strftime('%Y%m%d')}.csv",
                mime="text/csv", use_container_width=True)

        display_df = df_show.sort_values("date", ascending=sort_asc)
        fmt_map = {"rate": "{:.4f}", "usdc": "{:.2f}", "jpyc": "{:,.0f}", "fees": "${:.4f}",
                   "il": "${:.4f}", "hold_val_usd": "${:.2f}", "lp_val_usd": "${:.2f}", "net_profit_usd": "${:.4f}"}
        valid_fmt = {k: v for k, v in fmt_map.items() if k in display_df.columns}
        st.dataframe(display_df.style.format(valid_fmt), use_container_width=True, height=350)
        st.caption(f"合計 {len(df_show)} 件")
