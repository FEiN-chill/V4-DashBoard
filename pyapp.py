import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import requests
import math
import os
import io
import numpy as np
from streamlit_gsheets import GSheetsConnection # ← ここを追加！

# ============================================================
# 1. 基本設定
# ============================================================
JST = timezone(timedelta(hours=+9), "JST")
st.set_page_config(
    page_title="Uniswap V4 LP Dashboard",
    layout="wide",
    page_icon="⚡",
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
API_KEY      = "7a401bdd8853e9f92d3cdd8819a77a73"
SUBGRAPH_ID  = "2CB2uQxcDKWDenagn2z17KQVCtfwSx5eXYuvqTciRTJu"
POOL_ID      = "0x3befb3625d0a36cb4bb84ff1c549dc5092fe8e6f527661c9e4825762b5cea727"

NEON_GREEN  = "#00E676"
NEON_BLUE   = "#00B0FF"
NEON_RED    = "#ff4b4b"
NEON_PURPLE = "#D500F9"
NEON_AMBER  = "#FFD600"
NEON_ORANGE = "#FF6D00"

# --- スプレッドシート接続 ---
conn = st.connection("gsheets", type=GSheetsConnection)

# ============================================================
# 3. データ管理 (クラウドDB仕様に差し替え)
# ============================================================
def load_settings() -> dict:
    defaults = {
        "INITIAL_USDC": 478.14, "INITIAL_JPYC": 86135.12,
        "TICK_LOWER": 326810, "TICK_UPPER": 327250,
        "CARRYOVER_PROFIT": 0.0, "CARRYOVER_FEES": 0.0,
        "BASE_DATE": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "PHASE_START_DATE": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "FEE_TARGET_MONTHLY": 30.0,
    }
    try:
        df = conn.read(worksheet="settings", ttl=600)
        df = df.dropna(subset=['key'])
        s = dict(zip(df["key"], df["value"]))
        for k in ["INITIAL_USDC", "INITIAL_JPYC", "TICK_UPPER", "TICK_LOWER", "CARRYOVER_PROFIT", "CARRYOVER_FEES", "FEE_TARGET_MONTHLY"]:
            s[k] = float(s.get(k, defaults.get(k, 0)))
        return s
    except Exception:
        return defaults

def save_settings(s: dict):
    df = pd.DataFrame(list(s.items()), columns=["key", "value"])
    conn.update(worksheet="settings", data=df)
    st.cache_data.clear()

def load_history() -> pd.DataFrame:
    try:
        df = conn.read(worksheet="history", ttl=600)
        df = df.dropna(how="all")
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
        return df
    except Exception:
        return pd.DataFrame()

def save_history(new_df: pd.DataFrame):
    existing = load_history()
    combined = pd.concat([existing, new_df], ignore_index=True) if not existing.empty else new_df
    
    # ↓↓↓ 【ここを追加】並び替える前に、すべてを確実に「日時データ」に変換して統一する ↓↓↓
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    # ↑↑↑
    
    combined = combined.drop_duplicates(subset=["date"], keep="last")
    combined = combined.sort_values("date").reset_index(drop=True)
    conn.update(worksheet="history", data=combined)
    st.cache_data.clear()

def reset_history():
    cols = ["date", "rate", "usdc", "jpyc", "fees", "il", "hold_val_usd", "lp_val_usd", "net_profit_usd"]
    conn.update(worksheet="history", data=pd.DataFrame(columns=cols))
    st.cache_data.clear()

def import_history_csv(uploaded_file) -> tuple[bool, str]:
    try:
        df_new = pd.read_csv(uploaded_file)
        df_new["date"] = pd.to_datetime(df_new["date"])
        required = {"date", "rate", "fees"}
        if not required.issubset(df_new.columns):
            return False, f"必須列が不足しています: {required - set(df_new.columns)}"
        existing = load_history()
        combined = pd.concat([existing, df_new], ignore_index=True) if not existing.empty else df_new
        combined = combined.drop_duplicates(subset=["date"], keep="last")
        combined = combined.sort_values("date").reset_index(drop=True)
        conn.update(worksheet="history", data=combined)
        st.cache_data.clear()
        return True, f"{len(df_new)} 件をインポートしました（重複は上書き）"
    except Exception as e:
        return False, str(e)

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
    if price <= 0: return 0
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
    if A == 0: return 0.0, 0.0, tick_to_price(tick_lower), tick_to_price(tick_upper)
    disc = max(0, B**2 - 4 * A * C)
    v = (-B + math.sqrt(disc)) / (2 * A)
    L = Y / (v - sqrtP_L)
    if sqrtP_curr <= sqrtP_L: amt0 = L * (sqrtP_U - sqrtP_L) / (sqrtP_L * sqrtP_U); amt1 = 0.0
    elif sqrtP_curr >= sqrtP_U: amt0 = 0.0; amt1 = L * (sqrtP_U - sqrtP_L)
    else: amt0 = L * (sqrtP_U - sqrtP_curr) / (sqrtP_curr * sqrtP_U); amt1 = L * (sqrtP_curr - sqrtP_L)
    return amt0 / d0, amt1 / d1, tick_to_price(tick_lower), tick_to_price(tick_upper)

def compute_fee_stats(df_history: pd.DataFrame, phase_start_date) -> float:
    actual_phase_start = phase_start_date
    if not df_history.empty and df_history.iloc[0]["date"] < phase_start_date:
        actual_phase_start = df_history.iloc[0]["date"]
    df_phase = df_history[df_history["date"] >= actual_phase_start]
    fee_avg_24h = 0.0
    if len(df_phase) >= 2:
        p_days = (df_phase.iloc[-1]["date"] - df_phase.iloc[0]["date"]).total_seconds() / 86400
        if p_days > 0.01: fee_avg_24h = (df_phase.iloc[-1]["fees"] - df_phase.iloc[0]["fees"]) / p_days
    return fee_avg_24h

# ============================================================
# 5. UI コンポーネント
# ============================================================
def metric_card(title, value, sub="", accent=NEON_GREEN, delta_positive=None):
    ind = ""
    if delta_positive is True:  ind = f"<span style='color:{NEON_GREEN};'>▲</span> "
    elif delta_positive is False: ind = f"<span style='color:{NEON_RED};'>▼</span> "
    return f"""
    <div style="background:linear-gradient(145deg,#111720,#0d1117);border:1px solid rgba(255,255,255,0.08);
        border-top:2px solid {accent};border-radius:10px;padding:18px 16px 14px;min-height:110px;
        display:flex;flex-direction:column;justify-content:space-between;">
        <div style="color:#8b949e;font-size:0.72rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;">{title}</div>
        <div style="color:#f0f6fc;font-size:1.55rem;font-weight:700;font-family:'Space Mono',monospace;line-height:1.1;">{value}</div>
        <div style="color:{accent};font-size:0.78rem;margin-top:6px;">{ind}{sub}</div>
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

def range_meter(range_pct, tick_lower, tick_upper, p_low, p_up, current_rate, is_sim=False):
    in_range = 5 <= range_pct <= 95
    warn_zone = range_pct < 10 or range_pct > 90
    dot_color = NEON_AMBER if is_sim else (NEON_RED if not in_range else (NEON_ORANGE if warn_zone else NEON_GREEN))
    if is_sim: status = "SIMULATION ACTIVE"
    elif not in_range: status = "⚠ OUT OF RANGE"
    elif warn_zone: status = "⚡ APPROACHING BOUNDARY"
    else: status = "IN RANGE ✓"

    st.markdown(f"""
    <div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:14px 16px;margin-bottom:12px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
            <span style="color:#8b949e;font-size:0.72rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;">📍 Range Position</span>
            <span style="color:{dot_color};font-size:0.75rem;font-weight:700;">{status}</span>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:0.68rem;color:#555f6e;margin-bottom:6px;font-family:'Space Mono',monospace;">
            <span>{p_low:.4f}<br>Tick {tick_lower}</span>
            <span style="text-align:right;">{p_up:.4f}<br>Tick {tick_upper}</span>
        </div>
        <div style="position:relative;width:100%;height:10px;border-radius:5px;
            background:linear-gradient(to right,{NEON_RED} 0%,{NEON_RED} 5%,{NEON_ORANGE} 10%,{NEON_GREEN} 20%,{NEON_GREEN} 80%,{NEON_ORANGE} 90%,{NEON_RED} 95%,{NEON_RED} 100%);
            overflow:visible;">
            <div style="width:14px;height:14px;background:{dot_color};border-radius:50%;box-shadow:0 0 12px {dot_color};
                position:absolute;top:50%;left:calc({range_pct:.1f}% - 7px);transform:translateY(-50%);z-index:2;"></div>
        </div>
        <div style="display:flex;justify-content:space-between;margin-top:8px;font-size:0.68rem;color:#555f6e;font-family:'Space Mono',monospace;">
            <span>距離(下): {abs(current_rate - p_low):.4f}</span>
            <span style="color:{dot_color};font-weight:700;">{range_pct:.1f}%</span>
            <span>距離(上): {abs(p_up - current_rate):.4f}</span>
        </div>
    </div>""", unsafe_allow_html=True)

def plotly_dark_layout(**kwargs):
    base = dict(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=30, b=0),
        hovermode="x unified",
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
if "in_rate" not in st.session_state: st.session_state.in_rate = 160.0
if "confirm_reset" not in st.session_state: st.session_state.confirm_reset = False

settings = load_settings()
phase_start_date = pd.to_datetime(settings.get("PHASE_START_DATE", datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")))

st.markdown("""
    <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:0.5rem;">
        <h1 style="margin:0;font-size:1.6rem;color:#f0f6fc;font-family:'Space Mono',monospace;">⚡ Uniswap V4</h1>
        <span style="color:#00E676;font-size:0.7rem;font-weight:700;letter-spacing:0.15em;
            background:rgba(0,230,118,0.1);border:1px solid rgba(0,230,118,0.3);
            padding:2px 8px;border-radius:20px;font-family:'Space Mono',monospace;">LP DASHBOARD v3</span>
    </div>
    <div style="color:#555f6e;font-size:0.78rem;margin-bottom:1rem;font-family:'Space Mono',monospace;">
        JPYC / USDC · POOL ANALYTICS & STRATEGY
    </div>
""", unsafe_allow_html=True)

col_main, col_right = st.columns([3, 1], gap="large")

# ------------------------------------------------------------
# 右サイドパネル
# ------------------------------------------------------------
with col_right:
    panel_label("⚡ Auto Fetch")
    if st.button("🔄 The Graph から最新取得", use_container_width=True):
        with st.spinner("オンチェーンデータを取得中..."):
            fetched = fetch_pool_data()
        if fetched:
            st.session_state.in_rate = fetched
            st.success(f"✅ {fetched:.4f} JPYC/USDC")
        else:
            st.error("❌ 取得失敗。APIキーを確認してください。")

    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

    panel_label("🔮 What-If Simulator", color=NEON_AMBER)
    with st.container(border=True):
        sim_mode = st.toggle("シミュレーションモード")
        if sim_mode:
            sim_rate = st.slider(
                "仮定のレート (JPYC/USDC)",
                min_value=130.0, max_value=190.0,
                value=float(st.session_state.in_rate), step=0.5,
            )
            st.caption("⚠️ 左の画面がシミュレーション結果になります")
        else:
            sim_rate = st.session_state.in_rate

    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

    panel_label("📝 Record Data")
    with st.container(border=True):
        live_rate = st.number_input(
            "現在レート (JPYC/USDC)", value=float(st.session_state.in_rate), format="%.4f",
            help="The Graph から自動取得するか、手動で入力してください"
        )
        st.session_state.in_rate = live_rate

        now_usdc, now_jpyc, _, _ = calculate_exact_holdings(
            live_rate, settings["TICK_LOWER"], settings["TICK_UPPER"],
            settings["INITIAL_USDC"], settings["INITIAL_JPYC"]
        )
        st.markdown(f"""
        <div style="background:rgba(0,230,118,0.06);border:1px solid rgba(0,230,118,0.15);
            border-radius:8px;padding:10px 12px;margin:4px 0 8px;font-family:'Space Mono',monospace;font-size:0.78rem;">
            <div style="color:#8b949e;font-size:0.68rem;margin-bottom:4px;">算出枚数</div>
            <div style="color:#f0f6fc;">{now_usdc:.2f} <span style="color:#8b949e;">USDC</span></div>
            <div style="color:#f0f6fc;">{now_jpyc:,.0f} <span style="color:#8b949e;">JPYC</span></div>
        </div>""", unsafe_allow_html=True)

        df_hist_tmp = load_history()
        last_fees = float(df_hist_tmp.iloc[-1]["fees"]) if not df_hist_tmp.empty else 0.0
        earned_fees = st.number_input("累積回収済手数料 ($)", value=last_fees, step=0.01, format="%.4f")

        if not sim_mode:
            if st.button("⚡ データを記録", type="primary", use_container_width=True):
                save_hold = settings["INITIAL_USDC"] + settings["INITIAL_JPYC"] / live_rate
                save_lp   = now_usdc + now_jpyc / live_rate
                il        = save_lp - save_hold
                net_p     = il + earned_fees + settings["CARRYOVER_PROFIT"]
                new_row   = pd.DataFrame([{
                    "date": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
                    "rate": live_rate, "usdc": now_usdc, "jpyc": now_jpyc,
                    "fees": earned_fees, "il": il,
                    "hold_val_usd": save_hold, "lp_val_usd": save_lp, "net_profit_usd": net_p,
                }])
                with st.spinner("スプレッドシートへ記録中..."): save_history(new_row)
                st.toast("✅ クラウドへ記録しました！", icon="☁️")
                st.rerun()
        else:
            st.button("⚡ データを記録", disabled=True, use_container_width=True, help="シミュレーション中は記録できません")

    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

    panel_label("⚙️ Position Manage")
    tab_init, tab_rebuild = st.tabs(["🆕 新規", "🔄 再構築"])
    with tab_init:
        st.caption("⚠️ 履歴をリセットして新規開始")
        n_up   = st.number_input("Max Tick", value=int(settings["TICK_UPPER"]), step=10, key="i_up")
        n_low  = st.number_input("Min Tick", value=int(settings["TICK_LOWER"]), step=10, key="i_low")
        n_usdc = st.number_input("初期 USDC", value=settings["INITIAL_USDC"], format="%.2f", key="i_u")
        n_jpyc = st.number_input("初期 JPYC", value=settings["INITIAL_JPYC"], format="%.2f", key="i_j")
        n_target = st.number_input("月次手数料目標 ($)", value=settings.get("FEE_TARGET_MONTHLY", 30.0), format="%.2f", key="i_t")
        invalid = n_low >= n_up
        if invalid: st.error("Min Tick < Max Tick にしてください")

        if not st.session_state.confirm_reset:
            if st.button("🚀 新規スタート", use_container_width=True, disabled=invalid):
                st.session_state.confirm_reset = True
                st.rerun()
        else:
            st.warning("本当にリセットしますか？")
            c_yes, c_no = st.columns(2)
            with c_yes:
                if st.button("✅ はい", use_container_width=True):
                    settings.update({
                        "INITIAL_USDC": n_usdc, "INITIAL_JPYC": n_jpyc, "TICK_UPPER": n_up, "TICK_LOWER": n_low,
                        "CARRYOVER_PROFIT": 0.0, "CARRYOVER_FEES": 0.0, "FEE_TARGET_MONTHLY": n_target,
                        "BASE_DATE": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
                        "PHASE_START_DATE": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    save_settings(settings); reset_history()
                    st.session_state.confirm_reset = False
                    st.rerun()
            with c_no:
                if st.button("❌ いいえ", use_container_width=True):
                    st.session_state.confirm_reset = False
                    st.rerun()

    with tab_rebuild:
        st.caption("利益を確定し新 Tick で組み直し")
        r_up   = st.number_input("新・Max Tick", value=int(settings["TICK_UPPER"]), step=10, key="r_up")
        r_low  = st.number_input("新・Min Tick", value=int(settings["TICK_LOWER"]), step=10, key="r_low")
        r_usdc = st.number_input("新・初期 USDC", value=float(now_usdc), format="%.2f", key="r_u")
        r_jpyc = st.number_input("新・初期 JPYC", value=float(now_jpyc), format="%.2f", key="r_j")
        r_profit = st.number_input("今回の確定利益 ($)", value=0.0, format="%.2f", key="r_p")
        if st.button("🔄 再構築を反映", use_container_width=True):
            settings["CARRYOVER_PROFIT"] = float(settings.get("CARRYOVER_PROFIT", 0)) + r_profit
            settings.update({
                "INITIAL_USDC": r_usdc, "INITIAL_JPYC": r_jpyc, "TICK_UPPER": r_up, "TICK_LOWER": r_low,
                "PHASE_START_DATE": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
            })
            save_settings(settings)
            st.toast("✅ 再構築を反映しました", icon="🔄")
            st.rerun()

    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

    panel_label("💾 Data Management")
    with st.expander("CSVインポート / エクスポート"):
        df_export = load_history()
        if not df_export.empty:
            csv_buf = io.StringIO()
            df_export.to_csv(csv_buf, index=False)
            st.download_button(
                "⬇️ 履歴をエクスポート", data=csv_buf.getvalue(),
                file_name=f"lp_history_{datetime.now(JST).strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv", use_container_width=True,
            )
        uploaded = st.file_uploader("CSVをインポート", type=["csv"], label_visibility="collapsed")
        if uploaded is not None:
            ok, msg = import_history_csv(uploaded)
            if ok: st.success(msg); st.rerun()
            else: st.error(f"インポート失敗: {msg}")

# ------------------------------------------------------------
# メインパネル
# ------------------------------------------------------------
with col_main:
    df_history = load_history()

    if df_history.empty:
        st.markdown("""
        <div style="text-align:center;padding:80px 20px;border:1px dashed rgba(255,255,255,0.1);
            border-radius:16px;background:rgba(255,255,255,0.02);">
            <div style="font-size:3rem;margin-bottom:16px;">📊</div>
            <div style="color:#8b949e;font-size:1rem;font-weight:500;">まだデータがありません</div>
            <div style="color:#555f6e;font-size:0.82rem;margin-top:8px;">
                右パネルの「新規スタート」でポジションを登録してください
            </div>
        </div>""", unsafe_allow_html=True)
        st.stop()

    df_history = df_history.sort_values("date").reset_index(drop=True)
    latest = df_history.iloc[-1]

    active_rate  = sim_rate if sim_mode else float(latest["rate"])
    accent_color = NEON_AMBER if sim_mode else NEON_GREEN

    calc_usdc, calc_jpyc, p_low, p_up = calculate_exact_holdings(
        active_rate, settings["TICK_LOWER"], settings["TICK_UPPER"],
        settings["INITIAL_USDC"], settings["INITIAL_JPYC"]
    )
    position_val_usd = calc_usdc + calc_jpyc / active_rate
    usdc_ratio = (calc_usdc / position_val_usd * 100) if position_val_usd > 0 else 50.0
    jpyc_ratio = 100.0 - usdc_ratio

    range_pct = ((active_rate - p_low) / (p_up - p_low) * 100) if p_up > p_low else 0.0
    range_pct = max(0.0, min(100.0, range_pct))

    hold_val_usd = settings["INITIAL_USDC"] + settings["INITIAL_JPYC"] / active_rate
    il_val       = position_val_usd - hold_val_usd
    cumulative_fees = float(latest["fees"])
    net_profit   = il_val + cumulative_fees + settings["CARRYOVER_PROFIT"]

    fee_avg_24h  = compute_fee_stats(df_history, phase_start_date)
    base_capital = settings["INITIAL_USDC"] + settings["INITIAL_JPYC"] / 160
    apr_pct      = (fee_avg_24h * 365 / base_capital * 100) if base_capital > 0 else 0.0

    ttr_text = "ILなし" if il_val >= 0 else (
        f"回収目安 {abs(il_val)/fee_avg_24h:.1f}日" if fee_avg_24h > 0.001 else "データ不足"
    )

    if not sim_mode:
        if range_pct < 5 or range_pct > 95:
            st.markdown(f"""<div class="alert-banner" style="background:rgba(255,75,75,0.12);border:1px solid rgba(255,75,75,0.4);color:{NEON_RED};">
                🚨 CRITICAL: ポジションがレンジ外です！即座にリバランスを検討してください。</div>""", unsafe_allow_html=True)
        elif range_pct < 10 or range_pct > 90:
            st.markdown(f"""<div class="alert-banner" style="background:rgba(255,109,0,0.10);border:1px solid rgba(255,109,0,0.35);color:{NEON_ORANGE};">
                ⚡ WARNING: レンジ境界に近づいています。状況を注視してください。</div>""", unsafe_allow_html=True)

    range_meter(range_pct, int(settings["TICK_LOWER"]), int(settings["TICK_UPPER"]),
                p_low, p_up, active_rate, is_sim=sim_mode)

    section_header("📈", "Overview" + (" [SIMULATION]" if sim_mode else ""))
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(metric_card("Position Value", f"${position_val_usd:,.2f}", f"Rate: {active_rate:.4f}", accent=accent_color), unsafe_allow_html=True)
    with c2:
        ratio_accent = NEON_RED if (usdc_ratio > 85 or usdc_ratio < 15) else accent_color
        st.markdown(metric_card("Token Ratio", f"USDC {usdc_ratio:.0f}%", f"JPYC {jpyc_ratio:.0f}%", accent=ratio_accent), unsafe_allow_html=True)
    with c3: st.markdown(metric_card("Net Profit", f"${net_profit:.2f}", "IL + 手数料 + 繰越", accent=accent_color, delta_positive=net_profit >= 0), unsafe_allow_html=True)
    with c4: st.markdown(metric_card("Cumulative Fees", f"${cumulative_fees:.4f}", "手数料累計"), unsafe_allow_html=True)

    section_header("⚡", "Performance Metrics" + (" [SIMULATION]" if sim_mode else ""))
    p1, p2, p3, p4 = st.columns(4)
    with p1: st.markdown(metric_card("Impermanent Loss", f"${il_val:,.4f}", ttr_text, accent=NEON_RED if il_val < 0 else accent_color, delta_positive=il_val >= 0), unsafe_allow_html=True)
    with p2: st.markdown(metric_card("24h Avg Fee", f"${fee_avg_24h:.4f}", "フェーズ平均"), unsafe_allow_html=True)
    with p3: st.markdown(metric_card("30d Projection", f"${fee_avg_24h*30:.2f}", "着地予想"), unsafe_allow_html=True)
    with p4:
        apr_accent = NEON_GREEN if apr_pct >= 10 else (NEON_BLUE if apr_pct >= 5 else NEON_RED)
        st.markdown(metric_card("APR", f"{apr_pct:.1f}%", "年換算", accent=apr_accent), unsafe_allow_html=True)

    fee_target = settings.get("FEE_TARGET_MONTHLY", 30.0)
    if fee_target > 0:
        now_jst = datetime.now(JST)
        days_in_month = 30
        elapsed_days = min((now_jst - phase_start_date.replace(tzinfo=JST)).days + 1, days_in_month)
        monthly_pace = fee_avg_24h * days_in_month
        progress_ratio = min(cumulative_fees / fee_target, 1.0) if fee_target > 0 else 0
        pace_ratio = min(monthly_pace / fee_target, 1.5)
        bar_color = NEON_GREEN if pace_ratio >= 1.0 else (NEON_AMBER if pace_ratio >= 0.7 else NEON_RED)
        st.markdown(f"""
        <div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);
            border-radius:10px;padding:14px 16px;margin:8px 0;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
                <span style="color:#8b949e;font-size:0.72rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;">🎯 Monthly Fee Progress</span>
                <span style="color:{bar_color};font-size:0.78rem;font-weight:700;">
                    ${cumulative_fees:.2f} / ${fee_target:.2f}
                    &nbsp;|&nbsp; 30日ペース: ${monthly_pace:.2f}
                </span>
            </div>
            <div style="width:100%;height:8px;background:rgba(255,255,255,0.07);border-radius:4px;overflow:hidden;">
                <div style="width:{progress_ratio*100:.1f}%;height:100%;background:linear-gradient(90deg,{bar_color},{bar_color}aa);
                    border-radius:4px;transition:width 0.5s;"></div>
            </div>
            <div style="display:flex;justify-content:space-between;margin-top:6px;font-size:0.68rem;color:#555f6e;">
                <span>達成率 {progress_ratio*100:.1f}%</span>
                <span>目標まで ${max(fee_target-cumulative_fees,0):.2f}</span>
            </div>
        </div>""", unsafe_allow_html=True)

    section_header("📊", "Analytics & Strategy")
    tab_trend, tab_daily, tab_compare, tab_oracle, tab_raw = st.tabs([
        "📈 累積トレンド", "📊 手数料モメンタム", "⚖️ LP vs Hold", "🧠 Quant Oracle", "📋 Raw Data"
    ])

    with tab_trend:
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(
            x=df_history["date"], y=df_history["net_profit_usd"],
            mode="lines+markers", name="実質利益 (IL込)",
            line=dict(color=NEON_GREEN, width=2.5),
            marker=dict(size=4), fill="tozeroy", fillcolor="rgba(0,230,118,0.06)",
        ))
        fig1.add_trace(go.Scatter(
            x=df_history["date"], y=df_history["fees"],
            mode="lines+markers", name="累積手数料",
            line=dict(color=NEON_BLUE, width=2), marker=dict(size=4),
        ))
        if "il" in df_history.columns:
            fig1.add_trace(go.Scatter(
                x=df_history["date"], y=df_history["il"],
                mode="lines", name="IL",
                line=dict(color=NEON_RED, dash="dot", width=1.8),
            ))
        if fee_target > 0:
            fig1.add_hline(y=fee_target, line_color=NEON_AMBER, line_dash="dash",
                           annotation_text=f"月次目標 ${fee_target:.0f}", annotation_position="bottom right",
                           annotation_font_color=NEON_AMBER)
        fig1.add_hline(y=0, line_color="rgba(255,255,255,0.10)", line_dash="dot")
        fig1.update_layout(**plotly_dark_layout(yaxis=dict(tickprefix="$", gridcolor="rgba(255,255,255,0.04)")))
        st.plotly_chart(fig1, use_container_width=True)

    with tab_daily:
        col_tf, col_interp = st.columns([2, 3])
        with col_tf: timeframe = st.radio("集計時間軸", ["6時間", "12時間", "24時間"], horizontal=True, label_visibility="collapsed")
        with col_interp: show_interp = st.checkbox("補間データを表示", value=True, help="欠損期間を線形補間で埋めます")
        freq = {"6時間": "6h", "12時間": "12h", "24時間": "24h"}[timeframe]

        df_rs = df_history.copy().set_index("date").resample(freq)["fees"].max()
        if show_interp: df_rs = df_rs.interpolate(method="time")
        df_rs = df_rs.reset_index()
        df_rs["period_fee"] = df_rs["fees"].diff().fillna(df_rs["fees"]).clip(lower=0)
        df_rs["24h_pace"] = 0.0
        first_d = df_rs.iloc[0]["date"] if not df_rs.empty else None
        for idx, row in df_rs.iterrows():
            t = row["date"]; past = df_rs[df_rs["date"] <= t - pd.Timedelta(hours=24)]
            df_rs.at[idx, "24h_pace"] = (row["fees"] - past.iloc[-1]["fees"] if not past.empty else (row["fees"] / max((t - first_d).total_seconds()/86400, 0.01)) if first_d else 0)

        df_rs["ma7"] = df_rs["period_fee"].rolling(7, min_periods=1).mean()

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=df_rs["date"], y=df_rs["period_fee"], name=f"期間手数料", marker_color=NEON_PURPLE, opacity=0.75))
        fig2.add_trace(go.Scatter(x=df_rs["date"], y=df_rs["ma7"], mode="lines", name="移動平均 (7期間)", line=dict(color=NEON_AMBER, width=1.5, dash="dash")))
        fig2.add_trace(go.Scatter(x=df_rs["date"], y=df_rs["24h_pace"], mode="lines+markers", name="24h ペース", line=dict(color=NEON_GREEN, width=2)))
        fig2.update_layout(**plotly_dark_layout(yaxis=dict(tickprefix="$", gridcolor="rgba(255,255,255,0.04)")))
        st.plotly_chart(fig2, use_container_width=True)
        if show_interp: st.caption("⚠️ 補間ON: 欠損期間は線形補完されています")

    with tab_compare:
        if "hold_val_usd" in df_history.columns and "lp_val_usd" in df_history.columns:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=pd.concat([df_history["date"], df_history["date"].iloc[::-1]]),
                y=pd.concat([df_history["lp_val_usd"], df_history["hold_val_usd"].iloc[::-1]]),
                fill="toself", fillcolor="rgba(0,176,255,0.06)",
                line=dict(color="rgba(0,0,0,0)"), hoverinfo="skip", showlegend=False,
            ))
            fig3.add_trace(go.Scatter(
                x=df_history["date"], y=df_history["lp_val_usd"],
                mode="lines+markers", name="LP ポジション価値",
                line=dict(color=NEON_GREEN, width=2.5), marker=dict(size=4),
            ))
            fig3.add_trace(go.Scatter(
                x=df_history["date"], y=df_history["hold_val_usd"],
                mode="lines", name="HOLD の場合",
                line=dict(color=NEON_BLUE, width=2, dash="dash"),
            ))
            fig3.add_hline(y=base_capital, line_color="rgba(255,255,255,0.15)", line_dash="dot",
                           annotation_text="元本", annotation_position="bottom right")

            diff_series = df_history["lp_val_usd"] - df_history["hold_val_usd"]
            fig3.add_trace(go.Bar(
                x=df_history["date"], y=diff_series, name="LP - Hold 差分",
                marker_color=[NEON_GREEN if v >= 0 else NEON_RED for v in diff_series],
                opacity=0.5, yaxis="y2",
            ))

            fig3.update_layout(**plotly_dark_layout(
                yaxis=dict(title="USD", tickprefix="$", gridcolor="rgba(255,255,255,0.04)"),
                yaxis2=dict(title="差分 ($)", overlaying="y", side="right", showgrid=False, tickprefix="$"),
                barmode="overlay",
            ))
            st.plotly_chart(fig3, use_container_width=True)

            avg_outperf = diff_series.mean()
            st.info(f"📊 平均 LP超過収益: **${avg_outperf:+.2f}** / 記録 | LP優位期間: **{(diff_series > 0).sum()}** 回 / {len(diff_series)} 回")
        else:
            st.info("hold_val_usd / lp_val_usd のデータがありません。記録を重ねると表示されます。")

    with tab_oracle:
        st.markdown("#### 🤖 ボラティリティ分析 & 最適レンジ提案")
        st.caption("過去レートの統計分布から最適な Tick レンジを自動算出します。")

        if len(df_history) >= 3:
            rates = df_history["rate"].values
            rate_std   = float(np.std(rates)) or 1.5
            rate_mean  = float(np.mean(rates))
            rate_min   = float(np.min(rates))
            rate_max   = float(np.max(rates))

            x_range = np.linspace(rate_min * 0.97, rate_max * 1.03, 300)
            gaussian = np.exp(-0.5 * ((x_range - rate_mean) / rate_std) ** 2) / (rate_std * np.sqrt(2 * np.pi))

            fig_vol = go.Figure()
            fig_vol.add_trace(go.Histogram(
                x=rates, nbinsx=min(30, len(rates)), name="レート分布", marker_color=NEON_BLUE, opacity=0.6, histnorm="probability density",
            ))
            fig_vol.add_trace(go.Scatter(x=x_range, y=gaussian, mode="lines", name="正規分布近似", line=dict(color=NEON_GREEN, width=2)))
            for sigma, color, label in [(1, NEON_GREEN, "±1σ"), (2.5, NEON_AMBER, "±2.5σ")]:
                for sign in [1, -1]:
                    fig_vol.add_vline(x=rate_mean + sign * sigma * rate_std, line_color=color, line_dash="dash", line_width=1, annotation_text=label if sign == 1 else "", annotation_font_color=color, annotation_position="top")
            fig_vol.add_vline(x=active_rate, line_color=NEON_RED, line_width=2, annotation_text="現在", annotation_font_color=NEON_RED)
            fig_vol.update_layout(**plotly_dark_layout(title="過去レート分布 & 推奨レンジ帯", xaxis=dict(title="JPYC/USDC", gridcolor="rgba(255,255,255,0.04)"), yaxis=dict(title="確率密度", gridcolor="rgba(255,255,255,0.04)")))
            st.plotly_chart(fig_vol, use_container_width=True)

            col_n, col_w = st.columns(2)
            for col, sigma, label, style, desc in [
                (col_n, 1.0,  "🟢 Narrow (±1σ)",   "info",    "資金効率最大化。ブレイクアウトに注意。"),
                (col_w, 2.5,  "🛡️ Wide (±2.5σ)",   "success", "安定運用。手数料効率はやや低下。"),
            ]:
                with col:
                    lo = max(active_rate - sigma * rate_std, 0.001)
                    hi = active_rate + sigma * rate_std
                    t_lo, t_hi = price_to_tick(lo), price_to_tick(hi)
                    getattr(st, style)(f"**{label}**\n\nMin Tick: `{t_lo}` ({lo:.3f})\n\nMax Tick: `{t_hi}` ({hi:.3f})\n\n*{desc}*")
            st.markdown("---")
        else:
            st.info("ボラティリティ分析には3件以上の記録が必要です。")

        st.markdown("#### ⚖️ リバランス ROI 計算")
        st.caption("ガス代を手数料収入で何日で回収できるか判定します。")
        col_g1, col_g2 = st.columns(2)
        with col_g1: gas_usd = st.number_input("想定ガス代 (USD)", value=1.5, step=0.1, min_value=0.0)
        with col_g2: exp_fee = st.number_input("新レンジ 想定24h手数料 ($)", value=max(fee_avg_24h, 0.01), step=0.01, min_value=0.001)

        if exp_fee > 0:
            be_days = gas_usd / exp_fee
            roi_30 = (exp_fee * 30 - gas_usd) / gas_usd * 100 if gas_usd > 0 else 0
            if be_days < 3:   st.success(f"✅ **推奨** — ガス代回収: **{be_days:.1f}日** / 30日ROI: **{roi_30:.0f}%**")
            elif be_days < 7: st.warning(f"⚠️ **要検討** — ガス代回収: **{be_days:.1f}日** / 30日ROI: **{roi_30:.0f}%**")
            else:             st.error(f"❌ **非推奨** — ガス代回収: **{be_days:.1f}日** / 30日ROI: **{roi_30:.0f}%**")

            days_range = np.arange(0, 31, 1)
            gross = exp_fee * days_range
            net   = gross - gas_usd
            fig_roi = go.Figure()
            fig_roi.add_trace(go.Scatter(x=days_range, y=gross, mode="lines", name="累積収益 (税前)", line=dict(color=NEON_BLUE, width=2)))
            fig_roi.add_trace(go.Scatter(x=days_range, y=net,   mode="lines", name="純利益 (ガス差引)", line=dict(color=NEON_GREEN, width=2), fill="tozeroy", fillcolor="rgba(0,230,118,0.06)"))
            fig_roi.add_hline(y=0, line_color="rgba(255,255,255,0.2)", line_dash="dot")
            fig_roi.add_vline(x=be_days, line_color=NEON_AMBER, line_dash="dash", annotation_text=f"BEP {be_days:.1f}日", annotation_font_color=NEON_AMBER)
            fig_roi.update_layout(**plotly_dark_layout(title="30日間 収益シミュレーション", xaxis=dict(title="経過日数", gridcolor="rgba(255,255,255,0.04)"), yaxis=dict(title="USD", tickprefix="$", gridcolor="rgba(255,255,255,0.04)")))
            st.plotly_chart(fig_roi, use_container_width=True)

    with tab_raw:
        col_sort, col_dl = st.columns([3, 2])
        with col_sort: sort_asc = st.checkbox("昇順で表示", value=False)
        with col_dl:
            csv_raw = df_history.to_csv(index=False)
            st.download_button("⬇️ 現在の履歴をDL", data=csv_raw, file_name=f"lp_history_{datetime.now(JST).strftime('%Y%m%d')}.csv", mime="text/csv", use_container_width=True)

        display_df = df_history.sort_values("date", ascending=sort_asc)
        fmt_map = {"rate": "{:.4f}", "usdc": "{:.2f}", "jpyc": "{:,.0f}", "fees": "${:.4f}", "il": "${:.4f}", "hold_val_usd": "${:.2f}", "lp_val_usd": "${:.2f}", "net_profit_usd": "${:.4f}"}
        valid_fmt = {k: v for k, v in fmt_map.items() if k in display_df.columns}
        st.dataframe(display_df.style.format(valid_fmt), use_container_width=True, height=350)
        st.caption(f"合計 {len(df_history)} 件 | 期間: {df_history['date'].min().strftime('%Y/%m/%d')} 〜 {df_history['date'].max().strftime('%Y/%m/%d')}")
