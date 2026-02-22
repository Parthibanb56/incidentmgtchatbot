import streamlit as st
from chatbot_engine import chatbot
from analytics import get_status_summary, get_monthly_trend
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import socket
import getpass
from pathlib import Path
from urllib.parse import urlparse

# --------------------------
# App & Theme Configuration
# --------------------------
st.set_page_config(page_title="Incident AI Assistant", page_icon="🤖", layout="wide")

# ------------- LOGO SETTINGS -------------
LOGO_IMAGE = "assets/company_logo.jpg"   # <-- change to your file or URL
LOGO_WIDTH_PX = 160                      # adjust size
SIDEBAR_WIDTH_PX = 220                   # optional wider sidebar

# Optional CSS: widen sidebar + minor polish
st.markdown(f"""
<style>
section[data-testid="stSidebar"] {{
    width: {SIDEBAR_WIDTH_PX}px !important;
}}
section[data-testid="stSidebar"] > div:first-child {{
    width: {SIDEBAR_WIDTH_PX}px !important;
}}
.block-container {{ padding-top: 0.9rem; padding-bottom: 1rem; }}
[data-testid="stChatMessage"] {{ margin-bottom: 0.5rem; }}
.stPlotlyChart, .stDataFrame {{ border-radius: 8px; }}
</style>
""", unsafe_allow_html=True)

def _is_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return u.scheme in ("http", "https")
    except Exception:
        return False

def add_sidebar_logo(src: str, width: int | None = 220):
    """Reliable sidebar logo renderer (local path or URL)."""
    with st.sidebar:
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if not src:
            st.info("Set LOGO_IMAGE to a valid path or URL to show your company logo.")
            return
        if _is_url(src):
            try:
                st.image(src, width=width)
            except Exception as e:
                st.warning(f"Could not load logo from URL: {e}")
        else:
            p = Path(src).expanduser()
            if p.exists():
                st.image(str(p), width=width)
            else:
                st.warning(f"Logo not found at: {p.resolve()}")

# =============== MySQL Setup (EDIT THESE) ===============
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "mcaproject"
DB_PASSWORD = "parthimcaproject"     # <-- change
DB_NAME = "insurance"           # <-- change or create
DB_TABLE = "chat_logs"

try:
    import mysql.connector as mysql
    from mysql.connector.pooling import MySQLConnectionPool
except Exception as _e:
    mysql = None
    MySQLConnectionPool = None

_db_pool = None

def get_db_pool():
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    if mysql is None:
        st.error("mysql-connector-python is not installed. Run: pip install mysql-connector-python")
        st.stop()
    _db_pool = MySQLConnectionPool(
        pool_name="chat_pool",
        pool_size=5,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )
    return _db_pool

def ensure_table():
    # Create DB if missing (if permitted)
    try:
        raw = mysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, autocommit=True)
        cur = raw.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        cur.close()
        raw.close()
    except Exception:
        pass

    pool = get_db_pool()
    con = pool.get_connection()
    cur = con.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{DB_TABLE}` (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          ts DATETIME NOT NULL,
          user_machine VARCHAR(128) NOT NULL,
          user_name VARCHAR(128) NULL,
          question TEXT NOT NULL,
          status ENUM('success','error') NOT NULL,
          details TEXT NULL,
          response_ms INT NULL,
          page VARCHAR(64) NOT NULL DEFAULT 'Chat Assistant',
          INDEX idx_ts (ts),
          INDEX idx_machine (user_machine),
          INDEX idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.close()
    con.close()

def log_chat_db(question: str, status: str, details: str = "", response_ms: int | None = None, page: str = "Chat Assistant"):
    pool = get_db_pool()
    con = pool.get_connection()
    cur = con.cursor()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_machine = socket.gethostname()
    user_name = getpass.getuser()
    sql = f"""
        INSERT INTO `{DB_TABLE}` (ts, user_machine, user_name, question, status, details, response_ms, page)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(sql, (ts, user_machine, user_name, question, status, details, response_ms, page))
    cur.close()
    con.close()

def fetch_recent_history(limit: int = 25, only_today: bool = False) -> pd.DataFrame:
    pool = get_db_pool()
    con = pool.get_connection()
    # Use pandas read_sql for convenience
    query = f"""
        SELECT id, ts, question, status, response_ms
        FROM `{DB_TABLE}`
        WHERE user_machine = %s
        { "AND DATE(ts) = CURDATE()" if only_today else "" }
        ORDER BY id DESC
        LIMIT %s
    """
    df = pd.read_sql(query, con, params=(socket.gethostname(), limit))
    con.close()
    return df

# Initialize DB/table on app start
if mysql is not None:
    try:
        ensure_table()
    except Exception as e:
        st.sidebar.warning(f"MySQL not ready: {e}")

# ----------------- Sidebar (Logo + Nav + History) -----------------
add_sidebar_logo(LOGO_IMAGE, LOGO_WIDTH_PX)

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Chat Assistant", "Dashboard"])

with st.sidebar.expander("🕘 Recent History (this machine)"):
    if mysql is None:
        st.info("Install mysql-connector-python to enable history logging.")
    else:
        try:
            hist_df = fetch_recent_history(limit=25, only_today=False)
            if hist_df.empty:
                st.caption("No history yet.")
            else:
                def _label(row):
                    t = pd.to_datetime(row["ts"]).strftime("%H:%M")
                    icon = "✅" if row["status"] == "success" else "❌"
                    q = (row["question"][:60] + "…") if len(row["question"]) > 60 else row["question"]
                    return f"{t} {icon} {q}"
                options = [ _label(r) for _, r in hist_df.iterrows() ]
                choice = st.selectbox("Revisit an item", options=options, index=None, placeholder="Select to re-ask…")
                if choice:
                    idx = options.index(choice)
                    selected_question = hist_df.iloc[idx]["question"]
                    if "messages" not in st.session_state:
                        st.session_state.messages = [{"role": "system", "content": "You are Incident AI Assistant."}]
                    st.session_state.messages.append({"role": "user", "content": selected_question, "time": datetime.now()})
                    st.session_state.trigger_reask = True
                    st.success("Loaded into chat input. Sending…")
        except Exception as e:
            st.warning(f"Could not load history: {e}")

# ================= CHAT =================
if page == "Chat Assistant":
    st.title("🤖 Incident AI Assistant")

    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "system", "content": "You are Incident AI Assistant."}]
    if "last_len" not in st.session_state:
        st.session_state.last_len = 0

    chat_box = st.container(height=520, border=True)
    with chat_box:
        for msg in st.session_state.messages:
            if msg["role"] == "system":
                continue
            with st.chat_message("user" if msg["role"] == "user" else "assistant"):
                st.markdown(msg["content"])

    auto_send = st.session_state.pop("trigger_reask", False)
    user_input = None if auto_send else st.chat_input("Ask about Incident cases…")
    if auto_send:
        user_input = st.session_state.messages[-1]["content"]

    if user_input:
        question = user_input.strip()
        st.session_state.messages.append({"role": "user", "content": question, "time": datetime.now()})

        start = time.perf_counter()
        status = "success"
        details = "ok"
        reply = ""
        try:
            with st.chat_message("assistant"):
                with st.spinner("Thinking…"):
                    reply = chatbot(question)
                    st.markdown(reply)
        except Exception as e:
            status = "error"
            details = f"{type(e).__name__}: {e}"
            with st.chat_message("assistant"):
                st.error("Sorry, something went wrong while generating the response.")
        elapsed_ms = int((time.perf_counter() - start) * 1000)

        if reply:
            st.session_state.messages.append({"role": "assistant", "content": reply, "time": datetime.now()})

        try:
            log_chat_db(question=question, status=status, details=details, response_ms=elapsed_ms, page="Chat Assistant")
        except Exception as e:
            st.sidebar.warning(f"Log failed: {e}")

        st.rerun()

# ================= DASHBOARD =================
elif page == "Dashboard":
    st.title("📊 Operations Dashboard")

    status_data = get_status_summary()
    trend_data = get_monthly_trend()

    df_status = pd.DataFrame(status_data)
    df_trend = pd.DataFrame(trend_data)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Status Distribution")
        fig1 = px.pie(
            df_status, names="ticketstatus", values="total",
            color="ticketstatus", color_discrete_sequence=px.colors.qualitative.Set2, hole=0.35
        )
        fig1.update_traces(textposition="inside", textinfo="percent+label")
        fig1.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("Monthly Trend")
        x_col = "month"
        if df_trend["month"].dtype == object:
            try:
                df_trend["_month_dt"] = pd.to_datetime(df_trend["month"])
                x_col = "_month_dt"
            except Exception:
                pass
        fig2 = px.line(df_trend, x=x_col, y="total", markers=True, color_discrete_sequence=["#4F46E5"])
        fig2.update_layout(xaxis_title="Month", yaxis_title="Total Incidents", margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Status Data")
    st.dataframe(df_status.style.format(thousands=","), use_container_width=True, hide_index=True)