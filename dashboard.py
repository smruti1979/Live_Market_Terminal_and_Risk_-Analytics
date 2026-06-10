import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Live Market Terminal", layout="wide")
st.title("📈 Live Market Terminal & Risk Analytics")

# --- DATABASE FETCH FUNCTIONS ---
def update_engine_config(window, threshold):
    conn = sqlite3.connect('kafka_analytics.db', timeout=5.0, isolation_level=None)
    cursor = conn.cursor()
    cursor.execute('UPDATE engine_config SET window_seconds = ?, drop_threshold = ? WHERE id = 1', (window, threshold))
    conn.close()

def get_current_config():
    try:
        conn = sqlite3.connect('file:kafka_analytics.db?mode=ro&timeout=5.0', uri=True)
        df = pd.read_sql_query("SELECT window_seconds, drop_threshold FROM engine_config WHERE id = 1", conn)
        conn.close()
        if not df.empty:
            return int(df.iloc[0]['window_seconds']), float(df.iloc[0]['drop_threshold'])
        return 60, 2.0
    except Exception:
        return 60, 2.0

def fetch_market_data(ticker):
    try:
        conn = sqlite3.connect('file:kafka_analytics.db?mode=ro&timeout=5.0', uri=True)
        
        df_history = pd.read_sql_query(
            "SELECT current_price, moving_avg_60s, timestamp FROM stock_history WHERE ticker = ? ORDER BY timestamp DESC LIMIT 50", 
            conn, params=(ticker,)
        )
        df_alerts = pd.read_sql_query("SELECT ticker, old_price, new_price, drop_percentage, timestamp FROM flash_alerts ORDER BY timestamp DESC LIMIT 3", conn)
        conn.close()
        
        if not df_history.empty:
            df_history = df_history.iloc[::-1].reset_index(drop=True)
            df_history['Time'] = df_history['timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime('%H:%M:%S'))
        return df_history, df_alerts
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

# Read current variables for sync
db_window, db_threshold = get_current_config()

# --- SIDEBAR CONTROL CENTER ---
st.sidebar.title("🎛️ Engine Control Center")

if "streaming_active" not in st.session_state:
    st.session_state.streaming_active = False

col_start, col_stop = st.sidebar.columns(2)
if col_start.button("▶️ Start Engine", type="primary"):
    st.session_state.streaming_active = True
if col_stop.button("⏹️ Stop Engine", type="secondary"):
    st.session_state.streaming_active = False

st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ Parameter Tuning")

input_window = st.sidebar.number_input("⏱️ Time Window (Seconds)", min_value=10, max_value=300, value=db_window, step=5)
input_threshold = st.sidebar.number_input("🚨 Crash Threshold (%)", min_value=0.1, max_value=10.0, value=db_threshold, step=0.1)

# Only hit the DB write connection if the UI sliders differ from current DB settings
if input_window != db_window or input_threshold != db_threshold:
    update_engine_config(input_window, input_threshold)
    st.sidebar.success("Config Synced!")

# Expanded to list more fallback assets for dashboard utility
selected_ticker = st.sidebar.selectbox("🎯 Select Active Asset", ["AAPL", "MSFT", "GOOGL", "NVDA"])

# --- COHESIVE STREAMING FRAGMENT (FIXED PARAMETER HOISTING) ---
@st.fragment(run_every=1.0)
def render_live_dashboard():
    """Reads live tracking dimensions directly out of global variables for interactive sync."""
    if not st.session_state.streaming_active:
        st.info("⏸️ Engine is currently idle. Click 'Start Engine' to monitor live data streams.")
        return

    # FIX: Reading directly from live layout scope updates the queries instantly
    ticker = selected_ticker
    window = input_window
    threshold = input_threshold

    df_h, df_a = fetch_market_data(ticker)
    
    # 1. System Risk Alerts
    if not df_a.empty:
        # Filter alerts to only show for the active selected asset to keep display clean
        active_alerts = df_a[df_a['ticker'] == ticker]
        if not active_alerts.empty:
            for _, row in active_alerts.iterrows():
                alert_time = datetime.fromtimestamp(row['timestamp']).strftime('%H:%M:%S')
                st.error(f"⚠️ [{alert_time}] ANOMALY: **{row['ticker']}** dropped **{row['drop_percentage']:.2f}%**!")
        else:
            st.success(f"🟢 Nominal Mode for {ticker}. Limits configured to {threshold}%.")
    else:
        st.success(f"🟢 Nominal Operation Mode. Monitoring drops exceeding {threshold}%.")
            
    # 2. Key Figures Summary Cards
    if not df_h.empty:
        latest = df_h.iloc[-1]
        cur_p, mov_a = latest['current_price'], latest['moving_avg_60s']
        spread = cur_p - mov_a
        
        c1, c2, c3 = st.columns(3)
        c1.metric(f"💰 {ticker} Live Price", f"${cur_p:,.2f}")
        c2.metric(f"⏱️ {window}s Moving Avg", f"${mov_a:,.2f}")
        c3.metric("📊 Spread Matrix", f"${spread:,.2f}", delta=round(spread, 2))
            
        # 3. Dynamic Multi-Line Graph Frame
        st.markdown(f"### Price Execution Overlay Matrix: {ticker}")
        chart_data = df_h.set_index('Time')[['current_price', 'moving_avg_60s']]
        chart_data.columns = ['Live Price', f'{window}s Moving Average']
        
        plot_df = chart_data.reset_index()
        melt_df = plot_df.melt('Time', var_name='Metric', value_name='Price')

        import altair as alt

                # --- FIX: Clean up the x-axis text crowding and overlap layout ---
        altair_chart = alt.Chart(melt_df).mark_line().encode(
            x=alt.X(
                'Time:N', 
                sort=None,
                axis=alt.Axis(
                    labelAngle=-45,          # Rotates text to a clean, readable 45-degree angle
                    labelOverlap='parity',   # Automatically hides every other label when crowded
                    grid=True                # Adds a subtle vertical guide line grid
                )
            ),
            y=alt.Y('Price:Q', scale=alt.Scale(zero=False)),
            color=alt.Color('Metric:N', scale=alt.Scale(
                domain=['Live Price', f'{window}s Moving Average'], 
                range=["#FF4B4B", "#00C0F2"]
            ))
        ).properties(height=400)
        
        st.altair_chart(altair_chart, use_container_width=True)
    else:
        st.warning(f"Waiting for incoming data streams for {ticker} on Kafka topic 'stock_prices'...")

# Run the smooth display frame container without static argument mappings
render_live_dashboard()
