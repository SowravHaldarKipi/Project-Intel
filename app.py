import streamlit as st
import json
import pandas as pd
import io
import re
from datetime import datetime
from snowflake.snowpark.context import get_active_session

try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

st.set_page_config(layout="wide", page_title="Cortex Project Intel", page_icon="📊")

# ─────────────────────────────────────────────────────────────
# THEME
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
    :root {
        --forest:#1a3a2a; --forest-deep:#112218; --forest-mid:#1f4633;
        --forest-muted:#243d2f; --green-accent:#3d8c5e; --green-bright:#4caf78;
        --green-glow:rgba(76,175,120,0.18); --green-border:rgba(76,175,120,0.25);
        --white:#ffffff; --white-90:rgba(255,255,255,0.90); --white-60:rgba(255,255,255,0.60);
        --white-06:rgba(255,255,255,0.05); --black:#080e0a; --black-soft:#0d1a11;
        --border:rgba(255,255,255,0.08); --border-green:rgba(76,175,120,0.22);
        --text-primary:#f0f5f2; --text-secondary:rgba(255,255,255,0.55); --text-muted:rgba(255,255,255,0.28);
        --radius-sm:6px; --radius-md:10px; --radius-lg:14px;
        --amber:rgba(240,165,0,1); --red:rgba(224,92,92,1);
    }
    html,body,[class*="css"],.stApp,div,p,span,label,input,textarea,select,button {
        font-family:'Plus Jakarta Sans',sans-serif !important; font-size:13px !important;
    }
    .stApp { background:var(--forest-deep) !important; color:var(--text-primary) !important; }
    .stApp::after {
        content:''; position:fixed; inset:0; pointer-events:none; z-index:0;
        background-image:linear-gradient(rgba(76,175,120,0.03) 1px,transparent 1px),
                         linear-gradient(90deg,rgba(76,175,120,0.03) 1px,transparent 1px);
        background-size:32px 32px;
    }
    .stApp > header { display:none !important; }
    #root > div:first-child { padding-top:0 !important; }
    .block-container { padding-top:0.75rem !important; padding-bottom:2rem !important; max-width:100% !important; }
    [data-testid="stSidebar"] { background:var(--black-soft) !important; border-right:1px solid var(--border) !important; }
    [data-testid="stSidebar"] > div:first-child { padding-top:1.5rem !important; }
    [data-testid="stSidebar"] label,[data-testid="stSidebar"] .stMarkdown p {
        font-size:11px !important; color:var(--text-secondary) !important;
        text-transform:uppercase !important; letter-spacing:0.08em !important; font-weight:600 !important;
    }
    [data-testid="stSidebar"] h3 {
        font-size:10px !important; font-weight:700 !important; text-transform:uppercase !important;
        letter-spacing:0.14em !important; color:var(--green-bright) !important;
        margin-bottom:0.6rem !important; padding-bottom:0.45rem !important; border-bottom:1px solid var(--border) !important;
    }
    [data-testid="stSidebar"] .stCheckbox label p,
    [data-testid="stSidebar"] .stCheckbox span,
    [data-testid="stSidebar"] .stCheckbox label {
        color: var(--text-primary) !important;
        font-size: 12px !important;
        font-weight: 500 !important;
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stSlider label,
    [data-testid="stSidebar"] .stTextInput label,
    [data-testid="stSidebar"] .stTextArea label,
    [data-testid="stSidebar"] .stFileUploader label {
        color: var(--text-secondary) !important;
    }
    .stSelectbox label, .stMultiSelect label {
        color: var(--text-primary) !important;
    }
    .stSelectbox [data-baseweb="select"] span,
    .stSelectbox [data-baseweb="select"] div {
        color: var(--text-primary) !important;
    }
    h1 { font-size:22px !important; font-weight:800 !important; color:var(--white) !important; margin:0 !important; }
    h2 { font-size:14px !important; font-weight:700 !important; color:var(--white-90) !important; margin:0 0 0.9rem 0 !important; }
    h3 { font-size:13px !important; font-weight:600 !important; color:var(--white-90) !important; margin-bottom:0.6rem !important; }
    [data-testid="stMetric"] {
        background:var(--forest-muted) !important; border-radius:var(--radius-md) !important;
        padding:1rem !important; border:1px solid var(--border) !important;
        position:relative !important; overflow:hidden !important; transition:border-color 0.2s,box-shadow 0.2s !important;
    }
    [data-testid="stMetric"]::before { content:''; position:absolute; top:0; left:0; right:0; height:2px;
        background:linear-gradient(90deg,var(--green-bright),transparent); }
    [data-testid="stMetric"]:hover { border-color:var(--border-green) !important; box-shadow:0 0 16px var(--green-glow) !important; }
    [data-testid="stMetricValue"] { font-size:18px !important; font-weight:800 !important; color:var(--white) !important; line-height:1 !important; }
    [data-testid="stMetricLabel"] { font-size:10px !important; font-weight:600 !important; color:var(--text-muted) !important; text-transform:uppercase !important; letter-spacing:0.08em !important; }
    .stButton > button {
        background:var(--green-accent) !important; color:var(--white) !important;
        font-weight:700 !important; font-size:11px !important; letter-spacing:0.07em !important;
        text-transform:uppercase !important; border:none !important; border-radius:var(--radius-sm) !important;
        padding:0.6rem 1.4rem !important; box-shadow:0 2px 12px rgba(61,140,94,0.35) !important;
        transition:all 0.18s ease !important; width:100% !important;
    }
    .stButton > button:hover { background:var(--green-bright) !important; box-shadow:0 4px 20px rgba(76,175,120,0.45) !important; transform:translateY(-1px) !important; }
    .stTabs [data-baseweb="tab-list"] {
        background:var(--forest-muted) !important; border-radius:var(--radius-md) !important;
        border:1px solid var(--border) !important; padding:0.25rem !important; gap:0.15rem !important;
    }
    .stTabs [data-baseweb="tab"] { background:transparent !important; color:var(--text-secondary) !important;
        font-size:12px !important; font-weight:600 !important; border-radius:var(--radius-sm) !important; padding:0.45rem 0.9rem !important; }
    .stTabs [aria-selected="true"] { background:var(--green-accent) !important; color:var(--white) !important; }
    .stTabs [data-baseweb="tab-panel"] {
        background:var(--forest) !important; border:1px solid var(--border) !important;
        border-radius:0 0 var(--radius-lg) var(--radius-lg) !important; padding:1.25rem !important; margin-top:-1px !important;
    }
    .streamlit-expanderHeader {
        background:var(--forest-muted) !important; border-radius:var(--radius-sm) !important;
        border:1px solid var(--border) !important; padding:0.6rem 1rem !important;
        font-weight:600 !important; font-size:12px !important; color:var(--text-secondary) !important;
    }
    .streamlit-expanderHeader:hover { border-color:var(--border-green) !important; color:var(--text-primary) !important; }
    .streamlit-expanderContent {
        background:var(--forest-muted) !important; border:1px solid var(--border) !important;
        border-top:none !important; border-radius:0 0 var(--radius-sm) var(--radius-sm) !important; padding:1rem !important;
    }
    .stAlert { background:rgba(76,175,120,0.08) !important; border:1px solid var(--border-green) !important;
               border-left:3px solid var(--green-bright) !important; border-radius:var(--radius-md) !important;
               color:var(--text-primary) !important; font-size:12px !important; }
    .stTextArea textarea,.stTextInput input {
        background:var(--forest-muted) !important; color:var(--text-primary) !important;
        border:1px solid var(--border) !important; border-radius:var(--radius-sm) !important; font-size:12px !important;
    }
    .stSelectbox > div > div,.stMultiSelect > div > div {
        background:var(--forest-muted) !important; border:1px solid var(--border) !important;
        border-radius:var(--radius-sm) !important; color:var(--text-primary) !important; font-size:12px !important;
    }
    .stSlider > div > div > div { background:var(--green-accent) !important; }
    .stDataFrame { border:1px solid var(--border) !important; border-radius:var(--radius-md) !important; font-size:12px !important; }
    hr { border:none !important; height:1px !important; background:var(--border) !important; margin:1.5rem 0 !important; }
    .top-header {
        position:sticky; top:0; z-index:999; background:rgba(17,34,24,0.92); backdrop-filter:blur(14px);
        border-bottom:1px solid rgba(76,175,120,0.15); padding:0.6rem 1.5rem;
        margin:-0.75rem -1rem 1.5rem -1rem; display:flex; align-items:center; gap:1rem;
    }
    .live-pill { display:inline-flex; align-items:center; gap:0.4rem; background:rgba(76,175,120,0.12);
                 border:1px solid rgba(76,175,120,0.4); border-radius:20px; padding:0.28rem 0.75rem;
                 font-size:10px; font-weight:700; color:#4caf78; letter-spacing:0.06em; text-transform:uppercase; }
    .live-dot { width:7px; height:7px; background:#4caf78; border-radius:50%; position:relative; flex-shrink:0; }
    .live-dot::after { content:''; position:absolute; inset:-3px; border-radius:50%;
                       border:2px solid rgba(76,175,120,0.5); animation:live-ping 1.4s ease-out infinite; }
    @keyframes live-ping { 0%{transform:scale(0.8);opacity:1;} 100%{transform:scale(2.2);opacity:0;} }
    .panel { background:var(--forest); border:1px solid var(--border); border-radius:var(--radius-lg);
             padding:1.25rem 1.4rem; margin-bottom:1.1rem; position:relative; overflow:hidden; }
    .panel::before { content:''; position:absolute; top:0; left:0; right:0; height:1px;
                     background:linear-gradient(90deg,transparent,var(--green-bright),transparent); opacity:0.4; }
    .section-label { font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:0.14em;
                     color:var(--green-bright); margin-bottom:0.35rem; opacity:0.8; }
    .sb-block { background:rgba(255,255,255,0.03); border:1px solid var(--border);
                border-radius:var(--radius-md); padding:0.85rem 1rem; margin-bottom:0.9rem; }
    .tag { display:inline-block; background:var(--white-06); border:1px solid var(--border);
           color:var(--text-secondary); font-size:10px; font-weight:600; padding:0.15rem 0.55rem;
           border-radius:4px; margin-right:0.3rem; letter-spacing:0.04em; }
    .proj-card { background:var(--forest-mid); border:1px solid var(--border); border-radius:var(--radius-md);
                 padding:0.85rem 1rem; margin-bottom:0.5rem; transition:border-color 0.2s,box-shadow 0.2s; }
    .proj-card:hover { border-color:var(--border-green); box-shadow:0 0 12px var(--green-glow); }
    .proj-card-name { font-weight:700; font-size:12px; color:var(--white-90); margin-bottom:0.2rem; }
    .proj-card-desc { font-size:11px; color:var(--text-secondary); line-height:1.45; }
    .sprint-card { background:var(--forest-mid); border:1px solid var(--border-green); border-radius:var(--radius-md);
                   padding:0.9rem 1rem; margin-bottom:0.6rem; border-left:3px solid var(--green-bright); }
    .sprint-header { font-size:12px; font-weight:700; color:var(--green-bright); margin-bottom:0.35rem; }
    .sprint-goal { font-size:11px; color:var(--white-90); font-weight:600; margin-bottom:0.4rem; }
    .sprint-task { font-size:11px; color:var(--text-secondary); padding:0.2rem 0; border-bottom:1px solid rgba(255,255,255,0.04); }
    .sprint-task:last-child { border-bottom:none; }
    .milestone-row { display:flex; gap:0.75rem; padding:0.45rem 0; border-bottom:1px solid var(--border); align-items:flex-start; }
    .milestone-week { font-size:10px; font-weight:700; color:var(--green-bright); min-width:4.5rem; padding-top:0.1rem; }
    .milestone-text { font-size:11px; color:var(--text-secondary); }
    .plan-section-title { font-size:11px; font-weight:700; color:#4caf78; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.5rem; }
    .pulse-loader { display:flex; flex-direction:column; align-items:center; justify-content:center;
                    gap:1.4rem; padding:3rem 2rem; background:var(--forest); border:1px solid var(--border);
                    border-radius:var(--radius-lg); margin-bottom:1.1rem; }
    .pulse-rings { position:relative; width:54px; height:54px; display:flex; align-items:center; justify-content:center; }
    .pulse-rings span { position:absolute; border-radius:50%; border:2px solid rgba(255,255,255,0.85); animation:pulse-expand 2s ease-out infinite; }
    .pulse-rings span:nth-child(1){width:54px;height:54px;animation-delay:0s;}
    .pulse-rings span:nth-child(2){width:38px;height:38px;animation-delay:0.35s;}
    .pulse-rings span:nth-child(3){width:22px;height:22px;animation-delay:0.7s;}
    .pulse-rings span:nth-child(4){width:10px;height:10px;background:#fff;border:none;animation:none;}
    @keyframes pulse-expand { 0%{transform:scale(0.4);opacity:1;} 100%{transform:scale(1);opacity:0;} }
    .pulse-label { font-size:11px; font-weight:600; color:rgba(255,255,255,0.5); letter-spacing:0.08em; text-transform:uppercase; text-align:center; }
    .pulse-sublabel { font-size:11px; color:rgba(255,255,255,0.28); text-align:center; margin-top:-0.8rem; }
    .pulse-steps { display:flex; flex-direction:column; gap:0.5rem; width:100%; max-width:320px; }
    .pulse-step { display:flex; align-items:center; gap:0.6rem; font-size:11px; color:rgba(255,255,255,0.35); }
    .pulse-step.active { color:rgba(255,255,255,0.9); }
    .step-dot { width:6px; height:6px; border-radius:50%; background:rgba(255,255,255,0.2); flex-shrink:0; }
    .pulse-step.active .step-dot { background:#fff; box-shadow:0 0 6px rgba(255,255,255,0.6); animation:blink 0.9s ease-in-out infinite; }
    @keyframes blink { 0%,100%{opacity:1;} 50%{opacity:0.3;} }

    /* ── FIX 1: Full Plan section labels ── */
    .plan-section-header {
        font-size: 12px !important;
        font-weight: 700 !important;
        color: #4caf78 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
        margin: 1.4rem 0 0.5rem 0 !important;
        padding-bottom: 0.4rem !important;
        border-bottom: 2px solid rgba(76,175,120,0.35) !important;
        display: block !important;
    }
    .plan-prose-line {
        color: rgba(255,255,255,0.72) !important;
        font-size: 12px !important;
        line-height: 1.8 !important;
        margin-bottom: 0.2rem !important;
        display: block !important;
    }
    .plan-bullet-line {
        color: rgba(255,255,255,0.72) !important;
        font-size: 12px !important;
        line-height: 1.7 !important;
        padding-left: 0.75rem !important;
        display: block !important;
    }
    .plan-numbered-line {
        color: rgba(255,255,255,0.72) !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        line-height: 1.7 !important;
        display: block !important;
    }
    .plan-table-wrap {
        overflow-x: auto !important;
        margin: 0.6rem 0 1.5rem 0 !important;
        border-radius: 8px !important;
        border: 1.5px solid rgba(76,175,120,0.35) !important;
        box-shadow: 0 2px 16px rgba(0,0,0,0.35) !important;
    }
    .plan-table {
        width: 100% !important;
        border-collapse: collapse !important;
        font-size: 11.5px !important;
    }
    .plan-th {
        background: #0e2018 !important;
        color: #4caf78 !important;
        padding: 0.6rem 1rem !important;
        text-align: left !important;
        font-size: 10px !important;
        font-weight: 800 !important;
        letter-spacing: 0.09em !important;
        text-transform: uppercase !important;
        border-bottom: 2px solid rgba(76,175,120,0.4) !important;
        border-right: 1px solid rgba(255,255,255,0.05) !important;
        white-space: nowrap !important;
    }
    .plan-th:last-child { border-right: none !important; }
    .plan-td {
        padding: 0.5rem 1rem !important;
        font-size: 11px !important;
        border-bottom: 1px solid rgba(255,255,255,0.05) !important;
        border-right: 1px solid rgba(255,255,255,0.04) !important;
        vertical-align: top !important;
        line-height: 1.6 !important;
        color: rgba(255,255,255,0.82) !important;
    }
    .plan-td:last-child { border-right: none !important; }
    .plan-td-even { background: #152a1e !important; }
    .plan-td-odd  { background: #112218 !important; }
    .plan-table tbody tr:hover .plan-td {
        background: rgba(76,175,120,0.07) !important;
    }

    /* ── FIX 2: Q&A Conversation History ── */
    .qa-section-label {
        font-size: 10px !important;
        font-weight: 700 !important;
        color: rgba(255,255,255,0.35) !important;
        text-transform: uppercase !important;
        letter-spacing: 0.12em !important;
        margin-bottom: 0.75rem !important;
        padding-bottom: 0.35rem !important;
        border-bottom: 1px solid rgba(255,255,255,0.06) !important;
        display: block !important;
    }
    .qa-question {
        background: rgba(76,175,120,0.1) !important;
        border: 1px solid rgba(76,175,120,0.3) !important;
        border-left: 3px solid #4caf78 !important;
        border-radius: 8px !important;
        padding: 0.7rem 1rem !important;
        margin-bottom: 0.35rem !important;
        font-size: 12px !important;
        color: #4caf78 !important;
        font-weight: 600 !important;
        display: block !important;
    }
    .qa-q-label {
        font-size: 9px !important;
        font-weight: 700 !important;
        color: rgba(76,175,120,0.6) !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
        margin-bottom: 0.25rem !important;
        display: block !important;
    }
    .qa-q-text {
        font-size: 12px !important;
        color: #f0f5f2 !important;
        font-weight: 600 !important;
        display: block !important;
    }
    .qa-answer {
        background: #1f3d2a !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        border-radius: 0 8px 8px 8px !important;
        padding: 0.85rem 1rem !important;
        margin-bottom: 1rem !important;
        font-size: 12px !important;
        color: rgba(255,255,255,0.82) !important;
        line-height: 1.75 !important;
        display: block !important;
    }
    .qa-a-label {
        font-size: 9px !important;
        font-weight: 700 !important;
        color: rgba(255,255,255,0.28) !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
        margin-bottom: 0.3rem !important;
        display: block !important;
    }

    /* ── FIX 3: Preview plan text ── */
    .plan-preview-wrap {
        background: #0d1a11 !important;
        border: 1px solid rgba(76,175,120,0.2) !important;
        border-radius: 8px !important;
        padding: 1rem 1.25rem !important;
        max-height: 320px !important;
        overflow-y: auto !important;
    }
    .plan-preview-wrap pre {
        font-family: 'JetBrains Mono', 'Courier New', monospace !important;
        font-size: 11px !important;
        color: #e0f0e6 !important;
        line-height: 1.75 !important;
        white-space: pre-wrap !important;
        word-break: break-word !important;
        margin: 0 !important;
        background: transparent !important;
    }

    /* ── FIX: CSV badge styling ── */
    .csv-badge-imported {
        display: inline-block !important;
        background: rgba(76,175,120,0.15) !important;
        border: 1px solid rgba(76,175,120,0.4) !important;
        color: #4caf78 !important;
        font-size: 9px !important;
        font-weight: 700 !important;
        padding: 0.1rem 0.45rem !important;
        border-radius: 4px !important;
        text-transform: uppercase !important;
        letter-spacing: 0.06em !important;
        margin-left: 0.4rem !important;
    }
    .csv-badge-pending {
        display: inline-block !important;
        background: rgba(240,165,0,0.1) !important;
        border: 1px solid rgba(240,165,0,0.35) !important;
        color: rgba(240,165,0,0.9) !important;
        font-size: 9px !important;
        font-weight: 700 !important;
        padding: 0.1rem 0.45rem !important;
        border-radius: 4px !important;
        text-transform: uppercase !important;
        letter-spacing: 0.06em !important;
        margin-left: 0.4rem !important;
    }

    textarea:disabled, textarea[disabled] {
        color: #e0f0e6 !important;
        -webkit-text-fill-color: #e0f0e6 !important;
        opacity: 1 !important;
        background: #0d1a11 !important;
    }
    .stTextArea textarea { color: var(--text-primary) !important; }

    .ai-output { background:var(--black-soft); border-left:3px solid var(--green-bright);
                 border:1px solid var(--border-green); border-radius:var(--radius-md);
                 padding:1.25rem 1.4rem; font-size:12px; line-height:1.85; color:var(--white-90); }
    .footer { text-align:center; margin-top:3rem; padding:1.5rem; color:var(--text-muted);
              font-size:11px; letter-spacing:0.05em; border-top:1px solid var(--border); }
    .footer span { color:var(--green-bright); }
</style>
""", unsafe_allow_html=True)

# ── Sticky Header ─────────────────────────────────────────────
st.markdown("""
<div class="top-header">
    <div style="width:32px;height:32px;background:linear-gradient(135deg,#3d8c5e,#1a3a2a);
                border-radius:8px;display:flex;align-items:center;justify-content:center;
                font-size:1rem;box-shadow:0 2px 12px rgba(76,175,120,0.3);flex-shrink:0;">📊</div>
    <div style="display:flex;flex-direction:column;">
        <div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:0.16em;color:#4caf78;line-height:1;">Snowflake Cortex</div>
        <div style="font-size:15px;font-weight:800;color:#fff;letter-spacing:-0.02em;line-height:1.2;">Cortex Project Intel</div>
    </div>
    <div style="font-size:11px;color:rgba(255,255,255,0.3);margin-top:5px;">AI-Powered Project Planning from Institutional Memory</div>
    <div style="margin-left:auto;display:flex;gap:0.5rem;align-items:center;">
        <span class="live-pill"><span class="live-dot"></span>Live</span>
        <span class="tag">Cortex AI</span>
        <span class="tag">PMO Intelligence</span>
    </div>
</div>
""", unsafe_allow_html=True)

session = get_active_session()

def cortex_call(prompt_text, label="Cortex"):
    safe = prompt_text.replace("'", "''")
    if len(safe) > 14000:
        safe = safe[:14000]
    try:
        result = session.sql("SELECT cortex_complete('" + safe + "') as r").collect()[0][0]
        return result or ""
    except Exception as e:
        err = str(e)
        if "400" in err or "token" in err.lower() or "limit" in err.lower():
            st.warning("Trimming " + label + " prompt and retrying…")
            try:
                result = session.sql(
                    "SELECT cortex_complete('" + safe[:7000] + "') as r"
                ).collect()[0][0]
                return result or ""
            except Exception as e2:
                st.error(label + " failed after retry: " + str(e2))
                return ""
        st.error(label + " error: " + str(e))
        return ""

# ── Session State ─────────────────────────────────────────────
for k, v in [("plan", None), ("plan_name", ""), ("qa_history", []),
             ("csv_projects", []), ("past_projects_df", None),
             ("wbs_structured", None), ("plan_methodology", "Agile / Scrum"),
             ("sow_requirements", None), ("past_patterns", None),
             ("csv_imported", False),
             # FIX: track imported CSV names so dropdown label is accurate
             ("imported_csv_names", set())]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Helpers ───────────────────────────────────────────────────
def safe_str(val):
    return '' if pd.isna(val) else str(val)

def parse_date_safe(date_str):
    if not date_str:
        return ''
    try:
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(dt):
            return dt.strftime('%Y-%m-%d')
    except Exception:
        pass
    return ''

def load_past_projects():
    try:
        # Load all projects (Completed + any status) so uploaded CSVs and saved plans always appear
        df = session.sql("SELECT * FROM projects").to_pandas()
        if df.empty:
            return pd.DataFrame(columns=['project_id','project_name','description',
                                         'wbs_summary','risk_log_summary','test_cases_summary','deployment_plan'])
        id_candidates = ['project_id','id','projectid','PROJECT_ID','PROJECTID']
        found_id = next((c for c in id_candidates if c in df.columns), None)
        if found_id:
            df.rename(columns={found_id: 'project_id'}, inplace=True)
        else:
            df['project_id'] = df.index + 1
        for col in ['project_name','description','wbs_summary','risk_log_summary','test_cases_summary','deployment_plan']:
            if col not in df.columns:
                df[col] = ''
        df = df.drop_duplicates(subset=['project_name'])
        return df[['project_id','project_name','description','wbs_summary',
                   'risk_log_summary','test_cases_summary','deployment_plan']]
    except Exception as e:
        st.error(f"Error loading projects: {e}")
        return pd.DataFrame(columns=['project_id','project_name','description',
                                     'wbs_summary','risk_log_summary','test_cases_summary','deployment_plan'])

def parse_csv_as_project(uploaded_file):
    try:
        df = pd.read_csv(uploaded_file)
        df.columns = df.columns.str.strip()
        col_map = {}
        for col in df.columns:
            cl = col.lower()
            if 'task' in cl and 'sub' not in cl: col_map['tasks'] = col
            elif 'subtask' in cl or 'sub task' in cl: col_map['subtasks'] = col
            elif 'start date' in cl: col_map['start_date'] = col
            elif 'end date' in cl: col_map['end_date'] = col
            elif 'project name' in cl: col_map['project_name'] = col

        project_name = safe_str(df[col_map['project_name']].iloc[0]) if 'project_name' in col_map \
                       else uploaded_file.name.replace('.csv','').replace('_',' ').title()

        wbs_lines = []
        for _, row in df.iterrows():
            task    = safe_str(row.get(col_map.get('tasks',''), ''))
            subtask = safe_str(row.get(col_map.get('subtasks',''), ''))
            start   = safe_str(row.get(col_map.get('start_date',''), ''))
            end     = safe_str(row.get(col_map.get('end_date',''), ''))
            if task or subtask:
                line = f"- {task}"
                if subtask: line += f" → {subtask}"
                if start or end: line += f" (Start: {start}, End: {end})"
                wbs_lines.append(line)

        start_date, end_date = '', ''
        if 'start_date' in col_map:
            for val in df[col_map['start_date']]:
                if pd.notna(val) and val:
                    start_date = parse_date_safe(safe_str(val)); break
        if 'end_date' in col_map:
            for val in df[col_map['end_date']]:
                if pd.notna(val) and val:
                    end_date = parse_date_safe(safe_str(val)); break

        return {"project_name": project_name,
                "description": f"Imported from {uploaded_file.name}. {len(df)} rows.",
                "wbs_summary": "\n".join(wbs_lines) or "No tasks found.",
                "start_date": start_date, "end_date": end_date, "technologies_used": ''}
    except Exception as e:
        st.error(f"Error parsing CSV: {e}")
        return None

def parse_wbs_into_sprints(plan_text, duration_weeks, methodology):
    is_agile = any(k in methodology.lower() for k in ['agile','scrum','safe'])
    sprint_len = 2
    num_sprints = max(1, duration_weeks // sprint_len)

    prompt = (
        f"Extract the WBS from this project plan as structured JSON.\n\n"
        f"Plan (excerpt):\n{plan_text[:3500]}\n\n"
        f"Methodology: {methodology} | Duration: {duration_weeks} weeks\n"
        f"{'Divide into ' + str(num_sprints) + ' Sprints of 2 weeks each.' if is_agile else 'Divide into phases.'}\n\n"
        f"Return ONLY a valid JSON array, no markdown:\n"
        f'[{{"sprint_num":1,"name":"Sprint 1: Title","weeks":"Week 1-2",'
        f'"goal":"One sentence goal.","stories":["task1","task2","task3"],'
        f'"deliverable":"What is delivered"}}]'
    )
    try:
        safe = prompt.replace("'", "''")
        raw  = session.sql(f"SELECT cortex_complete('{safe}') as r").collect()[0][0]
        raw  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(raw)
        if isinstance(parsed, list) and parsed:
            return parsed
    except Exception:
        pass
    sprints = []
    for i in range(1, num_sprints + 1):
        w_start = (i-1)*sprint_len + 1
        w_end   = i*sprint_len
        label   = f"Sprint {i}" if is_agile else f"Phase {i}"
        sprints.append({
            "sprint_num": i,
            "name": f"{label}: {'Setup & Planning' if i==1 else 'Build & Develop' if i<=num_sprints//2+1 else 'Test & Deploy'}",
            "weeks": f"Week {w_start}–{w_end}",
            "goal": f"Deliver iteration {i} of the project.",
            "stories": ["Define requirements", "Build core components", "Review and test"],
            "deliverable": f"Iteration {i} complete"
        })
    return sprints


def build_excel(plan_text, plan_name, wbs_sprints, methodology):
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = openpyxl.Workbook()

    DK  = "0e1c12"; MG  = "163020"; AG  = "2d7a50"; BG  = "4caf78"
    LG  = "e8f5ee"; LG2 = "f2faf5"; WH  = "FFFFFF"; GR  = "f5f8f5"
    TK  = "132018"; TM  = "2d4a36"; AM  = "c07a00"; RD  = "c0392b"
    BL  = "2d5fa8"; PU  = "6a3a9f"; BD  = "c8ddd0"

    def F(color, bold=False, size=10):
        return Font(name="Calibri", color=color, bold=bold, size=size)
    def BG_fill(color):
        return PatternFill("solid", fgColor=color)
    def AL(h="left", v="center", wrap=False):
        return Alignment(horizontal=h, vertical=v, wrap_text=wrap)
    def border():
        s = Side(style="thin", color=BD)
        return Border(left=s, right=s, top=s, bottom=s)
    def thick_bottom():
        s = Side(style="thin", color=BD)
        t = Side(style="medium", color=AG)
        return Border(left=s, right=s, top=s, bottom=t)

    def safe_title(t):
        import re
        c = re.sub(r'[:/\\?*\[\]]', '', t)
        c = c.encode('ascii', 'ignore').decode('ascii')
        return c.strip()[:31] or "Sheet"

    def xval(v):
        if v is None:
            return ""
        if isinstance(v, (str, int, float, bool)):
            return v
        if isinstance(v, dict):
            return v.get("name", v.get("task", v.get("story", str(v))))
        if isinstance(v, list):
            return "; ".join(xval(x) for x in v)
        return str(v)

    def write_col_headers(ws, cols, row=2, bg=AG):
        for c, (label, width) in enumerate(cols, 1):
            cell = ws.cell(row=row, column=c, value=label)
            cell.fill      = BG_fill(bg)
            cell.font      = F(WH, bold=True, size=10)
            cell.alignment = AL("center", "center")
            cell.border    = thick_bottom()
            ws.column_dimensions[get_column_letter(c)].width = width
        ws.row_dimensions[row].height = 22

    def title_row(ws, text, cols_count, bg=DK, font_size=14):
        ws.merge_cells(f"A1:{get_column_letter(cols_count)}1")
        c = ws["A1"]
        c.value      = text
        c.fill       = BG_fill(bg)
        c.font       = F(WH, bold=True, size=font_size)
        c.alignment  = AL("center", "center")
        c.border     = border()
        ws.row_dimensions[1].height = 40

    is_agile = any(k in methodology.lower() for k in ['agile','scrum','safe'])

    # Sheet 1 — Project Summary
    ws1 = wb.active
    ws1.title = safe_title("Project Summary")
    ws1.sheet_view.showGridLines = False
    ws1.sheet_view.zoomScale = 95

    ws1.merge_cells("A1:F3")
    h = ws1["A1"]
    h.value = "PROJECT PLAN"
    h.fill = BG_fill(DK); h.font = F(WH, bold=True, size=22)
    h.alignment = AL("left", "center")
    ws1.row_dimensions[1].height = 20
    ws1.row_dimensions[2].height = 36
    ws1.row_dimensions[3].height = 20

    ws1.merge_cells("A4:F4")
    s = ws1["A4"]
    s.value = f"{plan_name}"
    s.fill = BG_fill(MG); s.font = F(BG, bold=True, size=16)
    s.alignment = AL("left", "center")
    ws1.row_dimensions[4].height = 30

    ws1.merge_cells("A5:F5")
    m = ws1["A5"]
    m.value = (f"Cortex Project Intel  ·  {datetime.now().strftime('%d %B %Y')}"
               f"  ·  Methodology: {methodology}")
    m.fill = BG_fill(MG); m.font = F("a8d8b8", size=9)
    m.alignment = AL("left", "center")
    ws1.row_dimensions[5].height = 18
    ws1.row_dimensions[6].height = 10

    info = [
        ("Project Name",   plan_name),
        ("Methodology",    methodology),
        ("Generated",      datetime.now().strftime('%d %B %Y')),
        ("Sprints / Phases", str(len(wbs_sprints))),
        ("Source",         "Cortex Project Intel — Snowflake AI"),
    ]
    for i, (lbl, val) in enumerate(info, 7):
        lc = ws1.cell(row=i, column=1, value=lbl)
        vc = ws1.cell(row=i, column=2, value=val)
        lc.fill = BG_fill(AG); lc.font = F(WH, bold=True, size=10)
        lc.alignment = AL("right", "center"); lc.border = border()
        vc.fill = BG_fill(LG); vc.font = F(TK, size=10)
        vc.alignment = AL("left", "center"); vc.border = border()
        ws1.row_dimensions[i].height = 20

    ws1.row_dimensions[12].height = 12

    ws1.merge_cells("A13:F13")
    ph = ws1["A13"]
    ph.value = "FULL PLAN TEXT"
    ph.fill = BG_fill(AG); ph.font = F(WH, bold=True, size=10)
    ph.alignment = AL("center", "center"); ph.border = border()
    ws1.row_dimensions[13].height = 18

    ws1.merge_cells("A14:F80")
    pc = ws1["A14"]
    pc.value = plan_text
    pc.font = F(TK, size=9); pc.alignment = AL("left", "top", wrap=True)
    pc.fill = BG_fill(GR)
    for r in range(14, 81):
        ws1.row_dimensions[r].height = 13

    ws1.column_dimensions["A"].width = 22
    ws1.column_dimensions["B"].width = 55
    for col in ["C","D","E","F"]:
        ws1.column_dimensions[col].width = 14

    # Sheet 2 — WBS Sprint Board
    ws2 = wb.create_sheet(safe_title("WBS Sprint Board"))
    ws2.sheet_view.showGridLines = False
    ws2.freeze_panes = "A3"

    title_row(ws2, ("SPRINT BOARD" if is_agile else "WORK BREAKDOWN STRUCTURE")
                   + f"  —  {plan_name}", 10)

    WBS_COLS = [
        ("Sprint / Phase", 22), ("Weeks", 11), ("Sprint Goal", 36),
        ("Task / User Story", 44), ("Type", 14),
        ("Story Points", 12), ("Severity", 12),
        ("Assignee", 18), ("Deliverable", 32), ("Status", 12),
    ]
    write_col_headers(ws2, WBS_COLS)

    sp_pts   = [8, 8, 5, 5, 3, 3, 2]
    sev_list = ["High","High","Medium","Medium","Low","Medium","Low"]
    sev_col  = {"High": RD, "Medium": AM, "Low": AG}
    sev_bg_  = {"High":"fdeaea","Medium":"fff8e6","Low":"e8f5ee"}

    row = 3
    for s_idx, sprint in enumerate(wbs_sprints):
        sprint_name  = xval(sprint.get("name",""))
        weeks_lbl    = xval(sprint.get("weeks",""))
        goal         = xval(sprint.get("goal",""))
        deliverable  = xval(sprint.get("deliverable",""))
        raw_tasks    = sprint.get("stories", sprint.get("tasks", []))
        tasks = []
        for t in (raw_tasks if isinstance(raw_tasks, list) else []):
            sv = xval(t)
            if sv and str(sv).strip():
                tasks.append(str(sv).strip())
        if not tasks:
            tasks = ["Define requirements and acceptance criteria",
                     "Build and integrate core components",
                     "Test, review and sign off"]

        hdr_bg = DK if s_idx % 2 == 0 else MG

        ws2.merge_cells(f"A{row}:J{row}")
        banner = ws2.cell(row=row, column=1,
                          value=f"  {sprint_name}   |   {weeks_lbl}   |   {goal}")
        banner.fill = BG_fill(hdr_bg); banner.font = F(BG, bold=True, size=11)
        banner.alignment = AL("left", "center"); banner.border = border()
        ws2.row_dimensions[row].height = 26
        row += 1

        for t_idx, task in enumerate(tasks):
            row_bg = LG if t_idx % 2 == 0 else LG2
            pts    = sp_pts[min(t_idx, len(sp_pts)-1)]
            sev    = sev_list[min(t_idx, len(sev_list)-1)]
            sc     = sev_col[sev]; sb = sev_bg_[sev]
            t_type = "User Story" if is_agile else "Task"
            deliv  = xval(deliverable) if t_idx == 0 else ""

            row_vals = [xval(sprint_name), xval(weeks_lbl), xval(goal),
                        xval(task), t_type, pts, sev, "TBD", deliv, "To Do"]
            for c, val in enumerate(row_vals, 1):
                cell = ws2.cell(row=row, column=c, value=val)
                cell.border = border()
                if c == 6:
                    cell.fill = BG_fill("e8f0ff"); cell.font = F(BL, bold=True)
                    cell.alignment = AL("center","center")
                elif c == 7:
                    cell.fill = BG_fill(sb); cell.font = F(sc, bold=True)
                    cell.alignment = AL("center","center")
                elif c == 10:
                    cell.fill = BG_fill("fff8e6"); cell.font = F(AM, bold=True, size=9)
                    cell.alignment = AL("center","center")
                elif c == 9 and t_idx == 0:
                    cell.fill = BG_fill(row_bg); cell.font = F(AG, bold=True)
                    cell.alignment = AL("left","center", wrap=True)
                else:
                    cell.fill = BG_fill(row_bg)
                    cell.font = F(TK, bold=(c in [1,2] and t_idx==0))
                    cell.alignment = AL("left","center", wrap=True)
            ws2.row_dimensions[row].height = 19
            row += 1

        for c in range(1,11):
            g = ws2.cell(row=row, column=c, value="")
            g.fill = BG_fill("ddeee5"); g.border = border()
        ws2.row_dimensions[row].height = 5
        row += 1

    # Sheet 3 — RAID Log
    import re as _re
    ws3 = wb.create_sheet(safe_title("RAID Log"))
    ws3.sheet_view.showGridLines = False
    ws3.freeze_panes = "A3"

    RAID_COLS = [("ID",5), ("Category",15), ("Type",16),
                 ("Description",46), ("Likelihood",13), ("Impact",12),
                 ("Mitigation",44), ("Owner",20)]
    title_row(ws3, f"RAID LOG  —  {plan_name}", len(RAID_COLS))
    write_col_headers(ws3, RAID_COLS)

    lh_c = {"High":RD,"Medium":AM,"Low":AG}
    lh_b = {"High":"fdeaea","Medium":"fff8e6","Low":"e8f5ee"}
    cat_bg_ = {"Risk":"fdeaea","Assumption":"e8f0ff","Issue":"fff8e6","Dependency":"f0e8ff"}
    cat_fg_ = {"Risk":RD,"Assumption":BL,"Issue":AM,"Dependency":PU}

    raid_data = []
    sec = _re.search(r'RAID\s*LOG.*?(?=\n#{1,3}\s|\n\d+\.\s+[A-Z]|\Z)',
                     plan_text, _re.DOTALL|_re.IGNORECASE)
    if sec:
        skip = {'','ID','TYPE','Type','DESCRIPTION','Description','LIKELIHOOD','Likelihood',
                'IMPACT','Impact','MITIGATION','Mitigation','OWNER','Owner','Category','---'}
        for line in sec.group(0).split('\n'):
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4 and not all(p in skip for p in parts):
                raid_data.append(parts)

    if not raid_data:
        raid_data = [
            ["","Risk","Technical","Data access permissions not provisioned","High","High",
             "Pre-provision all Snowflake roles before Sprint 1","Platform Engineer"],
            ["","Risk","Timeline","Scope creep from stakeholder change requests","Medium","High",
             "Freeze scope after Sprint 2; route changes via CR process","Project Manager"],
            ["","Risk","Resource","Key person dependency on lead architect","Medium","Medium",
             "Cross-train 2 team members on architecture decisions","Tech Lead"],
            ["","Risk","Budget","Compute cost overrun on large warehouse queries","Low","High",
             "Set resource monitor alerts at 80% of credit budget","FinOps"],
            ["","Assumption","Process","Team availability maintained at 80% throughout","—","—",
             "Confirm resource allocation with line managers before start","PMO"],
            ["","Assumption","Technical","Source APIs remain stable during integration","—","—",
             "Document API versions; add version-check step","Data Engineer"],
            ["","Dependency","External","InfoSec approval for external API access","High","High",
             "Raise InfoSec request Week 1; track weekly","Project Manager"],
            ["","Dependency","Data","Source data quality meets >90% DQ threshold","Medium","High",
             "Run DQ profiling Sprint 1; escalate if score <90%","Data Engineer"],
            ["","Issue","Process","Legacy data quality issues in source tables","—","Medium",
             "Run cleansing pipeline in Sprint 1; document exceptions","Data Engineer"],
            ["","Issue","Technical","Snowflake credit limitations on trial account","Low","Low",
             "Monitor credit usage daily; upgrade account if needed","Platform Engineer"],
        ]

    for i, r in enumerate(raid_data[:25]):
        while len(r) < 8: r.append("")
        rn  = i + 3
        cat = xval(r[1]).strip().title()
        lh  = xval(r[4]).strip().title()
        im  = xval(r[5]).strip().title()
        rbg = LG if i % 2 == 0 else LG2

        for c, val in enumerate(r[:8], 1):
            cell = ws3.cell(row=rn, column=c, value=xval(val))
            cell.border = border()
            if c == 1:
                cell.value = i+1; cell.fill = BG_fill(AG)
                cell.font = F(WH, bold=True); cell.alignment = AL("center","center")
            elif c == 2:
                cell.fill = BG_fill(cat_bg_.get(cat,"f5f8f5"))
                cell.font = F(cat_fg_.get(cat,TK), bold=True)
                cell.alignment = AL("center","center")
            elif c == 3:
                cell.fill = BG_fill("f0f0ff"); cell.font = F(BL)
                cell.alignment = AL("center","center")
            elif c == 5:
                bg = lh_b.get(lh,"f5f8f5") if lh not in ("—","") else GR
                fg = lh_c.get(lh, TK)
                cell.fill = BG_fill(bg); cell.font = F(fg, bold=True)
                cell.alignment = AL("center","center")
            elif c == 6:
                bg = lh_b.get(im,"f5f8f5") if im not in ("—","") else GR
                fg = lh_c.get(im, TK)
                cell.fill = BG_fill(bg); cell.font = F(fg, bold=True)
                cell.alignment = AL("center","center")
            else:
                cell.fill = BG_fill(rbg); cell.font = F(TK)
                cell.alignment = AL("left","center", wrap=True)
        ws3.row_dimensions[rn].height = 22

    # Sheet 4 — Test Cases
    ws4 = wb.create_sheet(safe_title("Test Cases"))
    ws4.sheet_view.showGridLines = False
    ws4.freeze_panes = "A3"

    TC_COLS = [("ID",8),("Priority",12),("Test Type",16),
               ("Scenario",34),("Preconditions",28),
               ("Test Steps",46),("Expected Result",36),
               ("Actual Result",36),("Status",13)]
    title_row(ws4, f"TEST CASES  —  {plan_name}", len(TC_COLS))
    write_col_headers(ws4, TC_COLS)

    pri_c = {"High":RD,"Medium":AM,"Low":AG}
    pri_b = {"High":"fdeaea","Medium":"fff8e6","Low":"e8f5ee"}
    typ_b = {"Integration":"e8f0ff","Data Quality":"e8f5ee","Security":"fdeaea",
             "Performance":"fff8e6","Functional":"f0e8ff","Regression":"f5f5f5","UAT":"e8f5ee"}
    typ_f = {"Integration":BL,"Data Quality":AG,"Security":RD,
             "Performance":AM,"Functional":PU,"Regression":"555555","UAT":AG}

    tc_data = []
    tcs = _re.search(r'TEST\s*CASES.*?(?=\n#{1,3}\s|\n\d+\.\s+[A-Z]|\Z)',
                     plan_text, _re.DOTALL|_re.IGNORECASE)
    if tcs:
        skip2 = {'','ID','Scenario','Steps','Expected Result','Pass Criteria',
                 'SCENARIO','STEPS','EXPECTED','Priority','Test Type','---'}
        for line in tcs.group(0).split('\n'):
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4 and not all(p in skip2 for p in parts):
                tc_data.append(parts)

    if not tc_data:
        tc_data = [
            ["TC-01","High","Integration","End-to-end pipeline ingestion",
             "Source data available; pipeline deployed",
             "1. Stage source CSV  2. Execute pipeline  3. Query target table",
             "Row count matches source; no nulls in key columns","","Not Started"],
            ["TC-02","High","Data Quality","Schema and column type validation",
             "Target table created with expected schema",
             "1. Run schema check SQL  2. Validate types  3. Check PK uniqueness",
             "Zero schema violations; zero duplicate PKs","","Not Started"],
            ["TC-03","High","Security","Role-based access control",
             "VIEWER and EDITOR roles provisioned",
             "1. Login as VIEWER  2. Attempt INSERT  3. Verify permission error",
             "INSERT denied; SELECT succeeds for VIEWER","","Not Started"],
            ["TC-04","High","Functional","Cortex AI response validation",
             "CORTEX_COMPLETE enabled on warehouse",
             "1. Call CORTEX_COMPLETE  2. Parse JSON  3. Validate schema",
             "Valid JSON; all fields present; latency < 5s","","Not Started"],
            ["TC-05","Medium","Performance","Dashboard load time",
             "App deployed; tables populated",
             "1. Open app URL  2. Load main dashboard  3. Measure render time",
             "Dashboard interactive within 3 seconds","","Not Started"],
            ["TC-06","Medium","Data Quality","DQ rule execution",
             "DQ rule set configured",
             "1. Execute DQ rules  2. Check DQ score  3. Review error log",
             "DQ score >= 95%; flagged rows in error log","","Not Started"],
            ["TC-07","Medium","Regression","Pipeline idempotency",
             "Pipeline run successfully at least once",
             "1. Re-trigger pipeline  2. Query row count  3. Compare pre/post",
             "Row count unchanged; zero duplicates","","Not Started"],
            ["TC-08","Low","UAT","Business stakeholder sign-off",
             "All TC-01 through TC-07 passed",
             "1. Demo to business owner  2. Walk through KPIs  3. Capture sign-off",
             "Business owner approves; sign-off document saved","","Not Started"],
        ]

    for i, rd in enumerate(tc_data[:20]):
        while len(rd) < 9: rd.append("")
        rn   = i + 3
        rbg  = LG if i % 2 == 0 else LG2
        pri  = xval(rd[1]).strip()
        tt   = xval(rd[2]).strip()
        for c, val in enumerate(rd[:9], 1):
            cell = ws4.cell(row=rn, column=c, value=xval(val))
            cell.border = border()
            if c == 1:
                cell.fill = BG_fill(AG); cell.font = F(WH, bold=True)
                cell.alignment = AL("center","center")
            elif c == 2:
                cell.fill = BG_fill(pri_b.get(pri,"fff8e6"))
                cell.font = F(pri_c.get(pri,AM), bold=True)
                cell.alignment = AL("center","center")
            elif c == 3:
                cell.fill = BG_fill(typ_b.get(tt,"f5f8f5"))
                cell.font = F(typ_f.get(tt,TK), bold=True)
                cell.alignment = AL("center","center")
            elif c == 8:
                cell.fill = BG_fill("fafcfb"); cell.font = F("bbbbbb", size=9)
                cell.alignment = AL("left","top", wrap=True)
                if not cell.value: cell.value = "— fill after execution —"
            elif c == 9:
                cell.fill = BG_fill("fff8e6"); cell.font = F(AM, bold=True)
                cell.alignment = AL("center","center")
            else:
                cell.fill = BG_fill(rbg); cell.font = F(TK)
                cell.alignment = AL("left","top", wrap=True)
        ws4.row_dimensions[rn].height = 48

    # Sheet 5 — Delivery Timeline
    ws5 = wb.create_sheet(safe_title("Delivery Timeline"))
    ws5.sheet_view.showGridLines = False
    ws5.freeze_panes = "A3"

    TL_COLS = [("Week / Period",15),("Sprint / Phase",26),("Milestone",50),
               ("Tasks",44),("Owner",20),("Status",14),("Notes",28)]
    title_row(ws5, f"DELIVERY TIMELINE  —  {plan_name}", len(TL_COLS))
    write_col_headers(ws5, TL_COLS)

    st_col = {"Planned":"fff8e6","In Progress":"e8f0ff","Done":"e8f5ee","To Do":"f5f5f5"}
    st_fg  = {"Planned":AM,"In Progress":BL,"Done":AG,"To Do":"888888"}

    row = 3
    for s_idx, sprint in enumerate(wbs_sprints):
        sprint_name = xval(sprint.get("name",""))
        weeks_lbl   = xval(sprint.get("weeks",""))
        deliverable = xval(sprint.get("deliverable",""))
        goal        = xval(sprint.get("goal",""))
        raw_tasks   = sprint.get("stories", sprint.get("tasks", []))
        tasks = [str(xval(t)).strip() for t in (raw_tasks if isinstance(raw_tasks, list) else [])
                 if str(xval(t)).strip()]
        if not tasks:
            tasks = ["Define and build sprint deliverable", "Test and review output"]

        hdr_bg = DK if s_idx % 2 == 0 else MG
        rbg    = LG if s_idx % 2 == 0 else LG2

        hdr = [weeks_lbl, sprint_name, f"Deliverable: {deliverable}", goal, "Project Team","Planned",""]
        for c, val in enumerate(hdr, 1):
            cell = ws5.cell(row=row, column=c, value=xval(val))
            cell.border = border()
            if c <= 2:
                cell.fill = BG_fill(hdr_bg); cell.font = F(WH, bold=True, size=11)
                cell.alignment = AL("left","center")
            elif c == 3:
                cell.fill = BG_fill(rbg); cell.font = F(AG, bold=True)
                cell.alignment = AL("left","center", wrap=True)
            elif c == 6:
                cell.fill = BG_fill("fff8e6"); cell.font = F(AM, bold=True)
                cell.alignment = AL("center","center")
            else:
                cell.fill = BG_fill(rbg); cell.font = F(TK)
                cell.alignment = AL("left","center", wrap=True)
        ws5.row_dimensions[row].height = 26
        row += 1

        for t_idx, task in enumerate(tasks):
            tbg = "f7faf8" if s_idx%2==0 else "f2f7f3"
            tv = ["","", f"  • {task}","","TBD","To Do",""]
            for c, val in enumerate(tv, 1):
                cell = ws5.cell(row=row, column=c, value=xval(val))
                cell.border = border()
                if c == 3:
                    cell.fill = BG_fill(tbg); cell.font = F(TM, size=10)
                    cell.alignment = AL("left","center", wrap=True)
                elif c == 6:
                    status = "To Do"
                    cell.fill = BG_fill(st_col.get(status,"f5f5f5"))
                    cell.font = F(st_fg.get(status,"888888"), bold=True, size=9)
                    cell.alignment = AL("center","center")
                else:
                    cell.fill = BG_fill(tbg); cell.font = F(TK, size=9)
                    cell.alignment = AL("left","center")
            ws5.row_dimensions[row].height = 17
            row += 1

        for c in range(1,8):
            g = ws5.cell(row=row, column=c, value="")
            g.fill = BG_fill("ddeee5"); g.border = border()
        ws5.row_dimensions[row].height = 4
        row += 1

    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()


# ── Load past projects ─────────────────────────────────────────
if st.session_state.past_projects_df is None:
    st.session_state.past_projects_df = load_past_projects()
past_projects_df = st.session_state.past_projects_df
db_names_set = set(past_projects_df['project_name'].tolist())

# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""<div style="margin-bottom:1rem;">
        <div style="font-size:13px;font-weight:800;color:#f0f5f2;">Project Configuration</div>
        <div style="font-size:10px;color:rgba(255,255,255,0.28);margin-top:1px;">Define your project — Cortex selects the best templates automatically.</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<div class='sb-block'>", unsafe_allow_html=True)
    st.markdown("### 📄 Statement of Work (SOW)")
    sow_pdf  = st.file_uploader("Upload SOW PDF (mandatory)", type=["pdf"])
    sow_text = ""
    if sow_pdf is not None:
        if PDF_AVAILABLE:
            try:
                reader   = PyPDF2.PdfReader(sow_pdf)
                sow_text = "\n".join([p.extract_text() or "" for p in reader.pages])
                st.success(f"✅ SOW loaded ({len(sow_text):,} chars)")
            except Exception as e:
                st.error(f"PDF read error: {e}")
        else:
            st.warning("⚠️ PyPDF2 not installed — PDF extraction disabled.")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='sb-block'>", unsafe_allow_html=True)
    st.markdown("### 📂 Project Plans (CSV) — Multiple Allowed")
    st.markdown(
        "<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:0.5rem;'>"
        "Upload one or more CSV project plans. All will appear in the browse dropdown.</div>",
        unsafe_allow_html=True
    )
    plans_csvs = st.file_uploader(
        "Upload CSVs (hold Ctrl/Cmd to select multiple)",
        type=["csv"],
        accept_multiple_files=True
    )

    existing_names = {p["project_name"] for p in st.session_state.csv_projects}

    if plans_csvs:
        newly_added = 0
        for f in plans_csvs:
            proj = parse_csv_as_project(f)
            if proj and proj["project_name"] not in existing_names:
                st.session_state.csv_projects.append(proj)
                existing_names.add(proj["project_name"])
                newly_added += 1
        if newly_added:
            st.success(f"✅ {newly_added} new plan(s) added — {len(st.session_state.csv_projects)} total loaded.")
        else:
            st.info(f"{len(st.session_state.csv_projects)} plan(s) already loaded.")

    if st.session_state.csv_projects:
        st.markdown(
            f"<div style='font-size:11px;color:#4caf78;font-weight:600;margin:.35rem 0;'>"
            f"📎 {len(st.session_state.csv_projects)} plan(s) in memory</div>",
            unsafe_allow_html=True
        )
        for idx_p, proj in enumerate(st.session_state.csv_projects):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                nm = proj['project_name']
                already = nm in db_names_set or nm in st.session_state.imported_csv_names
                badge = "✅" if already else "⏳"
                st.markdown(
                    f"<div style='font-size:11px;color:rgba(255,255,255,0.7);padding:.2rem 0;'>"
                    f"📄 {proj['project_name']} <span style='color:{'#4caf78' if already else '#f0a500'};font-size:9px;'>{badge}</span></div>",
                    unsafe_allow_html=True
                )
            with col_b:
                btn_label = "Re-save" if already else "Import"
                if st.button(btn_label, key=f"import_csv_{idx_p}"):
                    try:
                        pn  = proj["project_name"].replace("'","''")
                        pd_ = proj["description"][:500].replace("'","''")
                        wb_ = proj["wbs_summary"][:3000].replace("'","''")
                        sd  = f"'{proj['start_date']}'" if proj["start_date"] else "NULL"
                        ed  = f"'{proj['end_date']}'"   if proj["end_date"]   else "NULL"
                        # DELETE existing row first so re-import works cleanly (UPSERT pattern)
                        try:
                            session.sql(
                                f"DELETE FROM projects WHERE project_name = '{pn}' AND description LIKE '%CSV%'"
                            ).collect()
                        except Exception:
                            pass
                        session.sql(
                            "INSERT INTO projects "
                            "(project_name,description,start_date,end_date,status,lead_architect,"
                            "technologies_used,wbs_summary,risk_log_summary,test_cases_summary,deployment_plan) "
                            f"VALUES('{pn}','{pd_}',{sd},{ed},'Completed','','','{wb_}','','','')"
                        ).collect()
                        st.session_state.imported_csv_names.add(proj["project_name"])
                        st.session_state.past_projects_df = load_past_projects()
                        st.session_state.csv_imported = True
                        st.success(f"✅ '{proj['project_name']}' saved to DB permanently.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Import failed: {e}")

        if st.button("🗑️ Clear All CSV Plans", key="clear_csvs"):
            st.session_state.csv_projects = []
            st.session_state.imported_csv_names = set()
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='sb-block'>", unsafe_allow_html=True)
    st.markdown("### 🆕 New Project Details")
    new_project_name = st.text_input("Project Name", "Data Cataloging Initiative")
    new_project_desc = st.text_area("Description",
        "A new data cataloging initiative to index all data assets in Snowflake.",
        height=90)
    new_project_tech = st.text_input("Key Technologies", "Snowflake, Streamlit, Python")
    customer_name    = st.text_input("Customer / Organisation", "Acme Corp")
    project_type     = st.selectbox("Project Type",
        ["Data Engineering", "Analytics / BI", "Data Science / ML",
         "Data Platform Migration", "App Development", "Data Governance", "Other"])
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='sb-block'>", unsafe_allow_html=True)
    st.markdown("### ⚙️ Plan Settings")
    team_size      = st.slider("Team Size", 2, 20, 5)
    duration_weeks = st.slider("Target Duration (weeks)", 2, 52, 12)
    risk_appetite  = st.select_slider("Risk Appetite", ["Low","Medium","High"], value="Medium")
    methodology    = st.selectbox("Methodology", ["Agile / Scrum","Waterfall","Hybrid","SAFe"])

    st.markdown("<div style='font-size:10px;font-weight:700;text-transform:uppercase;"
                "letter-spacing:0.12em;color:#4caf78;margin:0.6rem 0 0.3rem;'>Delivery Focus</div>",
                unsafe_allow_html=True)
    delivery_priority = st.selectbox("Priority",
        ["Balanced (scope, time, cost)",
         "Time-boxed (fixed deadline)",
         "Scope-driven (all requirements must be met)",
         "Cost-constrained (fixed budget)"])

    st.markdown("<div style='font-size:10px;font-weight:700;text-transform:uppercase;"
                "letter-spacing:0.12em;color:#4caf78;margin:0.6rem 0 0.3rem;'>Team Profile</div>",
                unsafe_allow_html=True)
    team_experience = st.selectbox("Team Snowflake Experience",
        ["Expert (3+ yrs)", "Intermediate (1-3 yrs)",
         "Beginner (< 1 yr)", "Mixed levels"])

    # ── Mandatory role headcounts ──────────────────────────
    st.markdown(
        "<div style='font-size:10px;color:rgba(255,255,255,0.4);margin:0.4rem 0 0.3rem;'>"
        "Mandatory Roles — set headcount (0 = not on this project)</div>",
        unsafe_allow_html=True
    )
    rc1, rc2 = st.columns(2)
    with rc1:
        role_pm  = st.number_input("👔 PM",  min_value=0, max_value=5, value=1, step=1)
        role_sa  = st.number_input("🏗️ SA", min_value=0, max_value=5, value=1, step=1)
        role_de  = st.number_input("⚙️ DE",    min_value=0, max_value=10, value=2, step=1)
    with rc2:
        role_ds  = st.number_input("🔬 DS",   min_value=0, max_value=5, value=1, step=1)
        role_qa  = st.number_input("🧪 QA",  min_value=0, max_value=5, value=1, step=1)

    # Derive legacy boolean flags for backward-compat with prompts
    has_dedicated_pm = role_pm > 0
    has_qa_resource  = role_qa > 0

    # Build a structured team roster string used in Cortex prompts
    team_roster = (
        f"PM×{role_pm}, SA×{role_sa}, DE×{role_de}, DS×{role_ds}, QA×{role_qa}"
    )
    # Recompute team_size from roster total
    team_size_from_roles = role_pm + role_sa + role_de + role_ds + role_qa
    # Allow override if user set slider above the roster total
    if team_size < team_size_from_roles:
        team_size = team_size_from_roles

    st.markdown("<div style='font-size:10px;font-weight:700;text-transform:uppercase;"
                "letter-spacing:0.12em;color:#4caf78;margin:0.6rem 0 0.3rem;'>Compliance & Constraints</div>",
                unsafe_allow_html=True)
    compliance_reqs  = st.multiselect("Compliance Requirements",
        ["GDPR", "HIPAA", "SOC 2", "PCI-DSS", "ISO 27001", "None"],
        default=["None"])
    has_data_sensitivity = st.selectbox("Data Sensitivity",
        ["Public", "Internal", "Confidential", "Restricted / PII"])
    st.markdown("</div>", unsafe_allow_html=True)

    generate_button = st.button("🚀 Generate Project Plan", type="primary")

# ═══════════════════════════════════════════════════════════════
# PAST PROJECTS PANEL
# ═══════════════════════════════════════════════════════════════
true_count      = len(past_projects_df)
csv_count       = len(st.session_state.csv_projects)
# FIX: names that exist in the DB (either always there or imported this session)
db_names_set    = set(past_projects_df['project_name'].tolist())
imported_names  = st.session_state.imported_csv_names  # names imported this session

if true_count > 0 or csv_count > 0:
    col_l, col_r = st.columns([2, 1])
    with col_l:
        st.markdown("<div class='section-label'>Institutional Memory</div>", unsafe_allow_html=True)
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        db_label  = f"{true_count} Completed Project{'s' if true_count!=1 else ''} in Database"
        csv_label = f" + {csv_count} CSV Plan{'s' if csv_count!=1 else ''}" if csv_count else ""
        st.markdown(f"<h2>📚 {db_label}{csv_label}</h2>", unsafe_allow_html=True)
        st.markdown(
            "<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:0.6rem;'>"
            "Cortex selects the most relevant past projects as templates. "
            "Use the dropdown to browse any project for reference."
            "</div>", unsafe_allow_html=True
        )

        project_names = past_projects_df['project_name'].tolist()

        # Build dropdown entries for CSV projects
        csv_display = []
        csv_map     = {}
        for p in st.session_state.csv_projects:
            nm  = p.get("project_name", "CSV Project")
            # FIX: check BOTH db names AND imported_names for accurate badge
            is_in_db = (nm in db_names_set) or (nm in imported_names)
            if is_in_db:
                lbl = f"📎 {nm} ✓ Imported"
            else:
                lbl = f"📎 {nm} · Pending Import"
            csv_display.append(lbl)
            csv_map[lbl] = p

        browse_options = ["— Select to view —"] + csv_display + project_names

        if len(browse_options) == 1:
            st.info("No projects loaded yet. Upload CSVs above or import to build institutional memory.")
        else:
            selected_proj = st.selectbox(
                "Browse a project for reference:",
                browse_options,
                help="Select any project — CSV uploads and DB projects both appear here."
            )
            if selected_proj and selected_proj != "— Select to view —":
                if selected_proj in csv_map:
                    proj_data = csv_map[selected_proj]
                    nm = proj_data.get('project_name','CSV Project')
                    is_in_db = (nm in db_names_set) or (nm in imported_names)
                    badge_html = (
                        "<span class='csv-badge-imported'>✓ In Database</span>"
                        if is_in_db else
                        "<span class='csv-badge-pending'>Pending Import</span>"
                    )
                    with st.expander(f"📁 {nm}", expanded=True):
                        st.markdown(
                            f"<div style='font-size:11px;color:rgba(255,255,255,0.5);margin-bottom:.5rem;'>"
                            f"📎 CSV Upload {badge_html}</div>",
                            unsafe_allow_html=True
                        )
                        d1, d2 = st.columns(2)
                        with d1:
                            st.markdown(f"**Description:** {proj_data.get('description','N/A')}")
                        with d2:
                            st.markdown(f"**Start:** {proj_data.get('start_date','—')} &nbsp; **End:** {proj_data.get('end_date','—')}")
                        st.markdown("**WBS Preview:**")
                        st.code(proj_data.get("wbs_summary","")[:600], language=None)
                else:
                    match = past_projects_df[past_projects_df["project_name"] == selected_proj]
                    if not match.empty:
                        row = match.iloc[0]
                        with st.expander(f"📁 {selected_proj}", expanded=True):
                            c1, c2 = st.columns(2)
                            with c1:
                                st.markdown(f"**Description:** {row.get('description','N/A')}")
                                st.markdown(f"**WBS:** {str(row.get('wbs_summary',''))[:300]}...")
                            with c2:
                                st.markdown(f"**RAID:** {str(row.get('risk_log_summary',''))[:300]}...")
                                st.markdown(f"**Deployment:** {str(row.get('deployment_plan',''))[:300]}...")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_r:
        st.markdown("<div class='section-label'>Quick Stats</div>", unsafe_allow_html=True)
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.metric("DB Projects", true_count)
        st.metric("CSV Projects", csv_count)
        st.metric("Total Available", true_count + csv_count)
        st.markdown("</div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# GENERATE PLAN
# ═══════════════════════════════════════════════════════════════
if generate_button:
    if not sow_pdf or not sow_text.strip():
        st.error("Please upload a SOW PDF before generating the plan.")
        st.stop()

    st.session_state.qa_history       = []
    st.session_state.wbs_structured   = None
    st.session_state.plan_methodology = methodology
    st.session_state.plan             = None
    st.session_state.sow_requirements = None
    st.session_state.past_patterns    = None

    loader = st.empty()

    def show_step(n, msg=""):
        steps = [
            "📄 Analysing SOW — extracting deliverables & scope",
            "🧠 Scanning institutional memory — matching past projects",
            "⚙️  Building WBS from SOW deliverables",
            "📚 Applying past project patterns — RAID, tests, timeline",
            "🔗 Assembling final plan document",
        ]
        items = "".join([
            '<div class="pulse-step' + (' active' if i == n else '') + '">'
            '<span class="step-dot"></span>' + s + '</div>'
            for i, s in enumerate(steps)
        ])
        loader.markdown(
            '<div class="pulse-loader">'
            '<div class="pulse-rings"><span></span><span></span><span></span><span></span></div>'
            '<div class="pulse-label">Cortex Project Intel</div>'
            '<div class="pulse-sublabel">' + (msg or "Processing...") + '</div>'
            '<div class="pulse-steps">' + items + '</div>'
            '</div>',
            unsafe_allow_html=True
        )

    is_agile    = any(k in methodology.lower() for k in ['agile', 'scrum', 'safe'])
    sprint_len  = 2
    num_sprints = max(1, duration_weeks // sprint_len)
    sprint_unit = "Sprint" if is_agile else "Phase"

    compliance_str = ", ".join(compliance_reqs) if compliance_reqs else "None"
    pm_str    = f"PM×{role_pm}" if role_pm > 0 else "No dedicated PM"
    qa_str    = f"QA×{role_qa}" if role_qa > 0 else "No dedicated QA"
    proj_context = (
        f"Project: {new_project_name}\n"
        f"Customer: {customer_name}\n"
        f"Type: {project_type}\n"
        f"Description: {new_project_desc[:300]}\n"
        f"Technologies: {new_project_tech}\n"
        f"Team: {team_size} people | Experience: {team_experience}\n"
        f"Roles: {team_roster} (PM=Project Manager, SA=Solution Architect, "
        f"DE=Data Engineer, DS=Data Scientist, QA=Quality Analyst)\n"
        f"Duration: {duration_weeks} weeks | Methodology: {methodology}\n"
        f"Delivery Priority: {delivery_priority}\n"
        f"Risk Appetite: {risk_appetite}\n"
        f"Data Sensitivity: {has_data_sensitivity}\n"
        f"Compliance: {compliance_str}"
    )

    # CALL 1 — SOW Analysis (deep extraction across 8 structured sections)
    show_step(0, "Reading " + sow_pdf.name + " ...")
    sow_call = (
        "You are a senior business analyst and PMO consultant reading a Statement of Work (SOW).\n\n"
        "Project context:\n" + proj_context + "\n\n"
        "Perform a DEEP analysis of the SOW below. Output EXACTLY these 8 numbered sections.\n"
        "Each section must be detailed — minimum 3-5 points each where applicable.\n\n"
        "1. PROJECT OBJECTIVE\n"
        "   Write 2-3 paragraphs: (a) what is being built and why, (b) the business problem it solves, "
        "(c) expected business outcomes and value delivered to " + customer_name + ".\n\n"
        "2. KEY DELIVERABLES\n"
        "   Numbered list. For each deliverable state: name, description, and the team/system responsible. "
        "Be specific — name every data pipeline, dashboard, model, integration, report, or platform component.\n\n"
        "3. ACCEPTANCE CRITERIA\n"
        "   For EACH deliverable from section 2, list measurable pass/fail criteria. "
        "E.g. row counts, latency thresholds, test pass rates, stakeholder sign-offs.\n\n"
        "4. FUNCTIONAL REQUIREMENTS\n"
        "   Bullet list of every functional requirement stated or implied in the SOW. "
        "Group by area (data ingestion, transformation, visualisation, security, etc.).\n\n"
        "5. NON-FUNCTIONAL REQUIREMENTS\n"
        "   Performance, scalability, availability, security, auditability, and maintainability requirements. "
        "Include any SLAs, RTO/RPO targets, or data freshness requirements mentioned.\n\n"
        "6. OUT OF SCOPE\n"
        "   List everything the SOW explicitly excludes or does not mention that a stakeholder might assume. "
        "Flag any grey areas that need clarification.\n\n"
        "7. CONSTRAINTS & DEPENDENCIES\n"
        "   (a) Budget constraints, (b) timeline / deadline constraints, "
        "(c) technology constraints (must use " + new_project_tech + "), "
        "(d) team constraints, (e) external dependencies (third-party systems, APIs, data sources), "
        "(f) " + compliance_str + " compliance requirements, "
        "(g) data sensitivity constraints (" + has_data_sensitivity + " data).\n\n"
        "8. RISKS & ASSUMPTIONS IN THE SOW\n"
        "   List any risks, assumptions, or open questions that the SOW itself raises. "
        "Flag anything ambiguous that could cause scope disputes or delivery delays.\n\n"
        "SOW TEXT:\n" + sow_text[:5000] + "\n\n"
        "Be thorough and specific. Reference exact wording from the SOW where relevant. "
        "If the SOW is silent on a topic, state 'Not specified in SOW — clarification recommended'."
    )
    sow_analysis = cortex_call(sow_call, "SOW Analysis")
    st.session_state.sow_requirements = sow_analysis

    # CALL 2 — Past Project Pattern Extraction
    show_step(1, "Scanning " + str(true_count) + " past projects...")
    candidates = [
        {"id": int(r['project_id']), "name": r['project_name'],
         "desc": str(r.get('description', ''))[:80]}
        for _, r in past_projects_df.iterrows()
    ]
    for ci, cp in enumerate(st.session_state.csv_projects):
        candidates.append({"id": -(ci + 1), "name": cp["project_name"],
                            "desc": cp.get("description","")[:80]})

    sel_raw = cortex_call(
        "Select up to 4 past projects most similar to:\n"
        + proj_context[:400] + "\n"
        "Candidates (JSON):\n" + json.dumps(candidates[:25])[:1200] + "\n\n"
        "Return ONLY a JSON array of IDs: [1, 4, 7]",
        "Template Selection"
    )
    selected_ids = []
    try:
        raw_ids = json.loads(
            sel_raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        )
        selected_ids = [int(x) for x in raw_ids if str(x).strip().isdigit()][:4]
    except Exception:
        selected_ids = [c["id"] for c in candidates[:3]]

    pattern_context = ""
    used_projects   = []
    csv_id_map = {-(i+1): p for i, p in enumerate(st.session_state.csv_projects)}

    for pid in selected_ids:
        match = past_projects_df[past_projects_df['project_id'].astype(str) == str(pid)]
        if pid < 0 and pid in csv_id_map:
            p = csv_id_map[pid]
            pattern_context += ("\n-- " + p['project_name'] + " (CSV) --\n"
                                 "WBS: " + p['wbs_summary'][:300] + "\n")
            used_projects.append(p['project_name'])
        elif not match.empty:
            r = match.iloc[0]
            pattern_context += ("\n-- " + r['project_name'] + " --\n"
                                 "RAID: " + str(r.get('risk_log_summary', ''))[:280] + "\n"
                                 "Tests: " + str(r.get('test_cases_summary', ''))[:200] + "\n"
                                 "Deployment: " + str(r.get('deployment_plan', ''))[:180] + "\n")
            used_projects.append(r['project_name'])

    if not pattern_context:
        pattern_context = "No past project data. Use PMO best practices."

    patterns_call = (
        "You are a PMO expert extracting reusable patterns from past Snowflake projects.\n\n"
        "New project context:\n" + proj_context + "\n\n"
        "Past project data:\n" + pattern_context[:3000] + "\n\n"
        "Output EXACTLY these four sections with the EXACT headers shown. "
        "Do not add extra text before or after each section header.\n\n"
        "### A. TOP RISKS\n"
        "Output exactly 8 pipe-delimited rows (no header row, no bullets, one risk per line).\n"
        "Format per row: CATEGORY | TYPE | DESCRIPTION | LIKELIHOOD | IMPACT | MITIGATION | OWNER\n"
        "CATEGORY must be one of: Risk, Assumption, Issue, Dependency\n"
        "LIKELIHOOD and IMPACT must each be: High, Medium, or Low\n"
        "Example row: Risk | Technical | Data pipeline fails on null values | High | High | Add null checks in transformation layer | Data Engineer\n\n"
        "### B. TEST PATTERNS\n"
        "Output exactly 8 pipe-delimited rows (no header row, no bullets, one test per line).\n"
        "Format per row: ID | PRIORITY | TYPE | SCENARIO | PRECONDITIONS | STEPS | EXPECTED RESULT\n"
        "PRIORITY must be: High, Medium, or Low\n"
        "TYPE must be one of: Integration, Data Quality, Security, Performance, Functional, Regression, UAT\n"
        "Example row: TC-01 | High | Integration | End-to-end data load | Source available | 1.Run pipeline 2.Query target | Row count matches source\n\n"
        "### C. TIMELINE LESSONS\n"
        "Output exactly 5 bullet points. Each bullet must be a specific, actionable lesson from past projects "
        "with a concrete recommendation (e.g. 'Integration testing took 40% longer than planned — "
        "add 3-day buffer after Sprint 3 integration tasks').\n\n"
        "### D. TEAM STRUCTURE\n"
        "Recommended roles for this project. Team roster: " + team_roster + " "
        "(PM=Project Manager, SA=Solution Architect, DE=Data Engineer, DS=Data Scientist, QA=Quality Analyst). "
        "Experience level: " + team_experience + ". "
        "For each role output: Role Title | Headcount | Key Responsibilities | Skills Required\n"
        "Output as pipe-delimited rows, one role per line.\n\n"
        "Be specific. Reference past project names where relevant. "
        "IMPORTANT: Output ONLY the four section headers and their content. No preamble, no conclusion."
    )
    past_patterns = cortex_call(patterns_call, "Pattern Extraction")
    st.session_state.past_patterns = past_patterns

    # CALL 3 — WBS from SOW
    show_step(2, "Decomposing SOW into sprint deliverables...")
    sprint_div = ("Divide into " + str(num_sprints) + " Sprints of 2 weeks each."
                  if is_agile else "Divide into phases.")
    wbs_call = (
        "You are a Snowflake Solutions Architect decomposing a SOW into a delivery plan.\n\n"
        "Project context:\n" + proj_context + "\n\n"
        "SOW Analysis:\n" + sow_analysis[:2300] + "\n\n"
        + sprint_div + "\n\n"
        "RULES:\n- Every task MUST map to a SOW deliverable.\n"
        "- Do NOT add scope not in the SOW.\n\n"
        "Return ONLY valid JSON array:\n"
        '[{"sprint_num":1,"name":"Sprint 1: Title","weeks":"Week 1-2",'
        '"goal":"Deliver SOW Deliverable 1.","sow_ref":"SOW Deliverable 1",'
        '"stories":["Task 1","Task 2","Task 3","Task 4"],'
        '"deliverable":"Acceptance criterion met"}]'
    )
    raw_wbs = cortex_call(wbs_call, "WBS Generation")
    wbs_sprints_gen = []
    try:
        clean = raw_wbs.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        wbs_sprints_gen = json.loads(clean)
        if not isinstance(wbs_sprints_gen, list):
            wbs_sprints_gen = []
    except Exception:
        pass
    if not wbs_sprints_gen:
        wbs_sprints_gen = parse_wbs_into_sprints(sow_analysis, duration_weeks, methodology)
    st.session_state.wbs_structured = wbs_sprints_gen

    # CALL 4 — Full Plan Assembly
    show_step(3, "Applying past project lessons to build RAID, tests and timeline...")
    wbs_for_prompt = "\n".join([
        "  " + s.get('name', '') + " (" + s.get('weeks', '') + "): "
        + s.get('goal', '') + " => " + s.get('deliverable', '')
        for s in wbs_sprints_gen
    ])[:1200]
    show_step(4, "Assembling final plan document...")

    assembly_call = (
        "You are a senior PMO consultant writing the final project plan.\n\n"
        "CRITICAL RULE: Every sprint, task, milestone, test case, and RAID item MUST directly trace back "
        "to a named deliverable or requirement from the SOW. Do NOT invent tasks outside the SOW scope.\n\n"
        "Project context:\n" + proj_context + "\n\n"
        "=== SOW SCOPE (USE AS PRIMARY SOURCE) ===\n" + sow_analysis[:1800] + "\n\n"
        "=== WBS — SPRINT PLAN (aligned to SOW deliverables) ===\n" + wbs_for_prompt + "\n\n"
        "=== PATTERNS FROM PAST PROJECTS (reference only) ===\n" + past_patterns[:1200] + "\n\n"
        "Write EXACTLY these 7 sections using the EXACT ## headers below:\n\n"
        "## EXECUTIVE SUMMARY\n"
        "3 paragraphs: (1) what SOW deliverables this plan covers and business value for "
        + customer_name + ", (2) how past project patterns reduce risk, (3) measurable KPIs and success criteria.\n\n"
        "## RAID LOG\n"
        "Min 10 rows. Every risk MUST reference a specific SOW deliverable or constraint.\n"
        "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
        "Format: CATEGORY | TYPE | DESCRIPTION | LIKELIHOOD | IMPACT | MITIGATION | OWNER\n"
        "CATEGORY must be: Risk, Assumption, Issue, or Dependency\n"
        "LIKELIHOOD and IMPACT must be: High, Medium, or Low\n\n"
        "## TEST CASES\n"
        "Min 10 rows. Each test must map to a SOW acceptance criterion.\n"
        "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
        "Format: ID | PRIORITY | TYPE | SCENARIO | PRECONDITIONS | STEPS | EXPECTED RESULT\n"
        "PRIORITY must be: High, Medium, or Low. TYPE must be: Integration, Data Quality, Security, Performance, Functional, Regression, or UAT\n\n"
        "## DELIVERY TIMELINE\n"
        "One row per sprint/milestone from the WBS. Tie each milestone to a SOW deliverable.\n"
        "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
        "Format: WEEK | SPRINT/PHASE | MILESTONE | OWNER | STATUS | RISK FLAG\n\n"
        "## DEPLOYMENT STRATEGY\n"
        "Prose only. Phased go-live plan tied to SOW acceptance criteria. Rollback plan. Sign-off gates. "
        "Reference specific deliverables by name.\n\n"
        "## SOW TRACEABILITY MATRIX\n"
        "One row per SOW deliverable. Every deliverable from the SOW must appear here.\n"
        "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
        "Format: SOW REQUIREMENT | SPRINT/PHASE | KEY TASKS | ACCEPTANCE CRITERIA | STATUS\n\n"
        "## LESSONS FROM PAST PROJECTS\n"
        "Bullet list. Each lesson must state: what happened in a past project and how it changed THIS plan.\n\n"
        "ABSOLUTE RULE: Sections with pipe-delimited format must contain ONLY data rows. "
        "No column headers, no section sub-titles, no bullet points, no blank lines within them."
    )
    full_plan = cortex_call(assembly_call, "Plan Assembly")
    if full_plan:
        st.session_state.plan      = full_plan
        st.session_state.plan_name = new_project_name
    else:
        st.session_state.plan = (
            "Plan assembly failed. SOW analysis and WBS are still available — "
            "check the SOW Analysis and WBS tabs."
        )

    loader.empty()
    st.rerun()

# ═══════════════════════════════════════════════════════════════
# DISPLAY PLAN
# ═══════════════════════════════════════════════════════════════
if st.session_state.plan:
    plan      = st.session_state.plan
    plan_name = st.session_state.plan_name
    meth      = st.session_state.plan_methodology or methodology
    sprints   = st.session_state.wbs_structured or []
    is_agile  = any(k in meth.lower() for k in ['agile','scrum','safe'])

    # Impact metrics
    st.markdown("<div class='section-label'>Delivery Impact</div>", unsafe_allow_html=True)
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1: st.metric("Planning Time", "Weeks → Minutes", "90% reduction")
    with mc2: st.metric("Risk Identification", "Reactive → Predictive", "From past projects")
    with mc3: st.metric("Templates Used", min(true_count, 4), "matched by Cortex")
    with mc4: st.metric("Sprint / Phase Count", len(sprints) if sprints else "—")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-label' style='margin-top:0.25rem;'>Generated Project Plan</div>", unsafe_allow_html=True)

    tab_plan, tab_wbs, tab_sow, tab_qa, tab_risks, tab_export = st.tabs([
        "📋 Full Plan",
        "🏃 WBS / Sprints",
        "📄 SOW Analysis",
        "💬 Ask the Plan",
        "⚠️ Risk Analyser",
        "📤 Export",
    ])

    # ── TAB 1: Full Plan ──────────────────────────────────────
    with tab_plan:
        st.markdown(f"<h2>📋 {plan_name}</h2>", unsafe_allow_html=True)

        # ── Full Plan renderer: colour-coded tables for every section ──
        def render_plan_with_tables(plan_text):
            LH_BG  = {"High":"rgba(224,92,92,0.15)","Medium":"rgba(240,165,0,0.12)","Low":"rgba(76,175,120,0.12)"}
            LH_COL = {"High":"#e05c5c","Medium":"#f0a500","Low":"#4caf78"}
            CAT_BG  = {"Risk":"rgba(224,92,92,0.12)","Assumption":"rgba(91,140,232,0.12)",
                       "Issue":"rgba(240,165,0,0.12)","Dependency":"rgba(160,91,232,0.12)"}
            CAT_COL = {"Risk":"#e05c5c","Assumption":"#5b8ce8","Issue":"#f0a500","Dependency":"#a05be8"}
            TT_BG  = {"Integration":"rgba(91,140,232,0.12)","Data Quality":"rgba(76,175,120,0.12)",
                      "Security":"rgba(224,92,92,0.12)","Performance":"rgba(240,165,0,0.12)",
                      "Functional":"rgba(160,91,232,0.12)","Regression":"rgba(140,140,140,0.12)",
                      "Uat":"rgba(76,175,120,0.12)","UAT":"rgba(76,175,120,0.12)"}
            TT_COL = {"Integration":"#5b8ce8","Data Quality":"#4caf78","Security":"#e05c5c",
                      "Performance":"#f0a500","Functional":"#a05be8","Regression":"#888",
                      "Uat":"#4caf78","UAT":"#4caf78"}
            ST_BG  = {"To Do":"rgba(140,140,140,0.12)","Planned":"rgba(240,165,0,0.12)",
                      "In Progress":"rgba(91,140,232,0.12)","Done":"rgba(76,175,120,0.12)",
                      "Complete":"rgba(76,175,120,0.12)","Not Started":"rgba(140,140,140,0.12)",
                      "Pending":"rgba(240,165,0,0.12)"}
            ST_COL = {"To Do":"#888","Planned":"#f0a500","In Progress":"#5b8ce8",
                      "Done":"#4caf78","Complete":"#4caf78","Not Started":"#888","Pending":"#f0a500"}

            # per-section config: icon, accent colour, headers list, column styler fn
            SECTIONS = {
                "EXECUTIVE SUMMARY":  {"icon":"📋","col":"#4caf78","table":False,"headers":[]},
                "RAID LOG":           {"icon":"⚠️","col":"#e05c5c","table":True,
                    "headers":["Category","Type","Description","Likelihood","Impact","Mitigation","Owner"],
                    "colfn": lambda ci, v: (
                        "background:%s;color:%s;font-weight:700;text-align:center;" % (CAT_BG.get(v.title(),"rgba(255,255,255,0.04)"), CAT_COL.get(v.title(),"rgba(255,255,255,0.55)")) if ci==0 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.5);text-align:center;font-size:10px;" if ci==1 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.88);font-weight:600;" if ci==2 else
                        "background:%s;color:%s;font-weight:700;text-align:center;" % (LH_BG.get(v.title(),"rgba(255,255,255,0.04)"), LH_COL.get(v.title(),"#aaa")) if ci in (3,4) else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.62);" if ci==5 else
                        "background:rgba(76,175,120,0.08);color:#4caf78;font-weight:600;text-align:center;"
                    )},
                "TEST CASES":         {"icon":"🧪","col":"#5b8ce8","table":True,
                    "headers":["ID","Priority","Type","Scenario","Preconditions","Steps","Expected Result"],
                    "colfn": lambda ci, v: (
                        "background:rgba(91,140,232,0.12);color:#5b8ce8;font-weight:700;text-align:center;white-space:nowrap;" if ci==0 else
                        "background:%s;color:%s;font-weight:700;text-align:center;" % (LH_BG.get(v.title(),"rgba(255,255,255,0.04)"), LH_COL.get(v.title(),"#aaa")) if ci==1 else
                        "background:%s;color:%s;font-weight:700;text-align:center;white-space:nowrap;" % (TT_BG.get(v.title(),TT_BG.get(v,"rgba(255,255,255,0.04)")), TT_COL.get(v.title(),TT_COL.get(v,"#aaa"))) if ci==2 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.88);font-weight:600;" if ci==3 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.55);" if ci==4 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.65);" if ci==5 else
                        "background:rgba(76,175,120,0.08);color:#4caf78;font-weight:600;"
                    )},
                "DELIVERY TIMELINE":  {"icon":"📅","col":"#f0a500","table":True,
                    "headers":["Week","Sprint / Phase","Milestone","Owner","Status","Risk Flag"],
                    "colfn": lambda ci, v: (
                        "background:rgba(240,165,0,0.12);color:#f0a500;font-weight:700;text-align:center;white-space:nowrap;" if ci==0 else
                        "background:rgba(91,140,232,0.08);color:#5b8ce8;font-weight:600;" if ci==1 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.88);font-weight:600;" if ci==2 else
                        "background:rgba(76,175,120,0.07);color:#4caf78;text-align:center;" if ci==3 else
                        "background:%s;color:%s;font-weight:700;text-align:center;" % (ST_BG.get(v.title(),ST_BG.get(v,"rgba(140,140,140,0.1)")), ST_COL.get(v.title(),ST_COL.get(v,"#888"))) if ci==4 else
                        "background:%s;color:%s;font-weight:700;text-align:center;" % (LH_BG.get(v.title(),LH_BG.get(v,"rgba(76,175,120,0.08)")), LH_COL.get(v.title(),LH_COL.get(v,"#4caf78")))
                    )},
                "DEPLOYMENT STRATEGY":{"icon":"🚀","col":"#a05be8","table":False,"headers":[]},
                "SOW TRACEABILITY":   {"icon":"🔗","col":"#4caf78","table":True,
                    "headers":["SOW Requirement","Sprint / Phase","Key Tasks","Acceptance Criteria","Status"],
                    "colfn": lambda ci, v: (
                        "background:rgba(76,175,120,0.12);color:#4caf78;font-weight:700;" if ci==0 else
                        "background:rgba(91,140,232,0.08);color:#5b8ce8;font-weight:600;text-align:center;white-space:nowrap;" if ci==1 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.75);" if ci==2 else
                        "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.6);font-style:italic;" if ci==3 else
                        "background:%s;color:%s;font-weight:700;text-align:center;" % (ST_BG.get(v.title(),ST_BG.get(v,"rgba(140,140,140,0.1)")), ST_COL.get(v.title(),ST_COL.get(v,"#888")))
                    )},
                "LESSONS FROM PAST":  {"icon":"📚","col":"#4caf78","table":False,"headers":[]},
            }

            def make_table(sec_key, rows):
                if not rows: return ""
                sec     = SECTIONS.get(sec_key, {})
                headers = sec.get("headers", [])
                colfn   = sec.get("colfn", lambda ci, v: "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.75);")
                accent  = sec.get("col","#4caf78")
                th = "".join(
                    "<th style='padding:0.5rem 0.85rem;text-align:left;font-size:10px;"
                    "font-weight:700;letter-spacing:0.07em;color:%s;white-space:nowrap;"
                    "background:#0a1a10;border-bottom:2px solid %s44;"
                    "border-right:1px solid rgba(255,255,255,0.04);'>%s</th>" % (accent, accent, h)
                    for h in headers
                )
                body = ""
                for row in rows:
                    while len(row) < len(headers): row.append("")
                    row = row[:len(headers)]
                    cells = ""
                    for ci, cell in enumerate(row):
                        v   = str(cell).strip()
                        sty = colfn(ci, v)
                        cells += (
                            "<td style='padding:0.45rem 0.85rem;font-size:11px;"
                            "vertical-align:top;line-height:1.6;"
                            "border-bottom:1px solid rgba(255,255,255,0.04);"
                            "border-right:1px solid rgba(255,255,255,0.04);%s'>%s</td>" % (sty, v)
                        )
                    body += "<tr>%s</tr>" % cells
                return (
                    "<div style='overflow-x:auto;margin:0.6rem 0 1.5rem 0;"
                    "border-radius:8px;border:1px solid %s33;'>"
                    "<table style='width:100%%;border-collapse:collapse;'>"
                    "<thead><tr>%s</tr></thead><tbody>%s</tbody>"
                    "</table></div>" % (accent, th, body)
                )

            def make_header(sec_key, title):
                sec    = SECTIONS.get(sec_key, {"icon":"▸","col":"#4caf78"})
                return (
                    "<div style='display:flex;align-items:center;gap:0.55rem;"
                    "margin:1.5rem 0 0.6rem 0;padding:0.55rem 0.9rem;"
                    "background:rgba(255,255,255,0.025);border-radius:8px;"
                    "border-left:3px solid %s;'>"
                    "<span style='font-size:1rem;'>%s</span>"
                    "<span style='font-size:12px;font-weight:800;color:%s;"
                    "text-transform:uppercase;letter-spacing:0.1em;'>%s</span>"
                    "</div>" % (sec["col"], sec["icon"], sec["col"], title)
                )

            lines        = plan_text.split("\n")
            output       = ""
            cur_sec      = None
            tbl_rows     = []
            in_tbl       = False

            for line in lines:
                s = line.strip()
                if s.startswith("##") or s.startswith("# "):
                    if in_tbl and tbl_rows and cur_sec:
                        output += make_table(cur_sec, tbl_rows)
                    tbl_rows = []; in_tbl = False; cur_sec = None
                    title = s.lstrip("#").strip()
                    matched = next((k for k in SECTIONS if k in title.upper()), None)
                    cur_sec = matched
                    output += make_header(matched or title.upper(), title)
                    if matched and SECTIONS[matched]["table"]:
                        in_tbl = True
                elif in_tbl and "|" in s:
                    parts = [p.strip() for p in s.split("|") if p.strip()]
                    if parts and not all(set(p) <= set("-: ") for p in parts):
                        hdrs = SECTIONS.get(cur_sec, {}).get("headers", [])
                        if parts[0].upper() not in [h.upper() for h in hdrs]:
                            tbl_rows.append(parts)
                elif in_tbl and s == "":
                    if tbl_rows and cur_sec:
                        output += make_table(cur_sec, tbl_rows)
                        tbl_rows = []
                    in_tbl = False
                elif not in_tbl and s:
                    if s.startswith("- ") or s.startswith("* ") or s.startswith("• "):
                        c = re.sub(r"^[-*•]\s*","",s)
                        output += ("<div style='display:flex;gap:0.5rem;padding:0.22rem 0.2rem;"
                                   "color:rgba(255,255,255,0.75);font-size:12px;line-height:1.7;'>"
                                   "<span style='color:#4caf78;flex-shrink:0;font-size:0.75rem;margin-top:0.1rem;'>▸</span>"
                                   "<span>%s</span></div>" % c)
                    elif re.match(r"^\d+\.\s", s):
                        m2 = re.match(r"^(\d+)\.\s*(.*)", s)
                        if m2:
                            output += ("<div style='display:flex;gap:0.5rem;padding:0.22rem 0.2rem;"
                                       "color:rgba(255,255,255,0.78);font-size:12px;line-height:1.7;'>"
                                       "<span style='color:#4caf78;font-weight:700;flex-shrink:0;min-width:1.4rem;'>%s.</span>"
                                       "<span>%s</span></div>" % (m2.group(1), m2.group(2)))
                    else:
                        output += ("<div style='color:rgba(255,255,255,0.7);font-size:12px;"
                                   "line-height:1.85;padding:0.1rem 0.2rem;'>%s</div>" % s)

            if in_tbl and tbl_rows and cur_sec:
                output += make_table(cur_sec, tbl_rows)
            return output

        rendered = render_plan_with_tables(plan)
        st.markdown(
            "<div style='background:#112218;border:1px solid rgba(76,175,120,0.2);"
            "border-radius:12px;padding:1.5rem 1.75rem;'>"
            + rendered +
            "</div>",
            unsafe_allow_html=True
        )
        st.success("✅ Plan generated — WBS from SOW · RAID & Tests from institutional memory.")

    # ── TAB 2: Visual WBS / Sprints ────────────────────────────
    with tab_wbs:
        label = "Sprint Board" if is_agile else "Work Breakdown Structure"
        st.markdown(f"<h2>🏃 {label} — {plan_name}</h2>", unsafe_allow_html=True)
        cadence_label = "2-week sprints" if is_agile else "Project phases"
        unit_label    = "sprints"        if is_agile else "phases"
        st.markdown(
            f"<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:0.75rem;'>"
            f"{cadence_label} · {len(sprints)} {unit_label} · {meth}</div>",
            unsafe_allow_html=True
        )

        if not sprints:
            st.info("No sprint data — regenerate the plan to see the structured WBS.")
        else:
            sum_cols = st.columns(min(len(sprints), 6))
            for i, sp in enumerate(sprints[:6]):
                with sum_cols[i]:
                    st.markdown(f"""
                    <div style="background:rgba(76,175,120,0.08);border:1px solid rgba(76,175,120,0.25);
                                border-radius:8px;padding:0.6rem;text-align:center;">
                        <div style="font-size:9px;font-weight:700;color:#4caf78;text-transform:uppercase;
                                    letter-spacing:0.1em;">{sp.get('weeks','')}</div>
                        <div style="font-size:11px;font-weight:700;color:rgba(255,255,255,0.85);
                                    margin-top:0.2rem;line-height:1.3;">{sp.get('name','').split(':')[0]}</div>
                    </div>""", unsafe_allow_html=True)

            st.markdown("<hr>", unsafe_allow_html=True)

            scol1, scol2 = st.columns(2)
            for i, sp in enumerate(sprints):
                raw_tasks = sp.get("stories", sp.get("tasks", []))
                display_tasks = []
                for t in raw_tasks:
                    if isinstance(t, dict):
                        display_tasks.append(t.get("name", t.get("title", t.get("text", str(t)))))
                    elif t:
                        display_tasks.append(str(t))
                with (scol1 if i % 2 == 0 else scol2):
                    tasks_html = "".join([
                        f"<div class='sprint-task'>{'🔵' if is_agile else '▸'} {t}</div>"
                        for t in display_tasks
                    ])
                    deliverable = str(sp.get("deliverable",""))
                    st.markdown(f"""
                    <div class="sprint-card">
                        <div class="sprint-header">{sp.get('name','')} &nbsp;·&nbsp; {sp.get('weeks','')}</div>
                        <div class="sprint-goal">🎯 {sp.get('goal','')}</div>
                        {tasks_html}
                        {f'<div style="font-size:10px;color:#4caf78;margin-top:0.4rem;font-weight:600;">✓ Deliverable: {deliverable}</div>' if deliverable else ''}
                    </div>""", unsafe_allow_html=True)

    # ── TAB 3: SOW Analysis ────────────────────────────────────
    with tab_sow:
        st.markdown("<h2>📄 SOW Analysis — Deep Document Intelligence</h2>", unsafe_allow_html=True)
        st.markdown(
            "<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:1rem;'>"
            "Full structured breakdown extracted by Cortex from your SOW — 8 sections covering "
            "objectives, deliverables, acceptance criteria, functional &amp; non-functional requirements, "
            "constraints, and open risks."
            "</div>", unsafe_allow_html=True
        )

        sow_req = st.session_state.sow_requirements or ""

        # ── SOW section icon map ──────────────────────────────
        SOW_ICONS = {
            "PROJECT OBJECTIVE":              ("🎯", "#4caf78"),
            "KEY DELIVERABLES":               ("📦", "#4caf78"),
            "ACCEPTANCE CRITERIA":            ("✅", "#4caf78"),
            "FUNCTIONAL REQUIREMENTS":        ("⚙️",  "#5bc8e8"),
            "NON-FUNCTIONAL REQUIREMENTS":    ("🔧", "#5bc8e8"),
            "OUT OF SCOPE":                   ("🚫", "#e08c5e"),
            "CONSTRAINTS":                    ("⛓️",  "#e0c45e"),
            "RISKS & ASSUMPTIONS":            ("⚠️",  "#e07070"),
            "RISKS AND ASSUMPTIONS":          ("⚠️",  "#e07070"),
        }

        def render_sow_analysis(text):
            if not text:
                return ""
            lines = text.split('\n')
            html  = ""
            in_section = False

            for line in lines:
                s = line.strip()
                if not s:
                    if in_section:
                        html += "<div style='height:0.35rem;'></div>"
                    continue

                # Detect numbered section header: "1. SECTION NAME" or "## SECTION NAME"
                is_header = False
                header_text = ""
                if re.match(r'^\d+\.', s):
                    header_text = re.sub(r'^\d+\.\s*', '', s).strip()
                    is_header = True
                elif s.startswith('##') or s.startswith('# '):
                    header_text = s.lstrip('#').strip()
                    is_header = True

                if is_header:
                    # Close previous section
                    if in_section:
                        html += "</div>"

                    # Find icon/colour
                    ht_upper = header_text.upper()
                    icon, color = "📌", "#4caf78"
                    for key, (ic, co) in SOW_ICONS.items():
                        if key in ht_upper:
                            icon, color = ic, co
                            break

                    html += (
                        f"<div style='margin-bottom:0.9rem;'>"
                        f"<div style='display:flex;align-items:center;gap:0.55rem;"
                        f"padding:0.6rem 0.9rem;margin-bottom:0.5rem;"
                        f"background:rgba(255,255,255,0.03);border-radius:8px;"
                        f"border-left:3px solid {color};'>"
                        f"<span style='font-size:1rem;'>{icon}</span>"
                        f"<span style='font-size:12px;font-weight:700;color:{color};"
                        f"text-transform:uppercase;letter-spacing:0.09em;'>{header_text}</span>"
                        f"</div>"
                        f"<div style='padding-left:0.5rem;'>"
                    )
                    in_section = True

                elif s.startswith('-') or s.startswith('•') or s.startswith('*'):
                    content = re.sub(r'^[-•*]\s*', '', s)
                    html += (
                        f"<div style='display:flex;gap:0.5rem;padding:0.22rem 0;"
                        f"color:rgba(255,255,255,0.78);font-size:12px;line-height:1.65;'>"
                        f"<span style='color:#4caf78;flex-shrink:0;margin-top:0.1rem;'>▸</span>"
                        f"<span>{content}</span></div>"
                    )

                elif re.match(r'^\d+\.', s) and not is_header:
                    # Numbered sub-item inside a section
                    num_match = re.match(r'^(\d+)\.\s*(.*)', s)
                    if num_match:
                        num = num_match.group(1)
                        content = num_match.group(2)
                        html += (
                            f"<div style='display:flex;gap:0.5rem;padding:0.22rem 0;"
                            f"color:rgba(255,255,255,0.78);font-size:12px;line-height:1.65;'>"
                            f"<span style='color:#4caf78;font-weight:700;flex-shrink:0;"
                            f"min-width:1.2rem;'>{num}.</span>"
                            f"<span>{content}</span></div>"
                        )

                elif '|' in s:
                    # Inline pipe-separated sub-table (e.g. in constraints)
                    parts = [p.strip() for p in s.split('|') if p.strip()]
                    if len(parts) >= 2:
                        html += (
                            "<div style='display:flex;gap:0.5rem;flex-wrap:wrap;"
                            "padding:0.25rem 0;'>"
                        )
                        for p in parts:
                            html += (
                                f"<span style='background:rgba(76,175,120,0.1);"
                                f"border:1px solid rgba(76,175,120,0.25);border-radius:4px;"
                                f"padding:0.15rem 0.55rem;font-size:11px;"
                                f"color:rgba(255,255,255,0.75);'>{p}</span>"
                            )
                        html += "</div>"
                    else:
                        html += (
                            f"<div style='color:rgba(255,255,255,0.72);font-size:12px;"
                            f"line-height:1.75;padding:0.12rem 0;'>{s}</div>"
                        )

                else:
                    # Plain prose
                    html += (
                        f"<div style='color:rgba(255,255,255,0.72);font-size:12px;"
                        f"line-height:1.8;padding:0.12rem 0;'>{s}</div>"
                    )

            if in_section:
                html += "</div></div>"
            return html

        if sow_req:
            st.markdown(
                "<div style='background:#112218;border:1px solid rgba(76,175,120,0.18);"
                "border-radius:12px;padding:1.5rem 1.75rem;'>"
                + render_sow_analysis(sow_req) +
                "</div>",
                unsafe_allow_html=True
            )
        else:
            st.info("SOW analysis not available — regenerate the plan.")

        # ── SOW Workflow Diagram ───────────────────────────────
        if sprints:
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown(
                "<h2>🗺️ Project Workflow Diagram</h2>",
                unsafe_allow_html=True
            )
            st.markdown(
                "<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:0.75rem;'>"
                "Visual end-to-end delivery flow — SOW intake through to deployment, "
                "aligned to your sprint plan."
                "</div>", unsafe_allow_html=True
            )

            def build_workflow_svg(sprint_list, methodology):
                is_ag = any(k in methodology.lower() for k in ['agile','scrum','safe'])
                # ── fixed pipeline stages ──
                stages = [
                    ("SOW\nIntake",    "#3d8c5e", "📄"),
                    ("Discovery &\nRequirements", "#2d5fa8", "🔍"),
                    ("Architecture\n& Design",    "#6a3a9f", "🏗️"),
                    ("Development\n& Build",       "#c07a00", "⚙️"),
                    ("Testing &\nQA",              "#5b8ce8", "🧪"),
                    ("UAT &\nSign-off",             "#2d7a50", "✅"),
                    ("Deployment\n& Handover",      "#4caf78", "🚀"),
                ]
                # map sprints to stages
                n     = len(stages)
                sw    = 120   # stage box width
                sh    = 64    # stage box height
                gap   = 52    # gap between boxes (arrow space)
                pad_x = 30
                pad_y = 110
                total_w = pad_x * 2 + n * sw + (n - 1) * gap
                total_h = pad_y + sh + 140   # room for sprint pills below

                svg_parts = [
                    f'<svg viewBox="0 0 {total_w} {total_h}" xmlns="http://www.w3.org/2000/svg" '
                    f'style="width:100%;max-width:{total_w}px;font-family:Plus Jakarta Sans,sans-serif;">',
                    # background
                    f'<rect width="{total_w}" height="{total_h}" fill="#0d1a11" rx="12"/>',
                    # subtle grid lines
                ]
                for gi in range(1, n):
                    gx = pad_x + gi * (sw + gap) - gap // 2
                    svg_parts.append(
                        f'<line x1="{gx}" y1="20" x2="{gx}" y2="{total_h-20}" '
                        f'stroke="rgba(255,255,255,0.04)" stroke-width="1"/>'
                    )

                # ── stage boxes + arrows ──
                for i, (label, color, icon) in enumerate(stages):
                    x  = pad_x + i * (sw + gap)
                    y  = pad_y
                    cx = x + sw // 2

                    # arrow between boxes
                    if i > 0:
                        ax1 = x - gap + 4
                        ax2 = x - 4
                        ay  = y + sh // 2
                        svg_parts.append(
                            f'<line x1="{ax1}" y1="{ay}" x2="{ax2}" y2="{ay}" '
                            f'stroke="#4caf78" stroke-width="2" stroke-dasharray="4 3" opacity="0.6"/>'
                        )
                        svg_parts.append(
                            f'<polygon points="{ax2},{ay-5} {ax2+8},{ay} {ax2},{ay+5}" '
                            f'fill="#4caf78" opacity="0.7"/>'
                        )

                    # box shadow / glow
                    svg_parts.append(
                        f'<rect x="{x+2}" y="{y+2}" width="{sw}" height="{sh}" '
                        f'rx="8" fill="{color}" opacity="0.15"/>'
                    )
                    # box
                    svg_parts.append(
                        f'<rect x="{x}" y="{y}" width="{sw}" height="{sh}" '
                        f'rx="8" fill="{color}" fill-opacity="0.18" '
                        f'stroke="{color}" stroke-opacity="0.7" stroke-width="1.5"/>'
                    )
                    # icon
                    svg_parts.append(
                        f'<text x="{cx}" y="{y+22}" text-anchor="middle" '
                        f'font-size="14" dominant-baseline="middle">{icon}</text>'
                    )
                    # label (multi-line)
                    lines_lbl = label.split('\n')
                    for li, ln in enumerate(lines_lbl):
                        ly = y + 36 + li * 14
                        svg_parts.append(
                            f'<text x="{cx}" y="{ly}" text-anchor="middle" '
                            f'font-size="9" font-weight="700" fill="{color}" '
                            f'letter-spacing="0.04em">{ln}</text>'
                        )

                    # step number circle
                    svg_parts.append(
                        f'<circle cx="{x+14}" cy="{y+14}" r="9" fill="{color}" fill-opacity="0.8"/>'
                    )
                    svg_parts.append(
                        f'<text x="{x+14}" y="{y+14}" text-anchor="middle" '
                        f'dominant-baseline="middle" font-size="8" font-weight="800" fill="#fff">{i+1}</text>'
                    )

                # ── Sprint pills below stages ──
                pill_y = pad_y + sh + 20
                pill_h = 22

                # distribute sprints across stage positions
                sprint_colors = [
                    "#4caf78","#5b8ce8","#a05be8","#f0a500","#e05c5c","#5bc8e8","#4caf78"
                ]
                n_sp   = len(sprint_list)
                slot_w = (total_w - pad_x * 2) / max(n_sp, 1)

                for si, sp in enumerate(sprint_list):
                    sx      = pad_x + si * slot_w
                    sxc     = sx + slot_w / 2
                    sc      = sprint_colors[si % len(sprint_colors)]
                    sp_name = str(sp.get('name', f'Sprint {si+1}')).split(':')[0].strip()
                    sp_wk   = str(sp.get('weeks', ''))

                    # connector dot from stage area
                    stage_i = min(int(si * n / max(n_sp, 1)), n - 1)
                    stage_cx = pad_x + stage_i * (sw + gap) + sw // 2

                    svg_parts.append(
                        f'<line x1="{stage_cx}" y1="{pad_y + sh}" x2="{sxc}" y2="{pill_y}" '
                        f'stroke="{sc}" stroke-opacity="0.3" stroke-width="1" stroke-dasharray="3 3"/>'
                    )
                    # pill bg
                    pill_w = min(slot_w - 8, total_w / n_sp - 4)
                    svg_parts.append(
                        f'<rect x="{sxc - pill_w/2}" y="{pill_y}" '
                        f'width="{pill_w}" height="{pill_h}" '
                        f'rx="11" fill="{sc}" fill-opacity="0.15" '
                        f'stroke="{sc}" stroke-opacity="0.5" stroke-width="1"/>'
                    )
                    svg_parts.append(
                        f'<text x="{sxc}" y="{pill_y + 8}" text-anchor="middle" '
                        f'font-size="8" font-weight="700" fill="{sc}">{sp_name}</text>'
                    )
                    svg_parts.append(
                        f'<text x="{sxc}" y="{pill_y + 18}" text-anchor="middle" '
                        f'font-size="7" fill="rgba(255,255,255,0.4)">{sp_wk}</text>'
                    )

                # ── Legend row ──
                leg_y = total_h - 24
                leg_items = [
                    ("#4caf78", "Sprint / Phase"),
                    ("#5b8ce8", "Development Gate"),
                    ("#f0a500", "Review Gate"),
                    ("#e05c5c", "Risk Checkpoint"),
                ]
                lx = pad_x
                for lc, lt in leg_items:
                    svg_parts.append(
                        f'<circle cx="{lx+5}" cy="{leg_y}" r="4" fill="{lc}" opacity="0.8"/>'
                    )
                    svg_parts.append(
                        f'<text x="{lx+13}" y="{leg_y}" dominant-baseline="middle" '
                        f'font-size="8" fill="rgba(255,255,255,0.4)">{lt}</text>'
                    )
                    lx += 130

                svg_parts.append('</svg>')
                return "".join(svg_parts)

            wf_svg = build_workflow_svg(sprints, meth)
            st.markdown(
                "<div style='background:#0d1a11;border:1px solid rgba(76,175,120,0.18);"
                "border-radius:12px;padding:1rem;overflow-x:auto;'>"
                + wf_svg +
                "</div>",
                unsafe_allow_html=True
            )

        # ── Patterns from Past Projects ───────────────────────
        past_p = st.session_state.past_patterns or ""
        if past_p:
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown(
                "<h2>🧠 Patterns Applied from Past Projects</h2>",
                unsafe_allow_html=True
            )
            st.markdown(
                "<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:1rem;'>"
                "Risks, test patterns, timeline lessons and team structure extracted from matched past projects "
                "and adapted for this project."
                "</div>", unsafe_allow_html=True
            )

            def render_patterns(text):
                """
                Parse the 4 sections (### A. TOP RISKS, ### B. TEST PATTERNS,
                ### C. TIMELINE LESSONS, ### D. TEAM STRUCTURE) and render each
                with appropriate UI: risk cards, test table, bullet lessons, role table.
                """

                # Split into sections by ### A. / ### B. / ### C. / ### D.
                section_re = re.compile(
                    r'###\s*([A-D])\.\s*(.*?)(?=\n###\s*[A-D]\.|\Z)',
                    re.DOTALL | re.IGNORECASE
                )
                sections = {m.group(1).upper(): (m.group(2).strip(), m.group(0))
                            for m in section_re.finditer(text)}

                # Also try plain "A. TOP RISKS" headers without ### prefix
                if not sections:
                    section_re2 = re.compile(
                        r'^([A-D])\.\s+(TOP RISKS|TEST PATTERNS|TIMELINE LESSONS|TEAM STRUCTURE)(.*?)(?=^[A-D]\.\s+|\Z)',
                        re.DOTALL | re.IGNORECASE | re.MULTILINE
                    )
                    sections = {m.group(1).upper(): (m.group(2).strip() + m.group(3), m.group(0))
                                for m in section_re2.finditer(text)}

                html = ""

                # ── A. TOP RISKS ──────────────────────────────
                lh_bg  = {"High":"rgba(224,92,92,0.12)","Medium":"rgba(240,165,0,0.1)","Low":"rgba(76,175,120,0.1)","—":"rgba(255,255,255,0.04)"}
                lh_col = {"High":"#e05c5c","Medium":"#f0a500","Low":"#4caf78","—":"rgba(255,255,255,0.35)"}
                cat_col = {"Risk":"#e05c5c","Assumption":"#5b8ce8","Issue":"#f0a500",
                           "Dependency":"#a05be8","—":"rgba(255,255,255,0.5)"}
                cat_bg  = {"Risk":"rgba(224,92,92,0.1)","Assumption":"rgba(91,140,232,0.1)",
                           "Issue":"rgba(240,165,0,0.1)","Dependency":"rgba(160,91,232,0.1)","—":"transparent"}

                a_content = sections.get("A", ("", ""))[0] if "A" in sections else ""
                if not a_content:
                    # fallback: search raw text
                    m = re.search(r'(?:###?\s*A\.|A\.\s+TOP RISKS)(.*?)(?=###?\s*B\.|B\.\s+TEST|\Z)',
                                  text, re.DOTALL | re.IGNORECASE)
                    if m: a_content = m.group(1).strip()

                risk_rows = []
                for line in a_content.split('\n'):
                    parts = [p.strip() for p in line.split('|')]
                    parts = [p for p in parts if p]
                    if len(parts) >= 4 and not all(set(p) <= set('-: ') for p in parts):
                        skip_words = {'category','type','description','likelihood','impact',
                                      'mitigation','owner','---','top risks'}
                        if not any(p.lower() in skip_words for p in parts[:2]):
                            risk_rows.append(parts)

                html += (
                    "<div style='margin-bottom:1.5rem;'>"
                    "<div style='display:flex;align-items:center;gap:0.5rem;"
                    "margin-bottom:0.75rem;padding-bottom:0.4rem;"
                    "border-bottom:2px solid rgba(224,92,92,0.3);'>"
                    "<span style='font-size:1rem;'>⚠️</span>"
                    "<span style='font-size:12px;font-weight:700;color:#e05c5c;"
                    "text-transform:uppercase;letter-spacing:0.1em;'>A. Top Risks</span>"
                    f"<span style='font-size:10px;color:rgba(255,255,255,0.3);"
                    f"margin-left:auto;'>{len(risk_rows)} risks identified</span>"
                    "</div>"
                )

                if risk_rows:
                    for r in risk_rows:
                        while len(r) < 7: r.append("—")
                        cat  = r[0].strip().title()
                        rtype = r[1].strip()
                        desc  = r[2].strip()
                        lh    = r[3].strip().title()
                        im    = r[4].strip().title()
                        mit   = r[5].strip()
                        owner = r[6].strip()
                        cbg   = cat_bg.get(cat, "rgba(255,255,255,0.04)")
                        ccol  = cat_col.get(cat, "rgba(255,255,255,0.5)")
                        lbg   = lh_bg.get(lh, lh_bg["—"])
                        lcol  = lh_col.get(lh, lh_col["—"])
                        ibg   = lh_bg.get(im, lh_bg["—"])
                        icol  = lh_col.get(im, lh_col["—"])

                        html += (
                            f"<div style='background:{lbg};border:1px solid {lcol}22;"
                            f"border-left:3px solid {lcol};border-radius:8px;"
                            f"padding:0.75rem 1rem;margin-bottom:0.5rem;'>"

                            # Header row: category badge + type + likelihood + impact badges
                            f"<div style='display:flex;align-items:flex-start;"
                            f"gap:0.4rem;margin-bottom:0.4rem;flex-wrap:wrap;'>"
                            f"<span style='background:{cbg};border:1px solid {ccol}44;"
                            f"color:{ccol};font-size:9px;font-weight:700;padding:0.1rem 0.5rem;"
                            f"border-radius:4px;text-transform:uppercase;'>{cat}</span>"
                            f"<span style='background:rgba(255,255,255,0.06);border:1px solid "
                            f"rgba(255,255,255,0.1);color:rgba(255,255,255,0.5);font-size:9px;"
                            f"font-weight:600;padding:0.1rem 0.5rem;border-radius:4px;'>{rtype}</span>"
                            f"<span style='margin-left:auto;display:flex;gap:0.3rem;'>"
                            f"<span style='background:{lh_bg.get(lh)};color:{lh_col.get(lh)};"
                            f"border:1px solid {lh_col.get(lh)}44;font-size:9px;font-weight:700;"
                            f"padding:0.1rem 0.45rem;border-radius:4px;text-transform:uppercase;'>L: {lh}</span>"
                            f"<span style='background:{ibg};color:{icol};"
                            f"border:1px solid {icol}44;font-size:9px;font-weight:700;"
                            f"padding:0.1rem 0.45rem;border-radius:4px;text-transform:uppercase;'>I: {im}</span>"
                            f"</span></div>"

                            # Description
                            f"<div style='font-size:12px;font-weight:600;"
                            f"color:rgba(255,255,255,0.88);margin-bottom:0.35rem;'>{desc}</div>"

                            # Mitigation + Owner
                            f"<div style='display:flex;gap:1rem;flex-wrap:wrap;'>"
                            f"<div style='font-size:11px;color:rgba(255,255,255,0.55);flex:1;'>"
                            f"🛡️ <span style='color:rgba(255,255,255,0.72);'>{mit}</span></div>"
                            f"<div style='font-size:10px;color:rgba(255,255,255,0.3);white-space:nowrap;'>"
                            f"👤 {owner}</div>"
                            f"</div>"
                            f"</div>"
                        )
                else:
                    html += "<div style='color:rgba(255,255,255,0.35);font-size:12px;'>No risk data parsed.</div>"

                html += "</div>"

                # ── B. TEST PATTERNS ──────────────────────────
                b_content = sections.get("B", ("", ""))[0] if "B" in sections else ""
                if not b_content:
                    m = re.search(r'(?:###?\s*B\.|B\.\s+TEST PATTERNS)(.*?)(?=###?\s*C\.|C\.\s+TIMELINE|\Z)',
                                  text, re.DOTALL | re.IGNORECASE)
                    if m: b_content = m.group(1).strip()

                test_rows = []
                for line in b_content.split('\n'):
                    parts = [p.strip() for p in line.split('|')]
                    parts = [p for p in parts if p]
                    if len(parts) >= 4 and not all(set(p) <= set('-: ') for p in parts):
                        skip_words = {'id','priority','type','scenario','preconditions',
                                      'steps','expected result','---','test patterns'}
                        if not any(p.lower() in skip_words for p in parts[:2]):
                            test_rows.append(parts)

                pri_col = {"High":"#e05c5c","Medium":"#f0a500","Low":"#4caf78"}
                pri_bg  = {"High":"rgba(224,92,92,0.12)","Medium":"rgba(240,165,0,0.1)","Low":"rgba(76,175,120,0.1)"}
                typ_col = {"Integration":"#5b8ce8","Data Quality":"#4caf78","Security":"#e05c5c",
                           "Performance":"#f0a500","Functional":"#a05be8","Regression":"#888888","UAT":"#4caf78"}
                typ_bg  = {"Integration":"rgba(91,140,232,0.1)","Data Quality":"rgba(76,175,120,0.1)",
                           "Security":"rgba(224,92,92,0.1)","Performance":"rgba(240,165,0,0.1)",
                           "Functional":"rgba(160,91,232,0.1)","Regression":"rgba(128,128,128,0.1)",
                           "UAT":"rgba(76,175,120,0.1)"}

                html += (
                    "<div style='margin-bottom:1.5rem;'>"
                    "<div style='display:flex;align-items:center;gap:0.5rem;"
                    "margin-bottom:0.75rem;padding-bottom:0.4rem;"
                    "border-bottom:2px solid rgba(91,140,232,0.3);'>"
                    "<span style='font-size:1rem;'>🧪</span>"
                    "<span style='font-size:12px;font-weight:700;color:#5b8ce8;"
                    "text-transform:uppercase;letter-spacing:0.1em;'>B. Test Patterns</span>"
                    f"<span style='font-size:10px;color:rgba(255,255,255,0.3);"
                    f"margin-left:auto;'>{len(test_rows)} test cases</span>"
                    "</div>"
                )

                if test_rows:
                    # Render as a styled table
                    tc_headers = ["ID", "Priority", "Type", "Scenario", "Preconditions", "Steps", "Expected Result"]
                    th_cells = "".join(
                        f"<th style='background:#162840;color:#5b8ce8;padding:0.5rem 0.75rem;"
                        f"text-align:left;font-size:10px;font-weight:700;letter-spacing:0.06em;"
                        f"border:1px solid rgba(91,140,232,0.2);white-space:nowrap;'>{h}</th>"
                        for h in tc_headers
                    )
                    rows_html = ""
                    for i, r in enumerate(test_rows):
                        while len(r) < 7: r.append("")
                        r = r[:7]
                        row_bg = "#1a2e42" if i % 2 == 0 else "#152438"
                        pri    = r[1].strip().title()
                        ttype  = r[2].strip().title()
                        p_col  = pri_col.get(pri, "#888")
                        p_bg   = pri_bg.get(pri, "rgba(255,255,255,0.05)")
                        t_col  = typ_col.get(ttype, "#888")
                        t_bg   = typ_bg.get(ttype, "rgba(255,255,255,0.05)")
                        cells  = ""
                        for ci, cell in enumerate(r):
                            val = str(cell).strip()
                            if ci == 0:
                                style = (f"background:{row_bg};color:#5b8ce8;font-weight:700;"
                                         f"font-size:10px;border:1px solid rgba(91,140,232,0.15);"
                                         f"padding:0.4rem 0.6rem;white-space:nowrap;")
                            elif ci == 1:
                                style = (f"background:{p_bg};color:{p_col};font-weight:700;"
                                         f"font-size:10px;text-align:center;"
                                         f"border:1px solid rgba(91,140,232,0.15);padding:0.4rem 0.6rem;")
                            elif ci == 2:
                                style = (f"background:{t_bg};color:{t_col};font-weight:700;"
                                         f"font-size:10px;text-align:center;"
                                         f"border:1px solid rgba(91,140,232,0.15);padding:0.4rem 0.6rem;")
                            else:
                                style = (f"background:{row_bg};color:rgba(255,255,255,0.75);"
                                         f"font-size:11px;vertical-align:top;line-height:1.5;"
                                         f"border:1px solid rgba(91,140,232,0.15);padding:0.4rem 0.6rem;")
                            cells += f"<td style='{style}'>{val}</td>"
                        rows_html += f"<tr>{cells}</tr>"

                    html += (
                        "<div style='overflow-x:auto;border-radius:8px;"
                        "border:1px solid rgba(91,140,232,0.25);margin-bottom:0.5rem;'>"
                        "<table style='width:100%;border-collapse:collapse;'>"
                        f"<thead><tr>{th_cells}</tr></thead>"
                        f"<tbody>{rows_html}</tbody>"
                        "</table></div>"
                    )
                else:
                    html += "<div style='color:rgba(255,255,255,0.35);font-size:12px;'>No test pattern data parsed.</div>"

                html += "</div>"

                # ── C. TIMELINE LESSONS ───────────────────────
                c_content = sections.get("C", ("", ""))[0] if "C" in sections else ""
                if not c_content:
                    m = re.search(r'(?:###?\s*C\.|C\.\s+TIMELINE LESSONS)(.*?)(?=###?\s*D\.|D\.\s+TEAM|\Z)',
                                  text, re.DOTALL | re.IGNORECASE)
                    if m: c_content = m.group(1).strip()

                html += (
                    "<div style='margin-bottom:1.5rem;'>"
                    "<div style='display:flex;align-items:center;gap:0.5rem;"
                    "margin-bottom:0.75rem;padding-bottom:0.4rem;"
                    "border-bottom:2px solid rgba(240,165,0,0.3);'>"
                    "<span style='font-size:1rem;'>📅</span>"
                    "<span style='font-size:12px;font-weight:700;color:#f0a500;"
                    "text-transform:uppercase;letter-spacing:0.1em;'>C. Timeline Lessons</span>"
                    "</div>"
                )

                lesson_lines = [l.strip() for l in c_content.split('\n') if l.strip()]
                if lesson_lines:
                    for lesson in lesson_lines:
                        content = re.sub(r'^[-•*\d\.]+\s*', '', lesson).strip()
                        if not content:
                            continue
                        html += (
                            f"<div style='display:flex;gap:0.65rem;padding:0.5rem 0.75rem;"
                            f"margin-bottom:0.4rem;background:rgba(240,165,0,0.07);"
                            f"border-left:3px solid rgba(240,165,0,0.5);"
                            f"border-radius:0 6px 6px 0;'>"
                            f"<span style='color:#f0a500;font-size:0.85rem;flex-shrink:0;"
                            f"margin-top:0.05rem;'>⏱</span>"
                            f"<span style='font-size:12px;color:rgba(255,255,255,0.78);"
                            f"line-height:1.65;'>{content}</span>"
                            f"</div>"
                        )
                else:
                    html += "<div style='color:rgba(255,255,255,0.35);font-size:12px;'>No timeline lessons parsed.</div>"

                html += "</div>"

                # ── D. TEAM STRUCTURE ─────────────────────────
                d_content = sections.get("D", ("", ""))[0] if "D" in sections else ""
                if not d_content:
                    m = re.search(r'(?:###?\s*D\.|D\.\s+TEAM STRUCTURE)(.*?)$',
                                  text, re.DOTALL | re.IGNORECASE)
                    if m: d_content = m.group(1).strip()

                html += (
                    "<div style='margin-bottom:1rem;'>"
                    "<div style='display:flex;align-items:center;gap:0.5rem;"
                    "margin-bottom:0.75rem;padding-bottom:0.4rem;"
                    "border-bottom:2px solid rgba(76,175,120,0.3);'>"
                    "<span style='font-size:1rem;'>👥</span>"
                    "<span style='font-size:12px;font-weight:700;color:#4caf78;"
                    "text-transform:uppercase;letter-spacing:0.1em;'>D. Team Structure</span>"
                    "</div>"
                )

                role_rows = []
                prose_lines = []
                for line in d_content.split('\n'):
                    s = line.strip()
                    if not s:
                        continue
                    if '|' in s:
                        parts = [p.strip() for p in s.split('|') if p.strip()]
                        if len(parts) >= 2 and not all(set(p) <= set('-: ') for p in parts):
                            skip_h = {'role title','headcount','key responsibilities',
                                      'skills required','role','responsibilities','---'}
                            if not any(p.lower() in skip_h for p in parts[:2]):
                                role_rows.append(parts)
                    else:
                        prose_lines.append(s)

                if role_rows:
                    role_headers = ["Role Title", "Headcount", "Key Responsibilities", "Skills Required"]
                    th_r = "".join(
                        f"<th style='background:#162818;color:#4caf78;padding:0.5rem 0.75rem;"
                        f"text-align:left;font-size:10px;font-weight:700;letter-spacing:0.06em;"
                        f"border:1px solid rgba(76,175,120,0.2);white-space:nowrap;'>{h}</th>"
                        for h in role_headers
                    )
                    rows_r = ""
                    for i, r in enumerate(role_rows):
                        while len(r) < 4: r.append("")
                        r = r[:4]
                        rbg = "#1a2e1f" if i % 2 == 0 else "#152318"
                        cells_r = ""
                        for ci, cell in enumerate(r):
                            if ci == 0:
                                style_r = (f"background:{rbg};color:#4caf78;font-weight:700;"
                                           f"font-size:11px;border:1px solid rgba(76,175,120,0.15);"
                                           f"padding:0.5rem 0.75rem;white-space:nowrap;")
                            elif ci == 1:
                                style_r = (f"background:{rbg};color:rgba(255,255,255,0.85);"
                                           f"font-weight:700;font-size:11px;text-align:center;"
                                           f"border:1px solid rgba(76,175,120,0.15);padding:0.5rem 0.75rem;")
                            else:
                                style_r = (f"background:{rbg};color:rgba(255,255,255,0.72);"
                                           f"font-size:11px;vertical-align:top;line-height:1.5;"
                                           f"border:1px solid rgba(76,175,120,0.15);padding:0.5rem 0.75rem;")
                            cells_r += f"<td style='{style_r}'>{cell.strip()}</td>"
                        rows_r += f"<tr>{cells_r}</tr>"

                    html += (
                        "<div style='overflow-x:auto;border-radius:8px;"
                        "border:1px solid rgba(76,175,120,0.25);'>"
                        "<table style='width:100%;border-collapse:collapse;'>"
                        f"<thead><tr>{th_r}</tr></thead>"
                        f"<tbody>{rows_r}</tbody>"
                        "</table></div>"
                    )
                elif prose_lines:
                    for pl in prose_lines:
                        content = re.sub(r'^[-•*\d\.]+\s*', '', pl).strip()
                        if content:
                            html += (
                                f"<div style='display:flex;gap:0.5rem;padding:0.25rem 0;"
                                f"color:rgba(255,255,255,0.75);font-size:12px;line-height:1.65;'>"
                                f"<span style='color:#4caf78;flex-shrink:0;'>▸</span>"
                                f"<span>{content}</span></div>"
                            )
                else:
                    html += "<div style='color:rgba(255,255,255,0.35);font-size:12px;'>No team structure data parsed.</div>"

                html += "</div>"
                return html

            st.markdown(
                "<div style='background:#112218;border:1px solid rgba(76,175,120,0.18);"
                "border-radius:12px;padding:1.5rem 1.75rem;'>"
                + render_patterns(past_p) +
                "</div>",
                unsafe_allow_html=True
            )

    # ── TAB 4: Q&A ─────────────────────────────────────────────
    with tab_qa:
        st.markdown("<h2>💬 Ask Cortex About This Plan</h2>", unsafe_allow_html=True)
        st.markdown(
            "<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:0.75rem;'>"
            "Cortex answers using the full plan, SOW analysis, and WBS. "
            "Ask anything — risks, tasks, emails, timelines, RACI."
            "</div>", unsafe_allow_html=True
        )

        suggestions = [
            "What are the top 3 risks for the steering committee?",
            "Write a project kick-off email to the team.",
            "What should Sprint 1 look like in detail?",
            "Generate a RACI matrix for this project.",
            "What compliance tasks are needed?",
            "Rewrite the executive summary for a non-technical audience.",
            "What is the critical path?",
            "Suggest mitigation for the highest impact risks.",
        ]
        sc1, sc2, sc3 = st.columns(3)
        for i, s in enumerate(suggestions):
            col = [sc1, sc2, sc3][i % 3]
            with col:
                if st.button(s, key=f"sugg_{i}"):
                    st.session_state['pending_question'] = s

        st.markdown("<hr>", unsafe_allow_html=True)

        if 'qa_input_value' not in st.session_state:
            st.session_state['qa_input_value'] = ''

        if st.session_state.get('pending_question'):
            st.session_state['qa_input_value'] = st.session_state.pop('pending_question')

        user_q = st.text_input(
            "Your question:",
            value=st.session_state['qa_input_value'],
            placeholder="e.g. What dependencies could block Sprint 2?"
        )

        if st.button("🤖 Ask Cortex", key="ask_btn"):
            q = (user_q or '').strip()
            if not q:
                st.warning("Please type a question first.")
            else:
                sow_ctx = (st.session_state.sow_requirements or "")[:600]
                wbs_ctx = "\n".join([
                    s.get('name','') + " (" + s.get('weeks','') + "): " + s.get('goal','')
                    for s in (st.session_state.wbs_structured or [])
                ])[:500]
                qa_prompt = (
                    "You are a senior PMO consultant. Answer the question using the project context.\n\n"
                    "Project: " + plan_name + "\n"
                    "SOW Summary:\n" + sow_ctx + "\n\n"
                    "WBS Summary:\n" + wbs_ctx + "\n\n"
                    "Plan (excerpt):\n" + plan[:1800] + "\n\n"
                    "Question: " + q + "\n\n"
                    "Answer specifically and practically. "
                    "If the question asks for a document or template, produce it fully."
                )
                with st.spinner("Cortex is thinking…"):
                    ans = cortex_call(qa_prompt, "Q&A")
                if ans:
                    st.session_state.qa_history.append({"q": q, "a": ans})
                    st.session_state['qa_input_value'] = ''
                    st.rerun()
                else:
                    st.error("No response — check Cortex connection.")

        # ── Conversation History — structured render ──────────
        if st.session_state.qa_history:
            st.markdown(
                "<div style='font-size:10px;font-weight:700;color:rgba(255,255,255,0.3);"
                "text-transform:uppercase;letter-spacing:0.12em;margin:0.75rem 0 0.5rem;padding-bottom:0.35rem;"
                "border-bottom:1px solid rgba(255,255,255,0.06);'>💬 Conversation History</div>",
                unsafe_allow_html=True
            )

            def render_qa_answer(text):
                """Render a Cortex answer with proper structure — headings, bullets, numbered lists, tables, prose."""
                lines  = text.split('\n')
                out    = ""
                for line in lines:
                    s = line.strip()
                    if not s:
                        out += "<div style='height:0.4rem;'></div>"
                        continue
                    if s.startswith('###'):
                        t = s.lstrip('#').strip()
                        out += ("<div style='font-size:11px;font-weight:700;color:#5b8ce8;"
                                "text-transform:uppercase;letter-spacing:0.08em;"
                                "margin:0.8rem 0 0.3rem;padding-bottom:0.2rem;"
                                "border-bottom:1px solid rgba(91,140,232,0.25);'>%s</div>" % t)
                    elif s.startswith('##'):
                        t = s.lstrip('#').strip()
                        out += ("<div style='font-size:12px;font-weight:700;color:#4caf78;"
                                "text-transform:uppercase;letter-spacing:0.08em;"
                                "margin:0.8rem 0 0.3rem;padding:0.3rem 0.6rem;"
                                "background:rgba(76,175,120,0.07);border-left:2px solid #4caf78;"
                                "border-radius:0 4px 4px 0;'>%s</div>" % t)
                    elif s.startswith('#'):
                        t = s.lstrip('#').strip()
                        out += ("<div style='font-size:13px;font-weight:800;color:#4caf78;"
                                "margin:0.9rem 0 0.35rem;'>%s</div>" % t)
                    elif s.startswith('- ') or s.startswith('* ') or s.startswith('• '):
                        c = re.sub(r'^[-*•]\s*', '', s)
                        out += ("<div style='display:flex;gap:0.5rem;padding:0.2rem 0;"
                                "color:rgba(255,255,255,0.8);font-size:12px;line-height:1.65;'>"
                                "<span style='color:#4caf78;flex-shrink:0;font-size:0.7rem;"
                                "margin-top:0.2rem;'>▸</span><span>%s</span></div>" % c)
                    elif re.match(r'^\d+\.\s', s):
                        m2 = re.match(r'^(\d+)\.\s*(.*)', s)
                        if m2:
                            out += ("<div style='display:flex;gap:0.5rem;padding:0.2rem 0;"
                                    "color:rgba(255,255,255,0.8);font-size:12px;line-height:1.65;'>"
                                    "<span style='color:#4caf78;font-weight:700;flex-shrink:0;"
                                    "min-width:1.4rem;'>%s.</span><span>%s</span></div>"
                                    % (m2.group(1), m2.group(2)))
                    elif '|' in s:
                        # Mini inline table
                        parts = [p.strip() for p in s.split('|') if p.strip()]
                        if len(parts) >= 2 and not all(set(p) <= set('-: ') for p in parts):
                            cells = "".join(
                                "<td style='padding:0.35rem 0.65rem;font-size:11px;"
                                "color:rgba(255,255,255,0.75);border:1px solid rgba(255,255,255,0.06);"
                                "background:rgba(255,255,255,0.03);vertical-align:top;'>%s</td>" % p
                                for p in parts
                            )
                            out += ("<div style='overflow-x:auto;margin:0.3rem 0;'>"
                                    "<table style='border-collapse:collapse;font-size:11px;'>"
                                    "<tr>%s</tr></table></div>" % cells)
                        else:
                            out += "<div style='color:rgba(255,255,255,0.72);font-size:12px;line-height:1.8;'>%s</div>" % s
                    elif s.startswith('**') and s.endswith('**'):
                        t = s.strip('*')
                        out += "<div style='color:rgba(255,255,255,0.9);font-size:12px;font-weight:700;padding:0.1rem 0;'>%s</div>" % t
                    else:
                        out += "<div style='color:rgba(255,255,255,0.72);font-size:12px;line-height:1.85;padding:0.05rem 0;'>%s</div>" % s
                return out

            for qa in reversed(st.session_state.qa_history):
                # ── Question bubble ──
                st.markdown(
                    "<div style='background:rgba(76,175,120,0.08);border:1px solid rgba(76,175,120,0.25);"
                    "border-left:3px solid #4caf78;border-radius:8px;"
                    "padding:0.65rem 1rem;margin-bottom:0.3rem;'>"
                    "<div style='font-size:9px;font-weight:700;color:rgba(76,175,120,0.55);"
                    "text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.2rem;'>You asked</div>"
                    "<div style='font-size:12px;color:#f0f5f2;font-weight:600;'>❓ " + qa['q'] + "</div>"
                    "</div>",
                    unsafe_allow_html=True
                )
                # ── Cortex Answered bubble — fully structured ──
                st.markdown(
                    "<div style='background:#172a1e;border:1px solid rgba(255,255,255,0.07);"
                    "border-left:3px solid #5b8ce8;border-radius:0 8px 8px 8px;"
                    "padding:0.85rem 1.1rem;margin-bottom:1rem;'>"
                    "<div style='font-size:9px;font-weight:700;color:rgba(91,140,232,0.6);"
                    "text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.5rem;'>"
                    "🤖 Cortex answered</div>"
                    + render_qa_answer(qa['a']) +
                    "</div>",
                    unsafe_allow_html=True
                )
            if st.button("🗑️ Clear History", key="clear_qa"):
                st.session_state.qa_history = []
                st.rerun()

    # ── TAB 5: Risk Analyser ───────────────────────────────────
    with tab_risks:
        st.markdown("<h2>⚠️ Risk Analyser</h2>", unsafe_allow_html=True)
        if st.button("🔍 Extract & Score Risks with Cortex"):
            rp = (
                f"Extract all risks from this plan for \"{plan_name}\".\n"
                f"Plan:\n{plan[:2500]}\n\n"
                f"Return ONLY valid JSON array:\n"
                f'[{{"risk":"title","type":"Technical/Resource/Timeline/Budget/Compliance",'
                f'"likelihood":"High/Medium/Low","impact":"High/Medium/Low",'
                f'"mitigation":"one sentence","owner":"role"}}]'
            )
            with st.spinner(""):
                raw = cortex_call(rp, "Risk Analyser")
                try:
                    raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
                    risks = json.loads(raw)
                except Exception:
                    risks = [
                        {"risk":"Data access permissions","type":"Technical","likelihood":"High","impact":"High","mitigation":"Pre-provision IAM roles before Sprint 1","owner":"Platform Engineer"},
                        {"risk":"Scope creep","type":"Timeline","likelihood":"Medium","impact":"High","mitigation":"Lock scope after Sprint 2","owner":"Project Manager"},
                        {"risk":"Key person dependency","type":"Resource","likelihood":"Medium","impact":"Medium","mitigation":"Cross-train 2 members per component","owner":"Tech Lead"},
                    ]

            lbg = {"High":"rgba(224,92,92,0.12)","Medium":"rgba(240,165,0,0.1)","Low":"rgba(76,175,120,0.1)"}
            lco = {"High":"rgba(224,92,92,1)","Medium":"rgba(240,165,0,1)","Low":"#4caf78"}
            for r in risks:
                lh = r.get('likelihood','Medium'); im = r.get('impact','Medium')
                bg = lbg.get(lh,lbg['Medium']); co = lco.get(lh,lco['Medium'])
                st.markdown(f"""<div style='background:{bg};border:1px solid {co}33;border-radius:var(--radius-md);padding:0.85rem 1rem;margin-bottom:0.5rem;'>
                    <div style='display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:0.3rem;'>
                        <div style='font-weight:700;font-size:12px;color:var(--white-90);'>{r.get('risk','')}</div>
                        <div style='display:flex;gap:0.35rem;flex-shrink:0;margin-left:0.5rem;'>
                            <span style='background:{lbg.get(lh)};color:{lco.get(lh)};border:1px solid {lco.get(lh)}44;font-size:9px;font-weight:700;padding:0.15rem 0.5rem;border-radius:4px;text-transform:uppercase;'>L:{lh}</span>
                            <span style='background:{lbg.get(im)};color:{lco.get(im)};border:1px solid {lco.get(im)}44;font-size:9px;font-weight:700;padding:0.15rem 0.5rem;border-radius:4px;text-transform:uppercase;'>I:{im}</span>
                            <span style='background:rgba(255,255,255,0.07);color:rgba(255,255,255,0.5);border:1px solid var(--border);font-size:9px;font-weight:700;padding:0.15rem 0.5rem;border-radius:4px;'>{r.get('type','')}</span>
                        </div>
                    </div>
                    <div style='font-size:11px;color:rgba(255,255,255,0.6);margin-bottom:0.2rem;'>🛡️ {r.get('mitigation','')}</div>
                    <div style='font-size:10px;color:rgba(255,255,255,0.3);'>Owner: {r.get('owner','')}</div>
                </div>""", unsafe_allow_html=True)

    # ── TAB 6: Export ──────────────────────────────────────────
    with tab_export:
        st.markdown("<h2>📤 Export & Save</h2>", unsafe_allow_html=True)

        ec1, ec2, ec3 = st.columns(3)

        with ec1:
            st.markdown("<div class='plan-section-title'>💾 Save to Snowflake</div>", unsafe_allow_html=True)
            save_status = st.selectbox("Status", ["In Progress","Planning","On Hold"])

            # ── Review confirmation gate ───────────────────────
            st.markdown(
                "<div style='background:rgba(240,165,0,0.08);border:1px solid rgba(240,165,0,0.3);"
                "border-left:3px solid #f0a500;border-radius:8px;padding:0.7rem 0.9rem;"
                "margin-bottom:0.6rem;'>"
                "<div style='font-size:10px;font-weight:700;color:#f0a500;text-transform:uppercase;"
                "letter-spacing:0.08em;margin-bottom:0.35rem;'>⚠️ Review Gate</div>"
                "<div style='font-size:11px;color:rgba(255,255,255,0.75);'>"
                "Has the Project Plan been reviewed and approved before saving?</div>"
                "</div>",
                unsafe_allow_html=True
            )
            review_confirmed = st.radio(
                "Plan Reviewed?",
                options=["— Select —", "✅ Yes — Plan is reviewed, save it", "❌ No — Not yet reviewed"],
                index=0,
                key="save_review_gate",
                label_visibility="collapsed"
            )

            if review_confirmed == "✅ Yes — Plan is reviewed, save it":
                if st.button("💾 Save to Projects Table", key="save_btn"):
                    try:
                        esc = (plan or '').replace("'","''")
                        pn  = plan_name.replace("'","''")
                        # Save full plan text as wbs_summary + deployment_plan for future reference
                        wbs_part  = esc[:3000]
                        depl_part = esc[3000:5000] if len(esc) > 3000 else ''
                        session.sql(
                            f"INSERT INTO projects "
                            f"(project_name, description, status, wbs_summary, "
                            f"risk_log_summary, test_cases_summary, deployment_plan) "
                            f"VALUES('{pn}', 'AI-generated plan — reviewed & approved', "
                            f"'{save_status}', '{wbs_part}', '', '', '{depl_part}')"
                        ).collect()
                        st.success("✅ Project saved to DB — will inform future plans.")
                        st.session_state.past_projects_df = load_past_projects()
                        st.session_state.imported_csv_names.add(plan_name)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Save failed: {e}")
            elif review_confirmed == "❌ No — Not yet reviewed":
                st.markdown(
                    "<div style='background:rgba(224,92,92,0.1);border:1px solid rgba(224,92,92,0.3);"
                    "border-radius:8px;padding:0.6rem 0.9rem;margin-top:0.3rem;'>"
                    "<div style='font-size:11px;color:#e05c5c;font-weight:600;'>🚫 Save blocked.</div>"
                    "<div style='font-size:11px;color:rgba(255,255,255,0.6);margin-top:0.2rem;'>"
                    "Please review the plan with your team before saving to the database.</div>"
                    "</div>",
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    "<div style='font-size:11px;color:rgba(255,255,255,0.3);margin-top:0.3rem;'>"
                    "Select Yes or No above to proceed.</div>",
                    unsafe_allow_html=True
                )

        with ec2:
            st.markdown("<div class='plan-section-title'>📄 Download Text</div>", unsafe_allow_html=True)
            st.download_button("⬇ Download .txt", data=plan or "",
                               file_name=f"{plan_name.replace(' ','_')}_plan.txt", mime="text/plain")

        with ec3:
            st.markdown("<div class='plan-section-title'>📊 Download Excel</div>", unsafe_allow_html=True)
            st.markdown(
                "<div style='font-size:11px;color:rgba(255,255,255,0.4);margin-bottom:0.5rem;'>"
                "5 sheets: Summary · WBS/Sprints · RAID Log · Test Cases · Timeline</div>",
                unsafe_allow_html=True
            )
            if st.button("📎 Build Excel Workbook"):
                with st.spinner("Building Excel..."):
                    sp = sprints if sprints else parse_wbs_into_sprints(plan, duration_weeks, meth)
                    excel_bytes = build_excel(plan, plan_name, sp, meth)
                st.download_button(
                    label="⬇ Download .xlsx",
                    data=excel_bytes,
                    file_name=f"{plan_name.replace(' ','_')}_ProjectPlan.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        # ── FIX: Preview plan text with proper monospace rendering ──
        with st.expander("🔍 Preview plan text"):
            safe_plan = (
                (plan or "")
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            st.markdown(
                "<div class='plan-preview-wrap'>"
                "<pre>" + safe_plan + "</pre>"
                "</div>",
                unsafe_allow_html=True
            )

elif not generate_button:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("""<div style="text-align:center;padding:3rem 1rem;">
        <div style="font-size:2rem;margin-bottom:0.75rem;">📊</div>
        <div style="font-size:14px;font-weight:700;color:rgba(255,255,255,0.65);margin-bottom:0.5rem;">Ready to generate your project plan</div>
        <div style="font-size:12px;color:rgba(255,255,255,0.28);max-width:420px;margin:0 auto;line-height:1.75;">
            Upload your SOW · Fill in project details · Click <strong style="color:#4caf78;">Generate Project Plan</strong><br/><br/>
            Cortex will scan institutional memory and produce:<br/>
            WBS · Sprint Board · RAID Log · Test Cases · Timeline · Deployment Strategy
        </div>
    </div>""", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("""<div class='footer'>
    Powered by <span>Snowflake Cortex</span> &nbsp;·&nbsp;
    PMO Intelligence &nbsp;·&nbsp; Institutional Memory → Faster Delivery
</div>""", unsafe_allow_html=True)
