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

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600;700&display=swap');

    :root {
        /* Main dark theme */
        --forest:#1a3a2a; --forest-deep:#0e1e14; --forest-mid:#1f4633;
        --forest-muted:#1c3628; --green-accent:#3d8c5e; --green-bright:#4caf78;
        --green-glow:rgba(76,175,120,0.18); --green-border:rgba(76,175,120,0.25);
        --white:#ffffff; --white-90:rgba(255,255,255,0.90); --white-60:rgba(255,255,255,0.60);
        --white-06:rgba(255,255,255,0.05); --black:#060d08; --black-soft:#0a1410;
        --border:rgba(255,255,255,0.07); --border-green:rgba(76,175,120,0.22);
        --text-primary:#eef4f0; --text-secondary:rgba(255,255,255,0.50); --text-muted:rgba(255,255,255,0.26);
        --radius-sm:6px; --radius-md:10px; --radius-lg:14px;
        --amber:rgba(240,165,0,1); --red:rgba(224,92,92,1);

        /* Sidebar light theme */
        --sb-bg1: #f0faf4;
        --sb-bg2: #dff0e8;
        --sb-text: #1a3a2a;
        --sb-text-muted: #4a7a5e;
        --sb-accent: #2d7a50;
        --sb-accent-bright: #3d8c5e;
        --sb-border: rgba(45,122,80,0.2);
        --sb-card: rgba(255,255,255,0.8);
    }

    html,body,[class*="css"],.stApp,div,p,span,label,input,textarea,select,button {
        font-family:'DM Sans',sans-serif !important; font-size:13px !important;
    }

    .stApp { background: var(--forest-deep) !important; color:var(--text-primary) !important; }
    .stApp::after {
        content:''; position:fixed; inset:0; pointer-events:none; z-index:0;
        background-image:
            radial-gradient(ellipse at 20% 20%, rgba(61,140,94,0.06) 0%, transparent 60%),
            radial-gradient(ellipse at 80% 80%, rgba(76,175,120,0.04) 0%, transparent 50%),
            linear-gradient(rgba(76,175,120,0.025) 1px, transparent 1px),
            linear-gradient(90deg, rgba(76,175,120,0.025) 1px, transparent 1px);
        background-size: 100% 100%, 100% 100%, 40px 40px, 40px 40px;
    }

    .stApp > header { display:none !important; }
    #root > div:first-child { padding-top:0 !important; }
    .block-container { padding-top:0.75rem !important; padding-bottom:2rem !important; max-width:100% !important; }

    /* ════════════════════════════════════════
       SIDEBAR — Beautiful Light Theme
       ════════════════════════════════════════ */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f5fdf8 0%, #eaf7ef 35%, #dff0e8 100%) !important;
        border-right: 1px solid rgba(45,122,80,0.18) !important;
        box-shadow: 4px 0 32px rgba(0,0,0,0.1) !important;
    }
    [data-testid="stSidebar"] > div:first-child { padding-top: 0 !important; }

    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stMarkdown p {
        color: #2d5a3a !important;
        font-size: 11px !important;
        text-transform: none !important;
        letter-spacing: 0 !important;
        font-weight: 500 !important;
    }
    [data-testid="stSidebar"] h3 {
        font-size: 9px !important;
        font-weight: 800 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.18em !important;
        color: var(--sb-accent) !important;
        margin-bottom: 0.6rem !important;
        padding-bottom: 0.4rem !important;
        border-bottom: 2px solid rgba(45,122,80,0.2) !important;
    }
    [data-testid="stSidebar"] .stCheckbox label p,
    [data-testid="stSidebar"] .stCheckbox span,
    [data-testid="stSidebar"] .stCheckbox label {
        color: #1a3a2a !important;
        font-size: 12px !important;
        font-weight: 500 !important;
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stSlider label,
    [data-testid="stSidebar"] .stTextInput label,
    [data-testid="stSidebar"] .stTextArea label,
    [data-testid="stSidebar"] .stFileUploader label {
        color: #3a6a4a !important;
        font-weight: 600 !important;
    }
    [data-testid="stSidebar"] .stTextInput input,
    [data-testid="stSidebar"] .stTextArea textarea {
        background: rgba(255,255,255,0.9) !important;
        border: 1.5px solid rgba(45,122,80,0.22) !important;
        border-radius: 8px !important;
        color: #1a3a2a !important;
        font-size: 12px !important;
        box-shadow: 0 1px 4px rgba(0,0,0,0.05) !important;
    }
    [data-testid="stSidebar"] .stTextInput input:focus,
    [data-testid="stSidebar"] .stTextArea textarea:focus {
        border-color: #3d8c5e !important;
        box-shadow: 0 0 0 3px rgba(61,140,94,0.15) !important;
        outline: none !important;
    }
    [data-testid="stSidebar"] .stSelectbox > div > div,
    [data-testid="stSidebar"] .stMultiSelect > div > div {
        background: rgba(255,255,255,0.9) !important;
        border: 1.5px solid rgba(45,122,80,0.22) !important;
        border-radius: 8px !important;
        color: #1a3a2a !important;
    }
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] span,
    [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] div {
        color: #1a3a2a !important;
    }
    [data-testid="stSidebar"] .stSlider > div > div > div { background: #3d8c5e !important; }
    [data-testid="stSidebar"] .stAlert {
        background: rgba(61,140,94,0.08) !important;
        border-color: rgba(61,140,94,0.35) !important;
        color: #1a4a2a !important;
    }

    /* Sidebar button - gradient pop */
    [data-testid="stSidebar"] .stButton > button {
        background: linear-gradient(135deg, #1f5c3a 0%, #2d7a50 50%, #3d8c5e 100%) !important;
        color: #fff !important;
        font-weight: 800 !important;
        font-size: 11px !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
        border: none !important;
        border-radius: 10px !important;
        padding: 0.7rem 1.2rem !important;
        box-shadow: 0 4px 20px rgba(45,122,80,0.35), 0 1px 4px rgba(0,0,0,0.1) !important;
        transition: all 0.22s ease !important;
        width: 100% !important;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: linear-gradient(135deg, #2d7a50 0%, #4caf78 100%) !important;
        box-shadow: 0 6px 28px rgba(61,140,94,0.5), 0 2px 8px rgba(0,0,0,0.15) !important;
        transform: translateY(-2px) !important;
    }

    /* Sidebar card blocks */
    .sb-card {
        background: rgba(255,255,255,0.78);
        border: 1.5px solid rgba(45,122,80,0.16);
        border-radius: 12px;
        padding: 0.9rem 1rem;
        margin-bottom: 0.7rem;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05), 0 1px 3px rgba(0,0,0,0.03);
        transition: box-shadow 0.2s, border-color 0.2s;
    }
    .sb-card:hover {
        border-color: rgba(61,140,94,0.32) !important;
        box-shadow: 0 4px 20px rgba(61,140,94,0.1) !important;
    }

    /* Sidebar header banner */
    .sb-header {
        background: linear-gradient(135deg, #112218 0%, #1f4633 60%, #2d7a50 100%);
        padding: 1.2rem 1.1rem 1rem;
        margin-bottom: 0.75rem;
        position: relative;
        overflow: hidden;
    }
    .sb-header::after {
        content:'';
        position: absolute;
        top: -30px; right: -30px;
        width: 100px; height: 100px;
        background: radial-gradient(circle, rgba(76,175,120,0.25) 0%, transparent 70%);
        border-radius: 50%;
    }
    .sb-header-title {
        font-size: 14px !important; font-weight: 800 !important; color: #fff !important;
        letter-spacing: -0.01em; line-height: 1.2; margin-bottom: 0.2rem !important;
        text-transform: none !important;
    }
    .sb-header-sub {
        font-size: 10px !important; color: rgba(255,255,255,0.45) !important;
        font-weight: 500 !important; letter-spacing: 0.06em; text-transform: uppercase !important;
    }

    /* Health score widget */
    .health-widget {
        background: linear-gradient(135deg, #ffffff 0%, #f0faf4 100%);
        border: 1.5px solid rgba(45,122,80,0.22);
        border-radius: 12px;
        padding: 0.85rem 1rem;
        margin-bottom: 0.7rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.05);
    }
    .health-title {
        font-size: 9px !important; font-weight: 800 !important; color: #2d7a50 !important;
        text-transform: uppercase !important; letter-spacing: 0.16em !important;
        margin-bottom: 0.6rem !important; display: flex; align-items: center; gap: 0.35rem;
    }
    .health-row { margin-bottom: 0.4rem; }
    .health-row-label {
        display: flex; justify-content: space-between;
        font-size: 10px !important; font-weight: 600 !important; color: #2d5a3a !important;
        margin-bottom: 0.15rem; text-transform: none !important; letter-spacing: 0 !important;
    }
    .health-track {
        height: 5px; background: rgba(0,0,0,0.08); border-radius: 999px; overflow: hidden;
    }
    .health-fill { height: 100%; border-radius: 999px; }

    /* Effort estimator dark card */
    .effort-card {
        background: linear-gradient(135deg, #0e1e14 0%, #1a3a2a 100%);
        border: 1px solid rgba(76,175,120,0.2);
        border-radius: 12px;
        padding: 0.85rem 1rem;
        margin-bottom: 0.7rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.15);
    }
    .effort-title {
        font-size: 9px !important; font-weight: 800 !important; color: #4caf78 !important;
        text-transform: uppercase !important; letter-spacing: 0.16em !important;
        margin-bottom: 0.6rem !important;
    }
    .effort-row {
        display: flex; justify-content: space-between; align-items: center;
        padding: 0.25rem 0; border-bottom: 1px solid rgba(255,255,255,0.04);
        font-size: 11px !important;
    }
    .effort-row:last-child { border-bottom: none; padding-bottom: 0; }
    .effort-label { color: rgba(255,255,255,0.45) !important; font-weight: 500 !important; }
    .effort-value { color: #4caf78 !important; font-weight: 700 !important; }

    /* AI Tip card */
    .ai-tip-card {
        background: linear-gradient(135deg, #e8f7ee 0%, #d8f0e4 100%);
        border: 1.5px solid rgba(45,122,80,0.28);
        border-left: 4px solid #3d8c5e;
        border-radius: 0 12px 12px 0;
        padding: 0.8rem 0.9rem;
        margin-bottom: 0.7rem;
    }
    .ai-tip-header {
        font-size: 9px !important; font-weight: 800 !important; color: #2d7a50 !important;
        text-transform: uppercase !important; letter-spacing: 0.14em !important;
        margin-bottom: 0.3rem !important; display: flex; align-items: center; gap: 0.3rem;
    }
    .ai-tip-text {
        font-size: 11px !important; color: #1a3a2a !important; line-height: 1.6 !important;
        font-weight: 500 !important;
    }

    /* Methodology pill */
    .method-pill {
        display: inline-flex; align-items: center; gap: 0.4rem;
        background: rgba(45,122,80,0.12); border: 1.5px solid rgba(45,122,80,0.28);
        border-radius: 20px; padding: 0.28rem 0.75rem;
        font-size: 10px !important; font-weight: 700 !important; color: #1f5c3a !important;
        letter-spacing: 0.06em; text-transform: uppercase;
    }

    /* Role input on light bg */
    [data-testid="stSidebar"] .stNumberInput input {
        background: rgba(255,255,255,0.9) !important;
        border: 1.5px solid rgba(45,122,80,0.22) !important;
        color: #1a3a2a !important;
        border-radius: 8px !important;
    }

    /* Main content elements (unchanged dark theme) */
    .stSelectbox label, .stMultiSelect label { color: var(--text-primary) !important; }
    .stSelectbox [data-baseweb="select"] span,
    .stSelectbox [data-baseweb="select"] div { color: var(--text-primary) !important; }

    h1 { font-size:22px !important; font-weight:800 !important; color:var(--white) !important; margin:0 !important; }
    h2 { font-size:14px !important; font-weight:700 !important; color:var(--white-90) !important; margin:0 0 0.9rem 0 !important; }
    h3 { font-size:13px !important; font-weight:600 !important; color:var(--white-90) !important; margin-bottom:0.6rem !important; }

    [data-testid="stMetric"] {
        background:var(--forest-muted) !important; border-radius:var(--radius-md) !important;
        padding:1rem !important; border:1px solid var(--border) !important;
        position:relative !important; overflow:hidden !important;
        transition:border-color 0.2s,box-shadow 0.2s !important;
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
        position:sticky; top:0; z-index:999;
        background:rgba(14,30,20,0.94); backdrop-filter:blur(16px);
        border-bottom:1px solid rgba(76,175,120,0.12); padding:0.65rem 1.5rem;
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
                     background:linear-gradient(90deg,transparent,var(--green-bright),transparent); opacity:0.35; }

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

    .plan-section-header { font-size:12px !important; font-weight:700 !important; color:#4caf78 !important;
        text-transform:uppercase !important; letter-spacing:0.1em !important; margin:1.4rem 0 0.5rem 0 !important;
        padding-bottom:0.4rem !important; border-bottom:2px solid rgba(76,175,120,0.35) !important; display:block !important; }

    .plan-table-wrap { overflow-x:auto !important; margin:0.6rem 0 1.5rem 0 !important;
        border-radius:8px !important; border:1.5px solid rgba(76,175,120,0.35) !important;
        box-shadow:0 2px 16px rgba(0,0,0,0.35) !important; }
    .plan-table { width:100% !important; border-collapse:collapse !important; font-size:11.5px !important; }
    .plan-th { background:#0e2018 !important; color:#4caf78 !important; padding:0.6rem 1rem !important;
        text-align:left !important; font-size:10px !important; font-weight:800 !important;
        letter-spacing:0.09em !important; text-transform:uppercase !important;
        border-bottom:2px solid rgba(76,175,120,0.4) !important; border-right:1px solid rgba(255,255,255,0.05) !important; }
    .plan-td { padding:0.5rem 1rem !important; font-size:11px !important;
        border-bottom:1px solid rgba(255,255,255,0.05) !important; border-right:1px solid rgba(255,255,255,0.04) !important;
        vertical-align:top !important; line-height:1.6 !important; color:rgba(255,255,255,0.82) !important; }
    .plan-td-even { background:#152a1e !important; }
    .plan-td-odd  { background:#112218 !important; }

    .qa-question { background:rgba(76,175,120,0.1) !important; border:1px solid rgba(76,175,120,0.3) !important;
        border-left:3px solid #4caf78 !important; border-radius:8px !important;
        padding:0.7rem 1rem !important; margin-bottom:0.35rem !important;
        font-size:12px !important; color:#4caf78 !important; font-weight:600 !important; display:block !important; }
    .qa-answer { background:#1f3d2a !important; border:1px solid rgba(255,255,255,0.08) !important;
        border-radius:0 8px 8px 8px !important; padding:0.85rem 1rem !important;
        margin-bottom:1rem !important; font-size:12px !important; color:rgba(255,255,255,0.82) !important;
        line-height:1.75 !important; display:block !important; }

    .plan-preview-wrap { background:#0d1a11 !important; border:1px solid rgba(76,175,120,0.2) !important;
        border-radius:8px !important; padding:1rem 1.25rem !important; max-height:320px !important; overflow-y:auto !important; }
    .plan-preview-wrap pre { font-family:'JetBrains Mono','Courier New',monospace !important;
        font-size:11px !important; color:#e0f0e6 !important; line-height:1.75 !important;
        white-space:pre-wrap !important; word-break:break-word !important; margin:0 !important; background:transparent !important; }

    .csv-badge-imported { display:inline-block !important; background:rgba(76,175,120,0.15) !important;
        border:1px solid rgba(76,175,120,0.4) !important; color:#4caf78 !important;
        font-size:9px !important; font-weight:700 !important; padding:0.1rem 0.45rem !important;
        border-radius:4px !important; text-transform:uppercase !important; margin-left:0.4rem !important; }
    .csv-badge-pending { display:inline-block !important; background:rgba(240,165,0,0.1) !important;
        border:1px solid rgba(240,165,0,0.35) !important; color:rgba(240,165,0,0.9) !important;
        font-size:9px !important; font-weight:700 !important; padding:0.1rem 0.45rem !important;
        border-radius:4px !important; text-transform:uppercase !important; margin-left:0.4rem !important; }

    textarea:disabled, textarea[disabled] { color:#e0f0e6 !important; -webkit-text-fill-color:#e0f0e6 !important;
        opacity:1 !important; background:#0d1a11 !important; }
    .stTextArea textarea { color:var(--text-primary) !important; }

    .ai-output { background:var(--black-soft); border-left:3px solid var(--green-bright);
                 border:1px solid var(--border-green); border-radius:var(--radius-md);
                 padding:1.25rem 1.4rem; font-size:12px; line-height:1.85; color:var(--white-90); }

    .plan-section-title { font-size:11px; font-weight:700; color:#4caf78; text-transform:uppercase;
                         letter-spacing:0.1em; margin-bottom:0.5rem; }

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
             ("csv_imported", False), ("imported_csv_names", set())]:
    if k not in st.session_state:
        st.session_state[k] = v

def safe_str(val):
    return '' if pd.isna(val) else str(val)

def parse_date_safe(date_str):
    if not date_str: return ''
    try:
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(dt): return dt.strftime('%Y-%m-%d')
    except Exception: pass
    return ''

def load_past_projects():
    try:
        df = session.sql("SELECT * FROM projects").to_pandas()
        if df.empty:
            return pd.DataFrame(columns=['project_id','project_name','description',
                                         'wbs_summary','risk_log_summary','test_cases_summary','deployment_plan'])
        id_candidates = ['project_id','id','projectid','PROJECT_ID','PROJECTID']
        found_id = next((c for c in id_candidates if c in df.columns), None)
        if found_id: df.rename(columns={found_id: 'project_id'}, inplace=True)
        else: df['project_id'] = df.index + 1
        for col in ['project_name','description','wbs_summary','risk_log_summary','test_cases_summary','deployment_plan']:
            if col not in df.columns: df[col] = ''
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
                if pd.notna(val) and val: start_date = parse_date_safe(safe_str(val)); break
        if 'end_date' in col_map:
            for val in df[col_map['end_date']]:
                if pd.notna(val) and val: end_date = parse_date_safe(safe_str(val)); break
        return {"project_name": project_name,
                "description": f"Imported from {uploaded_file.name}. {len(df)} rows.",
                "wbs_summary": "\n".join(wbs_lines) or "No tasks found.",
                "start_date": start_date, "end_date": end_date, "technologies_used": ''}
    except Exception as e:
        st.error(f"Error parsing CSV: {e}")
        return None

def parse_wbs_into_sprints(plan_text, duration_weeks, methodology):
    """
    Extract real sprint/phase data from plan text.
    Strategy 1: Ask Cortex to structure the existing plan text into JSON.
    Strategy 2: Parse the DELIVERY TIMELINE pipe-delimited table directly from plan text.
    Strategy 3: Parse any sprint/phase headings + bullet tasks from plain text.
    Never falls back to dummy ["Define","Build","Test"] placeholders.
    """
    is_agile = any(k in methodology.lower() for k in ['agile','scrum','safe'])
    sprint_len = 2
    num_sprints = max(1, duration_weeks // sprint_len)
    unit = "Sprint" if is_agile else "Phase"

    # ── Strategy 1: Cortex JSON extraction from the real plan text ──
    prompt = (
        f"You are a PMO analyst. Read the project plan below and extract every sprint or phase "
        f"into a JSON array. Use the ACTUAL task names and deliverables from the plan — "
        f"do NOT invent generic placeholder text.\n\n"
        f"Plan text:\n{plan_text[:4000]}\n\n"
        f"Rules:\n"
        f"- Extract {num_sprints} {unit}s covering the full {duration_weeks}-week timeline.\n"
        f"- 'stories' must contain the REAL tasks from the plan (4-6 tasks per sprint).\n"
        f"- 'goal' must be the REAL sprint objective from the plan.\n"
        f"- 'deliverable' must be the REAL acceptance criterion or output.\n"
        f"- Copy task names verbatim from the plan — no paraphrasing, no placeholders.\n\n"
        f"Return ONLY a valid JSON array, no markdown fences, no extra text:\n"
        f'[{{"sprint_num":1,"name":"{unit} 1: <real title>","weeks":"Week 1-2",'
        f'"goal":"<real goal from plan>","stories":["<real task 1>","<real task 2>","<real task 3>","<real task 4>"],'
        f'"deliverable":"<real deliverable from plan>"}}]'
    )
    try:
        safe = prompt.replace("'", "''")
        raw  = session.sql(f"SELECT cortex_complete('{safe}') as r").collect()[0][0]
        # Strip any markdown fences
        raw = raw.strip()
        for fence in ["```json", "```JSON", "```"]:
            if raw.startswith(fence): raw = raw[len(fence):]
            if raw.endswith(fence):   raw = raw[:-len(fence)]
        raw = raw.strip()
        # Find JSON array bounds
        start = raw.find('['); end = raw.rfind(']')
        if start != -1 and end != -1: raw = raw[start:end+1]
        parsed = json.loads(raw)
        if isinstance(parsed, list) and len(parsed) > 0:
            # Validate: reject if stories are still generic placeholders
            first_stories = parsed[0].get('stories', [])
            placeholders = {'define','build','test','task 1','task 2','task 3','<real task 1>'}
            real_tasks = [s for s in first_stories if str(s).lower().strip() not in placeholders and len(str(s)) > 10]
            if real_tasks:
                return parsed
    except Exception:
        pass

    # ── Strategy 2: Parse DELIVERY TIMELINE pipe table from plan text ──
    sprints_from_timeline = {}
    tl_section = re.search(
        r'##\s*DELIVERY TIMELINE(.*?)(?=##\s+[A-Z]|\Z)',
        plan_text, re.DOTALL | re.IGNORECASE
    )
    if tl_section:
        for line in tl_section.group(1).split('\n'):
            parts = [p.strip() for p in line.split('|') if p.strip()]
            # Expect: WEEK | SPRINT/PHASE | MILESTONE | OWNER | STATUS | RISK FLAG
            if len(parts) >= 3 and not all(set(p) <= set('-: ') for p in parts):
                skip = {'week','sprint','phase','milestone','owner','status','risk','flag','---'}
                if parts[0].lower() in skip: continue
                week_lbl    = parts[0]
                sprint_name = parts[1] if len(parts) > 1 else f"{unit} {len(sprints_from_timeline)+1}"
                milestone   = parts[2] if len(parts) > 2 else ""
                key = sprint_name
                if key not in sprints_from_timeline:
                    sprints_from_timeline[key] = {
                        "sprint_num": len(sprints_from_timeline) + 1,
                        "name": sprint_name,
                        "weeks": week_lbl,
                        "goal": milestone,
                        "stories": [],
                        "deliverable": milestone
                    }
                else:
                    if milestone and milestone not in sprints_from_timeline[key]['stories']:
                        sprints_from_timeline[key]['stories'].append(milestone)

    if sprints_from_timeline:
        result = list(sprints_from_timeline.values())
        # Make sure each sprint has stories
        for sp in result:
            if not sp['stories'] and sp['goal']:
                sp['stories'] = [sp['goal']]
        return result

    # ── Strategy 3: Parse sprint/phase headings + bullets from raw plan text ──
    sprints_parsed = []
    current_sprint = None
    sprint_pattern = re.compile(
        r'^(?:#{1,3}\s*)?(' + unit + r'\s*\d+[:\-–]?\s*.+)$', re.IGNORECASE
    )
    week_pattern = re.compile(r'[Ww]eek\s*\d+', re.IGNORECASE)

    for line in plan_text.split('\n'):
        s = line.strip()
        if not s: continue
        m = sprint_pattern.match(s)
        if m:
            if current_sprint and current_sprint['stories']:
                sprints_parsed.append(current_sprint)
            idx = len(sprints_parsed) + 1
            wk_m = week_pattern.search(s)
            current_sprint = {
                "sprint_num": idx,
                "name": s.lstrip('#').strip(),
                "weeks": wk_m.group(0) if wk_m else f"Week {(idx-1)*sprint_len+1}–{idx*sprint_len}",
                "goal": "",
                "stories": [],
                "deliverable": ""
            }
        elif current_sprint:
            # Collect bullet tasks
            if re.match(r'^[-*•🔵▸]\s+', s):
                task = re.sub(r'^[-*•🔵▸]\s+', '', s).strip()
                if task and len(task) > 5:
                    current_sprint['stories'].append(task)
            elif re.match(r'^\d+\.\s+', s):
                task = re.sub(r'^\d+\.\s+', '', s).strip()
                if task and len(task) > 5:
                    current_sprint['stories'].append(task)
            elif not current_sprint['goal'] and len(s) > 10 and not s.startswith('#'):
                current_sprint['goal'] = s
    if current_sprint and current_sprint['stories']:
        sprints_parsed.append(current_sprint)

    if sprints_parsed:
        return sprints_parsed

    # ── Strategy 4: Extract from SOW analysis sections as last resort ──
    # Parse numbered deliverables from SOW text
    deliverables = []
    for line in plan_text.split('\n'):
        s = line.strip()
        m = re.match(r'^\d+\.\s+(.+)', s)
        if m and len(m.group(1)) > 15:
            deliverables.append(m.group(1))
        elif re.match(r'^[-*•]\s+', s):
            task = re.sub(r'^[-*•]\s+', '', s).strip()
            if len(task) > 20:
                deliverables.append(task)
    deliverables = list(dict.fromkeys(deliverables))[:num_sprints * 5]  # dedupe

    sprints = []
    tasks_per_sprint = max(3, len(deliverables) // max(num_sprints, 1))
    for i in range(num_sprints):
        w_start = i * sprint_len + 1
        w_end   = (i + 1) * sprint_len
        chunk   = deliverables[i*tasks_per_sprint:(i+1)*tasks_per_sprint]
        if not chunk:
            chunk = [f"Deliver {unit.lower()} {i+1} objectives"]
        label = f"{unit} {i+1}"
        phase_names = ["Discovery & Setup", "Architecture & Design", "Build & Integrate",
                       "Test & Validate", "UAT & Sign-off", "Deploy & Handover"]
        phase_label = phase_names[min(i, len(phase_names)-1)]
        sprints.append({
            "sprint_num": i+1,
            "name": f"{label}: {phase_label}",
            "weeks": f"Week {w_start}–{w_end}",
            "goal": chunk[0] if chunk else f"Complete {phase_label}",
            "stories": chunk,
            "deliverable": chunk[-1] if chunk else f"{phase_label} complete"
        })
    return sprints


def build_excel(plan_text, plan_name, wbs_sprints, methodology):
    import openpyxl, re as _re
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = openpyxl.Workbook()
    DK="0e1c12"; MG="163020"; AG="2d7a50"; BG_C="4caf78"; LG="e8f5ee"; LG2="f2faf5"
    WH="FFFFFF"; GR="f5f8f5"; TK="132018"; TM="2d4a36"; AM="c07a00"; RD="c0392b"
    BL="2d5fa8"; PU="6a3a9f"; BD="c8ddd0"

    def F(color, bold=False, size=10): return Font(name="Calibri", color=color, bold=bold, size=size)
    def BF(color): return PatternFill("solid", fgColor=color)
    def AL(h="left", v="center", wrap=False): return Alignment(horizontal=h, vertical=v, wrap_text=wrap)
    def bdr():
        s = Side(style="thin", color=BD); return Border(left=s, right=s, top=s, bottom=s)
    def thick_bot():
        s = Side(style="thin", color=BD); t = Side(style="medium", color=AG)
        return Border(left=s, right=s, top=s, bottom=t)

    def safe_title(t):
        c = _re.sub(r'[:/\\?*\[\]]', '', t).encode('ascii', 'ignore').decode('ascii').strip()[:31]
        return c or "Sheet"

    def xval(v):
        if v is None: return ""
        if isinstance(v, (str, int, float, bool)): return v
        if isinstance(v, dict): return v.get("name", v.get("task", v.get("story", str(v))))
        if isinstance(v, list): return "; ".join(str(xval(x)) for x in v)
        return str(v)

    def col_headers(ws, cols, row=2, bg=AG):
        for c, (lbl, w) in enumerate(cols, 1):
            cell = ws.cell(row=row, column=c, value=lbl)
            cell.fill = BF(bg); cell.font = F(WH, bold=True, size=10)
            cell.alignment = AL("center", "center"); cell.border = thick_bot()
            ws.column_dimensions[get_column_letter(c)].width = w
        ws.row_dimensions[row].height = 22

    def title_row(ws, text, n, bg=DK, fsz=14):
        ws.merge_cells(f"A1:{get_column_letter(n)}1")
        c = ws["A1"]; c.value = text; c.fill = BF(bg)
        c.font = F(WH, bold=True, size=fsz); c.alignment = AL("center", "center")
        c.border = bdr(); ws.row_dimensions[1].height = 40

    is_agile = any(k in methodology.lower() for k in ['agile','scrum','safe'])

    # ── Prepend mandatory Discovery Phase (first 2 weeks, universal) ──────────
    DISCOVERY_SPRINT = {
        "sprint_num": 0,
        "name": "Discovery & Setup: Foundation Sprint",
        "weeks": "Week 1–2",
        "goal": "Project foundation, Snowflake environment setup, RBAC, and discovery sessions",
        "sow_ref": "Standard — applies to all projects",
        "stories": [
            "Conduct Discovery Sessions with customer stakeholders",
            "Snowflake environment provisioning and configuration",
            "RBAC design and role hierarchy setup (SYSADMIN, USERADMIN, custom roles)",
            "Data source inventory and connectivity assessment",
            "Architecture Review Board (ARB) pack preparation",
            "Project Review Board (PRB) pack preparation",
            "Customer sign-off and acknowledgement of Discovery findings",
            "Sprint 1 planning and backlog grooming",
        ],
        "deliverable": "ARB/PRB packs approved · Customer acknowledgement received · Snowflake env ready",
    }
    # Only prepend if not already there
    first_name = str(wbs_sprints[0].get("name","")).lower() if wbs_sprints else ""
    if "discovery" not in first_name and "foundation" not in first_name:
        full_sprints = [DISCOVERY_SPRINT] + list(wbs_sprints)
    else:
        full_sprints = list(wbs_sprints)
        # Ensure the first sprint has the mandatory tasks even if named Discovery
        mandatory = DISCOVERY_SPRINT["stories"]
        existing = [str(xval(s)).lower() for s in full_sprints[0].get("stories", [])]
        for task in mandatory:
            if not any(task.lower()[:20] in e for e in existing):
                full_sprints[0].setdefault("stories", []).insert(0, task)

    # ════════════════════════════════════════
    # SHEET 1 — Project Summary
    # ════════════════════════════════════════
    ws1 = wb.active; ws1.title = safe_title("Project Summary")
    ws1.sheet_view.showGridLines = False; ws1.sheet_view.zoomScale = 95

    ws1.merge_cells("A1:F3"); h = ws1["A1"]; h.value = "PROJECT PLAN"
    h.fill = BF(DK); h.font = F(WH, bold=True, size=22); h.alignment = AL("left","center")
    for r in [1,2,3]: ws1.row_dimensions[r].height = 20 if r != 2 else 36

    ws1.merge_cells("A4:F4"); s = ws1["A4"]; s.value = plan_name
    s.fill = BF(MG); s.font = F(BG_C, bold=True, size=16); s.alignment = AL("left","center")
    ws1.row_dimensions[4].height = 30

    ws1.merge_cells("A5:F5"); m = ws1["A5"]
    m.value = f"Cortex Project Intel  ·  {datetime.now().strftime('%d %B %Y')}  ·  Methodology: {methodology}"
    m.fill = BF(MG); m.font = F("a8d8b8", size=9); m.alignment = AL("left","center")
    ws1.row_dimensions[5].height = 18; ws1.row_dimensions[6].height = 10

    info = [("Project Name", plan_name), ("Methodology", methodology),
            ("Generated", datetime.now().strftime('%d %B %Y')),
            ("Total Sprints / Phases", str(len(full_sprints))),
            ("Discovery Phase", "Week 1–2 (Standard — all projects)"),
            ("Source", "Cortex Project Intel — Snowflake AI")]
    for i, (lbl, val) in enumerate(info, 7):
        lc = ws1.cell(row=i, column=1, value=lbl); vc = ws1.cell(row=i, column=2, value=val)
        lc.fill = BF(AG); lc.font = F(WH, bold=True, size=10); lc.alignment = AL("right","center"); lc.border = bdr()
        vc.fill = BF(LG); vc.font = F(TK, size=10); vc.alignment = AL("left","center"); vc.border = bdr()
        ws1.row_dimensions[i].height = 20

    ws1.row_dimensions[13].height = 10
    ws1.merge_cells("A14:F14"); ph = ws1["A14"]; ph.value = "FULL PLAN TEXT"
    ph.fill = BF(AG); ph.font = F(WH, bold=True, size=10); ph.alignment = AL("center","center"); ph.border = bdr()
    ws1.row_dimensions[14].height = 18
    ws1.merge_cells("A15:F90"); pc = ws1["A15"]; pc.value = plan_text
    pc.font = F(TK, size=9); pc.alignment = AL("left","top",wrap=True); pc.fill = BF(GR)
    for r in range(15, 91): ws1.row_dimensions[r].height = 13
    ws1.column_dimensions["A"].width = 22; ws1.column_dimensions["B"].width = 55
    for col in ["C","D","E","F"]: ws1.column_dimensions[col].width = 14

    # ════════════════════════════════════════
    # SHEET 2 — WBS Sprint Board
    # ════════════════════════════════════════
    ws2 = wb.create_sheet(safe_title("WBS Sprint Board"))
    ws2.sheet_view.showGridLines = False; ws2.freeze_panes = "A3"
    WBS_COLS = [("Sprint / Phase",24),("Weeks",11),("Sprint Goal",38),("Task / User Story",46),
                ("Type",14),("Story Points",12),("Priority",12),("Assignee",18),("Deliverable",34),("Status",12)]
    title_row(ws2, ("SPRINT BOARD" if is_agile else "WORK BREAKDOWN STRUCTURE") + f"  —  {plan_name}", 10)
    col_headers(ws2, WBS_COLS)

    sp_pts = [8,8,5,5,3,3,2,3]; sev_list = ["High","High","Medium","Medium","Low","Medium","Low","Medium"]
    sev_col = {"High":RD,"Medium":AM,"Low":AG}; sev_bg = {"High":"fdeaea","Medium":"fff8e6","Low":"e8f5ee"}
    row = 3
    for s_idx, sprint in enumerate(full_sprints):
        sprint_name = str(xval(sprint.get("name",""))); weeks_lbl = str(xval(sprint.get("weeks","")))
        goal = str(xval(sprint.get("goal",""))); deliverable = str(xval(sprint.get("deliverable","")))
        raw_tasks = sprint.get("stories", sprint.get("tasks",[]))
        tasks = [str(xval(t)).strip() for t in (raw_tasks if isinstance(raw_tasks, list) else []) if str(xval(t)).strip()]
        if not tasks: tasks = [goal] if goal else ["Sprint deliverable"]
        hdr_bg = "0a1a0e" if s_idx == 0 else (DK if s_idx % 2 == 0 else MG)  # Discovery = darkest
        hdr_fg = BG_C if s_idx == 0 else BG_C

        ws2.merge_cells(f"A{row}:J{row}")
        banner = ws2.cell(row=row, column=1, value=f"  {sprint_name}   |   {weeks_lbl}   |   {goal}")
        banner.fill = BF(hdr_bg); banner.font = F(hdr_fg, bold=True, size=11)
        banner.alignment = AL("left","center"); banner.border = bdr()
        ws2.row_dimensions[row].height = 26; row += 1

        for t_idx, task in enumerate(tasks):
            row_bg = "e0f0e8" if s_idx == 0 and t_idx % 2 == 0 else \
                     "d4eadc" if s_idx == 0 else \
                     (LG if t_idx % 2 == 0 else LG2)
            pts = sp_pts[min(t_idx, len(sp_pts)-1)]
            sev = sev_list[min(t_idx, len(sev_list)-1)]
            sc = sev_col[sev]; sb = sev_bg[sev]
            t_type = "User Story" if is_agile else "Task"
            deliv = deliverable if t_idx == 0 else ""
            row_vals = [sprint_name, weeks_lbl, goal, task, t_type, pts, sev, "TBD", deliv, "To Do"]
            for c, val in enumerate(row_vals, 1):
                cell = ws2.cell(row=row, column=c, value=val); cell.border = bdr()
                if c == 6:
                    cell.fill = BF("e8f0ff"); cell.font = F(BL, bold=True); cell.alignment = AL("center","center")
                elif c == 7:
                    cell.fill = BF(sb); cell.font = F(sc, bold=True); cell.alignment = AL("center","center")
                elif c == 10:
                    cell.fill = BF("fff8e6"); cell.font = F(AM, bold=True, size=9); cell.alignment = AL("center","center")
                elif c == 9 and t_idx == 0:
                    cell.fill = BF(row_bg); cell.font = F(AG, bold=True); cell.alignment = AL("left","center",wrap=True)
                else:
                    cell.fill = BF(row_bg); cell.font = F(TK, bold=(c in [1,2] and t_idx == 0))
                    cell.alignment = AL("left","center",wrap=True)
            ws2.row_dimensions[row].height = 19; row += 1
        for c in range(1, 11):
            g = ws2.cell(row=row, column=c, value=""); g.fill = BF("ddeee5"); g.border = bdr()
        ws2.row_dimensions[row].height = 5; row += 1

    # ════════════════════════════════════════
    # SHEET 3 — RAID Log
    # ════════════════════════════════════════
    ws3 = wb.create_sheet(safe_title("RAID Log"))
    ws3.sheet_view.showGridLines = False; ws3.freeze_panes = "A3"
    RAID_COLS = [("ID",5),("Category",15),("Type",16),("Description",46),
                 ("Likelihood",13),("Impact",12),("Mitigation",44),("Owner",20)]
    title_row(ws3, f"RAID LOG  —  {plan_name}", len(RAID_COLS))
    col_headers(ws3, RAID_COLS)

    lh_c = {"High":RD,"Medium":AM,"Low":AG}
    lh_b = {"High":"fdeaea","Medium":"fff8e6","Low":"e8f5ee"}
    cat_bg = {"Risk":"fdeaea","Assumption":"e8f0ff","Issue":"fff8e6","Dependency":"f0e8ff"}
    cat_fg = {"Risk":RD,"Assumption":BL,"Issue":AM,"Dependency":PU}

    # Parse RAID from plan text
    raid_data = []
    sec = _re.search(r'##\s*RAID\s*LOG(.*?)(?=##\s+[A-Z]|\Z)', plan_text, _re.DOTALL|_re.IGNORECASE)
    if sec:
        skip = {'','id','type','category','description','likelihood','impact','mitigation','owner','---'}
        for line in sec.group(1).split('\n'):
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if len(parts) >= 4 and not all(set(p) <= set('-: ') for p in parts):
                if parts[0].lower() not in skip:
                    raid_data.append(parts)

    # Always include Discovery-phase standard RAID rows
    discovery_raid = [
        ["","Risk","Technical","Snowflake environment not provisioned before Week 1","High","High","Pre-provision environment Day 1; track in daily standup","Platform Engineer"],
        ["","Risk","Process","ARB/PRB approval delayed beyond Week 2","High","High","Submit ARB/PRB packs by end of Day 3; escalate if no response by Day 8","Project Manager"],
        ["","Assumption","Process","Customer stakeholders available for Discovery Sessions in Week 1–2","—","—","Confirm attendance list before project kick-off","Project Manager"],
        ["","Dependency","External","Customer sign-off on Discovery findings required before Sprint 2","High","High","Schedule sign-off meeting end of Week 2","Project Manager"],
        ["","Risk","Resource","RBAC roles not approved by InfoSec in time","Medium","High","Raise InfoSec request Week 1 Day 1; track daily","Platform Engineer"],
    ]

    if not raid_data:
        # Generic fallback RAID
        raid_data = [
            ["","Risk","Technical","Data access permissions not provisioned","High","High","Pre-provision all Snowflake roles before Sprint 1","Platform Engineer"],
            ["","Risk","Timeline","Scope creep from stakeholder change requests","Medium","High","Freeze scope after Sprint 2; route changes via CR process","Project Manager"],
            ["","Risk","Resource","Key person dependency on lead architect","Medium","Medium","Cross-train 2 team members on architecture decisions","Tech Lead"],
            ["","Risk","Budget","Compute cost overrun on large warehouse queries","Low","High","Set resource monitor alerts at 80% of credit budget","FinOps"],
            ["","Assumption","Process","Team availability maintained at 80% throughout","—","—","Confirm resource allocation with line managers before start","PMO"],
            ["","Assumption","Technical","Source APIs remain stable during integration","—","—","Document API versions; add version-check step","Data Engineer"],
            ["","Dependency","External","InfoSec approval for external API access","High","High","Raise InfoSec request Week 1; track weekly","Project Manager"],
            ["","Dependency","Data","Source data quality meets >90% DQ threshold","Medium","High","Run DQ profiling Sprint 1; escalate if score <90%","Data Engineer"],
            ["","Issue","Process","Legacy data quality issues in source tables","—","Medium","Run cleansing pipeline in Sprint 1; document exceptions","Data Engineer"],
            ["","Issue","Technical","Snowflake credit limitations on trial account","Low","Low","Monitor credit usage daily; upgrade account if needed","Platform Engineer"],
        ]

    # Combine: discovery rows first, then plan rows
    all_raid = discovery_raid + raid_data

    for i, r in enumerate(all_raid[:30]):
        while len(r) < 8: r.append("")
        rn = i + 3
        cat = str(r[1]).strip().title(); lh = str(r[4]).strip().title(); im = str(r[5]).strip().title()
        rbg = "e0f0e8" if i < len(discovery_raid) else (LG if i % 2 == 0 else LG2)
        for c, val in enumerate(r[:8], 1):
            cell = ws3.cell(row=rn, column=c, value=str(xval(val))); cell.border = bdr()
            if c == 1:
                cell.value = i + 1; cell.fill = BF(AG); cell.font = F(WH, bold=True); cell.alignment = AL("center","center")
            elif c == 2:
                cell.fill = BF(cat_bg.get(cat,"f5f8f5")); cell.font = F(cat_fg.get(cat,TK), bold=True); cell.alignment = AL("center","center")
            elif c == 3:
                cell.fill = BF("f0f0ff"); cell.font = F(BL); cell.alignment = AL("center","center")
            elif c == 5:
                bg = lh_b.get(lh,"f5f8f5") if lh not in ("—","") else GR
                cell.fill = BF(bg); cell.font = F(lh_c.get(lh,TK), bold=True); cell.alignment = AL("center","center")
            elif c == 6:
                bg = lh_b.get(im,"f5f8f5") if im not in ("—","") else GR
                cell.fill = BF(bg); cell.font = F(lh_c.get(im,TK), bold=True); cell.alignment = AL("center","center")
            else:
                cell.fill = BF(rbg); cell.font = F(TK); cell.alignment = AL("left","center",wrap=True)
        ws3.row_dimensions[rn].height = 22
    # Discovery section header annotation
    ws3.cell(row=2, column=1).value  # already set
    # Add a note row above discovery rows
    ws3.insert_rows(3)
    ws3.merge_cells(f"A3:{get_column_letter(len(RAID_COLS))}3")
    note = ws3["A3"]; note.value = "  ★  DISCOVERY PHASE RISKS (Week 1–2 — Standard for all projects)  ★"
    note.fill = BF("0a2a14"); note.font = F(BG_C, bold=True, size=10); note.alignment = AL("center","center")
    ws3.row_dimensions[3].height = 18

    # ════════════════════════════════════════
    # SHEET 4 — Test Cases
    # ════════════════════════════════════════
    ws4 = wb.create_sheet(safe_title("Test Cases"))
    ws4.sheet_view.showGridLines = False; ws4.freeze_panes = "A3"
    TC_COLS = [("ID",8),("Priority",12),("Test Type",16),("Scenario",34),
               ("Preconditions",28),("Test Steps",46),("Expected Result",36),
               ("Actual Result",36),("Status",13)]
    title_row(ws4, f"TEST CASES  —  {plan_name}", len(TC_COLS))
    col_headers(ws4, TC_COLS)

    pri_c = {"High":RD,"Medium":AM,"Low":AG}
    pri_b = {"High":"fdeaea","Medium":"fff8e6","Low":"e8f5ee"}
    typ_b = {"Integration":"e8f0ff","Data Quality":"e8f5ee","Security":"fdeaea",
             "Performance":"fff8e6","Functional":"f0e8ff","Regression":"f5f5f5","UAT":"e8f5ee"}
    typ_f = {"Integration":BL,"Data Quality":AG,"Security":RD,
             "Performance":AM,"Functional":PU,"Regression":"555555","UAT":AG}

    # Parse test cases from plan text
    tc_data = []
    tcs = _re.search(r'##\s*TEST\s*CASES(.*?)(?=##\s+[A-Z]|\Z)', plan_text, _re.DOTALL|_re.IGNORECASE)
    if tcs:
        skip2 = {'','id','scenario','steps','expected result','pass criteria','priority','test type','type','---'}
        for line in tcs.group(1).split('\n'):
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if len(parts) >= 4 and not all(set(p) <= set('-: ') for p in parts):
                if parts[0].lower() not in skip2:
                    tc_data.append(parts)

    # Mandatory Discovery Phase test cases
    discovery_tc = [
        ["DTC-01","High","Functional","Snowflake environment connectivity","Account provisioned","1.Login to Snowflake  2.Run SELECT CURRENT_VERSION()","Version returned; no errors","","Not Started"],
        ["DTC-02","High","Security","RBAC roles correctly provisioned","Roles created per design","1.Login as each role  2.Test permitted objects  3.Test restricted objects","Permitted access granted; restricted access denied","","Not Started"],
        ["DTC-03","High","Functional","ARB/PRB pack completeness review","Discovery sessions complete","1.Review ARB pack checklist  2.Review PRB pack checklist  3.Confirm customer sign-off","All sections complete; customer signed off","","Not Started"],
        ["DTC-04","Medium","Functional","Discovery session outputs documented","Sessions conducted","1.Review meeting notes  2.Verify all data sources captured  3.Confirm scope agreed","All outputs documented; scope baseline confirmed","","Not Started"],
    ]

    if not tc_data:
        tc_data = [
            ["TC-01","High","Integration","End-to-end pipeline ingestion","Source data available; pipeline deployed","1. Stage source CSV  2. Execute pipeline  3. Query target table","Row count matches source; no nulls in key columns","","Not Started"],
            ["TC-02","High","Data Quality","Schema and column type validation","Target table created","1. Run schema check SQL  2. Validate types  3. Check PK uniqueness","Zero schema violations; zero duplicate PKs","","Not Started"],
            ["TC-03","High","Security","Role-based access control","VIEWER and EDITOR roles provisioned","1. Login as VIEWER  2. Attempt INSERT  3. Verify permission error","INSERT denied; SELECT succeeds for VIEWER","","Not Started"],
            ["TC-04","High","Functional","Cortex AI response validation","CORTEX_COMPLETE enabled","1. Call CORTEX_COMPLETE  2. Parse JSON  3. Validate schema","Valid JSON; all fields present; latency < 5s","","Not Started"],
            ["TC-05","Medium","Performance","Dashboard load time","App deployed; tables populated","1. Open app URL  2. Load dashboard  3. Measure render time","Dashboard interactive within 3 seconds","","Not Started"],
            ["TC-06","Medium","Data Quality","DQ rule execution","DQ rule set configured","1. Execute DQ rules  2. Check DQ score  3. Review error log","DQ score >= 95%; flagged rows in error log","","Not Started"],
            ["TC-07","Medium","Regression","Pipeline idempotency","Pipeline run at least once","1. Re-trigger pipeline  2. Query row count  3. Compare pre/post","Row count unchanged; zero duplicates","","Not Started"],
            ["TC-08","Low","UAT","Business stakeholder sign-off","All TC-01–07 passed","1. Demo to business owner  2. Walk through KPIs  3. Capture sign-off","Business owner approves; sign-off document saved","","Not Started"],
        ]

    all_tc = discovery_tc + tc_data
    # Discovery section header
    ws4.insert_rows(3) if False else None  # placeholder — add after loop
    for i, rd in enumerate(all_tc[:25]):
        while len(rd) < 9: rd.append("")
        rn = i + 3; rbg = "e0f0e8" if i < len(discovery_tc) else (LG if i % 2 == 0 else LG2)
        pri = str(rd[1]).strip().title(); tt = str(rd[2]).strip().title()
        for c, val in enumerate(rd[:9], 1):
            cell = ws4.cell(row=rn, column=c, value=str(xval(val))); cell.border = bdr()
            if c == 1:
                cell.fill = BF(AG if i >= len(discovery_tc) else "0a2a14")
                cell.font = F(WH, bold=True); cell.alignment = AL("center","center")
            elif c == 2:
                cell.fill = BF(pri_b.get(pri,"fff8e6")); cell.font = F(pri_c.get(pri,AM), bold=True); cell.alignment = AL("center","center")
            elif c == 3:
                cell.fill = BF(typ_b.get(tt,"f5f5f5")); cell.font = F(typ_f.get(tt,TK), bold=True); cell.alignment = AL("center","center")
            elif c == 8:
                cell.fill = BF("fafcfb"); cell.font = F("bbbbbb", size=9); cell.alignment = AL("left","top",wrap=True)
                if not cell.value: cell.value = "— fill after execution —"
            elif c == 9:
                cell.fill = BF("fff8e6"); cell.font = F(AM, bold=True); cell.alignment = AL("center","center")
            else:
                cell.fill = BF(rbg); cell.font = F(TK); cell.alignment = AL("left","top",wrap=True)
        ws4.row_dimensions[rn].height = 44

    # ════════════════════════════════════════
    # SHEET 5 — Delivery Timeline
    # ════════════════════════════════════════
    ws5 = wb.create_sheet(safe_title("Delivery Timeline"))
    ws5.sheet_view.showGridLines = False; ws5.freeze_panes = "A3"
    TL_COLS = [("Week / Period",15),("Sprint / Phase",28),("Milestone",50),
               ("Tasks",44),("Owner",20),("Status",14),("Notes",28)]
    title_row(ws5, f"DELIVERY TIMELINE  —  {plan_name}", len(TL_COLS))
    col_headers(ws5, TL_COLS)

    st_col = {"Planned":"fff8e6","In Progress":"e8f0ff","Done":"e8f5ee","To Do":"f5f5f5"}
    st_fg  = {"Planned":AM,"In Progress":BL,"Done":AG,"To Do":"888888"}

    row = 3
    for s_idx, sprint in enumerate(full_sprints):
        sprint_name = str(xval(sprint.get("name",""))); weeks_lbl = str(xval(sprint.get("weeks","")))
        deliverable = str(xval(sprint.get("deliverable",""))); goal = str(xval(sprint.get("goal","")))
        raw_tasks = sprint.get("stories", sprint.get("tasks",[]))
        tasks = [str(xval(t)).strip() for t in (raw_tasks if isinstance(raw_tasks, list) else []) if str(xval(t)).strip()]
        if not tasks: tasks = [goal] if goal else ["Deliver sprint objectives"]

        is_disc = s_idx == 0 and "discovery" in sprint_name.lower() or "foundation" in sprint_name.lower()
        hdr_bg = "0a1a0e" if is_disc else (DK if s_idx % 2 == 0 else MG)
        rbg    = "d8eddf" if is_disc else (LG if s_idx % 2 == 0 else LG2)

        hdr = [weeks_lbl, sprint_name, f"Deliverable: {deliverable}", goal, "Project Team","Planned",""]
        for c, val in enumerate(hdr, 1):
            cell = ws5.cell(row=row, column=c, value=str(xval(val))); cell.border = bdr()
            if c <= 2:
                cell.fill = BF(hdr_bg); cell.font = F(BG_C, bold=True, size=11); cell.alignment = AL("left","center")
            elif c == 3:
                cell.fill = BF(rbg); cell.font = F(AG, bold=True); cell.alignment = AL("left","center",wrap=True)
            elif c == 6:
                cell.fill = BF("fff8e6"); cell.font = F(AM, bold=True); cell.alignment = AL("center","center")
            else:
                cell.fill = BF(rbg); cell.font = F(TK); cell.alignment = AL("left","center",wrap=True)
        ws5.row_dimensions[row].height = 26; row += 1

        for t_idx, task in enumerate(tasks):
            tbg = "eaf5ee" if is_disc else ("f7faf8" if s_idx%2==0 else "f2f7f3")
            tv = ["","", f"  • {task}","","TBD","To Do",""]
            for c, val in enumerate(tv, 1):
                cell = ws5.cell(row=row, column=c, value=str(xval(val))); cell.border = bdr()
                if c == 3:
                    cell.fill = BF(tbg); cell.font = F(TM, size=10); cell.alignment = AL("left","center",wrap=True)
                elif c == 6:
                    cell.fill = BF(st_col.get("To Do","f5f5f5")); cell.font = F(st_fg.get("To Do","888888"), bold=True, size=9)
                    cell.alignment = AL("center","center")
                else:
                    cell.fill = BF(tbg); cell.font = F(TK, size=9); cell.alignment = AL("left","center")
            ws5.row_dimensions[row].height = 17; row += 1

        # Gap row
        for c in range(1, 8):
            g = ws5.cell(row=row, column=c, value=""); g.fill = BF("ddeee5"); g.border = bdr()
        ws5.row_dimensions[row].height = 4; row += 1

    output = io.BytesIO(); wb.save(output); return output.getvalue()


# ── Load past projects ─────────────────────────────────────────
if st.session_state.past_projects_df is None:
    st.session_state.past_projects_df = load_past_projects()
past_projects_df = st.session_state.past_projects_df
db_names_set = set(past_projects_df['project_name'].tolist())

# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:

    # ── Header Banner (dark strip on light sidebar) ─────────────
    st.markdown("""
    <div style="background:linear-gradient(135deg,#112218 0%,#1f4633 60%,#2d7a50 100%);
                padding:1.2rem 1.1rem 1rem;margin-bottom:0.75rem;position:relative;overflow:hidden;">
        <div style="display:flex;align-items:center;gap:0.7rem;margin-bottom:0.5rem;">
            <div style="width:34px;height:34px;background:rgba(255,255,255,0.12);border-radius:9px;
                        display:flex;align-items:center;justify-content:center;font-size:1rem;
                        border:1px solid rgba(255,255,255,0.18);">📊</div>
            <div>
                <div style="font-size:9px;font-weight:700;color:rgba(255,255,255,0.45);text-transform:uppercase;letter-spacing:0.12em;">Snowflake Cortex</div>
                <div style="font-size:14px;font-weight:800;color:#fff;letter-spacing:-0.01em;line-height:1.2;">Project Intel</div>
            </div>
        </div>
        <div style="font-size:10px;color:rgba(255,255,255,0.35);line-height:1.5;">
            Configure your project below. Cortex selects the best templates automatically.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── SOW Upload ─────────────────────────────────────────────
    st.markdown("<div class='sb-card'>", unsafe_allow_html=True)
    st.markdown("### 📄 Statement of Work")
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
            st.warning("⚠️ PyPDF2 not installed.")
    st.markdown("</div>", unsafe_allow_html=True)

    # ── CSV Projects ───────────────────────────────────────────
    st.markdown("<div class='sb-card'>", unsafe_allow_html=True)
    st.markdown("### 📂 Project Plans (CSV)")
    plans_csvs = st.file_uploader("Upload CSVs (multiple allowed)", type=["csv"], accept_multiple_files=True)
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
            st.success(f"✅ {newly_added} new plan(s) added — {len(st.session_state.csv_projects)} total.")
        else:
            st.info(f"{len(st.session_state.csv_projects)} plan(s) already loaded.")
    if st.session_state.csv_projects:
        st.markdown(f"<div style='font-size:11px;color:#2d7a50;font-weight:700;margin:.35rem 0;'>📎 {len(st.session_state.csv_projects)} plan(s) in memory</div>", unsafe_allow_html=True)
        for idx_p, proj in enumerate(st.session_state.csv_projects):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                nm = proj['project_name']
                already = nm in db_names_set or nm in st.session_state.imported_csv_names
                badge = "✅" if already else "⏳"
                name_color = "#2d7a50" if already else "#b07a00"
                st.markdown(f"<div style='font-size:11px;color:#2d5a3a;padding:.2rem 0;'>📄 {proj['project_name']} <span style='color:{name_color};font-size:9px;'>{badge}</span></div>", unsafe_allow_html=True)
            with col_b:
                btn_label = "Re-save" if (proj['project_name'] in db_names_set or proj['project_name'] in st.session_state.imported_csv_names) else "Import"
                if st.button(btn_label, key=f"import_csv_{idx_p}"):
                    try:
                        pn=proj["project_name"].replace("'","''")
                        pd_=proj["description"][:500].replace("'","''")
                        wb_=proj["wbs_summary"][:3000].replace("'","''")
                        sd=f"'{proj['start_date']}'" if proj["start_date"] else "NULL"
                        ed=f"'{proj['end_date']}'" if proj["end_date"] else "NULL"
                        try: session.sql(f"DELETE FROM projects WHERE project_name='{pn}' AND description LIKE '%CSV%'").collect()
                        except: pass
                        session.sql(f"INSERT INTO projects (project_name,description,start_date,end_date,status,lead_architect,technologies_used,wbs_summary,risk_log_summary,test_cases_summary,deployment_plan) VALUES('{pn}','{pd_}',{sd},{ed},'Completed','','','{wb_}','','','')").collect()
                        st.session_state.imported_csv_names.add(proj["project_name"])
                        st.session_state.past_projects_df = load_past_projects()
                        st.success(f"✅ Saved '{proj['project_name']}'")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Import failed: {e}")
        if st.button("🗑️ Clear All CSV Plans", key="clear_csvs"):
            st.session_state.csv_projects = []; st.session_state.imported_csv_names = set(); st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    # ── New Project Details ─────────────────────────────────────
    st.markdown("<div class='sb-card'>", unsafe_allow_html=True)
    st.markdown("### 🆕 New Project Details")
    new_project_name = st.text_input("Project Name", "Data Cataloging Initiative")
    new_project_desc = st.text_area("Description", "A new data cataloging initiative to index all data assets in Snowflake.", height=80)
    new_project_tech = st.text_input("Key Technologies", "Snowflake, Streamlit, Python")
    customer_name    = st.text_input("Customer / Organisation", "Acme Corp")
    project_type     = st.selectbox("Project Type", ["Data Engineering","Analytics / BI","Data Science / ML","Data Platform Migration","App Development","Data Governance","Other"])
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Plan Settings ──────────────────────────────────────────
    st.markdown("<div class='sb-card'>", unsafe_allow_html=True)
    st.markdown("### ⚙️ Plan Settings")
    team_size      = st.slider("Team Size", 2, 20, 5)
    duration_weeks = st.slider("Target Duration (weeks)", 2, 52, 12)
    risk_appetite  = st.select_slider("Risk Appetite", ["Low","Medium","High"], value="Medium")
    methodology    = st.selectbox("Methodology", ["Agile / Scrum","Waterfall","Hybrid","SAFe"])
    delivery_priority = st.selectbox("Delivery Priority", ["Balanced (scope, time, cost)","Time-boxed (fixed deadline)","Scope-driven (all requirements)","Cost-constrained (fixed budget)"])
    team_experience   = st.selectbox("Team Experience", ["Expert (3+ yrs)","Intermediate (1-3 yrs)","Beginner (< 1 yr)","Mixed levels"])
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Roles ──────────────────────────────────────────────────
    st.markdown("<div class='sb-card'>", unsafe_allow_html=True)
    st.markdown("### 👥 Team Roles")
    rc1, rc2 = st.columns(2)
    with rc1:
        role_pm = st.number_input("👔 PM",  min_value=0, max_value=5, value=1)
        role_sa = st.number_input("🏗️ SA", min_value=0, max_value=5, value=1)
        role_de = st.number_input("⚙️ DE", min_value=0, max_value=10, value=2)
    with rc2:
        role_ds = st.number_input("🔬 DS",  min_value=0, max_value=5, value=1)
        role_qa = st.number_input("🧪 QA", min_value=0, max_value=5, value=1)
    has_dedicated_pm = role_pm > 0; has_qa_resource = role_qa > 0
    team_roster = f"PM×{role_pm}, SA×{role_sa}, DE×{role_de}, DS×{role_ds}, QA×{role_qa}"
    team_size_from_roles = role_pm + role_sa + role_de + role_ds + role_qa
    if team_size < team_size_from_roles: team_size = team_size_from_roles
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Compliance ─────────────────────────────────────────────
    st.markdown("<div class='sb-card'>", unsafe_allow_html=True)
    st.markdown("### 🔒 Compliance & Data")
    compliance_reqs  = st.multiselect("Compliance", ["GDPR","HIPAA","SOC 2","PCI-DSS","ISO 27001","None"], default=["None"])
    has_data_sensitivity = st.selectbox("Data Sensitivity", ["Public","Internal","Confidential","Restricted / PII"])
    st.markdown("</div>", unsafe_allow_html=True)

    # ════════════════════════════════════════
    # VALUE-ADD WIDGETS
    # ════════════════════════════════════════

    # 1. Project Health Score
    timeline_score = max(10, min(95, 100 - max(0, duration_weeks - 16) * 2))
    team_score     = max(20, min(95, 60 + (team_size_from_roles * 4)))
    risk_score     = {"Low": 90, "Medium": 68, "High": 42}.get(risk_appetite, 68)
    compliance_penalty = 8 * max(0, len([c for c in compliance_reqs if c != "None"]) - 1)
    risk_score = max(20, risk_score - compliance_penalty)
    experience_score = {"Expert (3+ yrs)":88,"Intermediate (1-3 yrs)":70,"Beginner (< 1 yr)":45,"Mixed levels":62}.get(team_experience, 70)

    def score_color(s):
        if s >= 75: return "#2d9e5e", "#4caf78", "rgba(76,175,120,0.6)"
        if s >= 50: return "#b07a00", "#f0a500", "rgba(240,165,0,0.5)"
        return "#a02020", "#e05c5c", "rgba(224,92,92,0.5)"

    bars = [("Timeline Health", timeline_score), ("Team Capacity", team_score),
            ("Risk Posture", risk_score), ("Team Readiness", experience_score)]

    bars_html = ""
    for label, score in bars:
        bg, fg, glow = score_color(score)
        bars_html += (
            "<div style='margin-bottom:0.45rem;'>"
            "<div style='display:flex;justify-content:space-between;font-size:10px;font-weight:600;"
            "color:#2d5a3a;margin-bottom:0.18rem;'>"
            "<span>" + label + "</span>"
            "<span style='color:" + fg + ";font-weight:700;'>" + str(score) + "%</span></div>"
            "<div style='height:5px;background:rgba(0,0,0,0.1);border-radius:999px;overflow:hidden;'>"
            "<div style='height:100%;width:" + str(score) + "%;border-radius:999px;"
            "background:linear-gradient(90deg," + bg + "," + fg + ");box-shadow:0 0 6px " + glow + ";'></div>"
            "</div></div>"
        )

    overall = int((timeline_score + team_score + risk_score + experience_score) / 4)
    ov_bg, ov_fg, _ = score_color(overall)
    ov_label = "Healthy" if overall >= 75 else "Moderate" if overall >= 50 else "At Risk"

    health_html = (
        "<div style='background:linear-gradient(135deg,#ffffff 0%,#f0faf4 100%);"
        "border:1.5px solid rgba(45,122,80,0.22);border-radius:12px;"
        "padding:0.85rem 1rem;margin-bottom:0.7rem;box-shadow:0 2px 12px rgba(0,0,0,0.05);'>"
        "<div style='display:flex;align-items:center;gap:0.35rem;margin-bottom:0.6rem;'>"
        "<span style='font-size:0.9rem;'>📈</span>"
        "<span style='font-size:9px;font-weight:800;color:#2d7a50;text-transform:uppercase;letter-spacing:0.16em;'>Project Health Score</span>"
        "<span style='margin-left:auto;background:linear-gradient(135deg," + ov_bg + "," + ov_fg + ");"
        "color:#fff;font-size:10px;font-weight:800;padding:0.15rem 0.6rem;border-radius:12px;'>"
        + str(overall) + "% — " + ov_label + "</span></div>"
        + bars_html +
        "</div>"
    )
    st.markdown(health_html, unsafe_allow_html=True)

    # 2. Effort Estimator
    story_points_per_sprint = {"Expert (3+ yrs)": 42, "Intermediate (1-3 yrs)": 30,
                                "Beginner (< 1 yr)": 18, "Mixed levels": 26}.get(team_experience, 30)
    num_sprints_est = max(1, duration_weeks // 2)
    total_sp        = story_points_per_sprint * num_sprints_est
    dev_days        = role_de * duration_weeks * 4
    qa_days         = role_qa * duration_weeks * 3
    pm_overhead_pct = 15 if role_pm > 0 else 8

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,#0e1e14 0%,#1a3a2a 100%);border:1px solid rgba(76,175,120,0.2);border-radius:12px;padding:0.85rem 1rem;margin-bottom:0.7rem;box-shadow:0 2px 12px rgba(0,0,0,0.15);">
        <div style="font-size:9px;font-weight:800;color:#4caf78;text-transform:uppercase;letter-spacing:0.16em;margin-bottom:0.6rem;">⚡ Effort Estimator</div>
        <div style='display:flex;justify-content:space-between;align-items:center;padding:0.25rem 0;border-bottom:1px solid rgba(255,255,255,0.04);font-size:11px;'><span style='color:rgba(255,255,255,0.45);font-weight:500;'>Total Sprints</span><span style='color:#4caf78;font-weight:700;'>{num_sprints_est} × 2-week</span></div>
        <div style='display:flex;justify-content:space-between;align-items:center;padding:0.25rem 0;border-bottom:1px solid rgba(255,255,255,0.04);font-size:11px;'><span style='color:rgba(255,255,255,0.45);font-weight:500;'>Story Points</span><span style='color:#4caf78;font-weight:700;'>~{total_sp} SP total</span></div>
        <div style='display:flex;justify-content:space-between;align-items:center;padding:0.25rem 0;border-bottom:1px solid rgba(255,255,255,0.04);font-size:11px;'><span style='color:rgba(255,255,255,0.45);font-weight:500;'>Dev Effort</span><span style='color:#4caf78;font-weight:700;'>{dev_days} person-days</span></div>
        <div style='display:flex;justify-content:space-between;align-items:center;padding:0.25rem 0;border-bottom:1px solid rgba(255,255,255,0.04);font-size:11px;'><span style='color:rgba(255,255,255,0.45);font-weight:500;'>QA Effort</span><span style='color:#4caf78;font-weight:700;'>{qa_days} person-days</span></div>
        <div style='display:flex;justify-content:space-between;align-items:center;padding:0.25rem 0;border-bottom:1px solid rgba(255,255,255,0.04);font-size:11px;'><span style='color:rgba(255,255,255,0.45);font-weight:500;'>PM Overhead</span><span style='color:#4caf78;font-weight:700;'>{pm_overhead_pct}% of total</span></div>
        <div style='display:flex;justify-content:space-between;align-items:center;padding:0.25rem 0;border-bottom:1px solid rgba(255,255,255,0.04);font-size:11px;'><span style='color:rgba(255,255,255,0.45);font-weight:500;'>Team Size</span><span style='color:#4caf78;font-weight:700;'>{team_size_from_roles} people</span></div>
    </div>
    """, unsafe_allow_html=True)

    # 3. Smart AI Tip
    tips = {
        "Agile / Scrum": ("🤖 Agile Tip", "For Snowflake projects, lock your schema contract in Sprint 1. Changing column types mid-sprint adds hidden rework — define a data contract first."),
        "Waterfall":     ("📐 Waterfall Tip", "Allocate 25-30% of your timeline to UAT. Waterfall projects often underestimate the time needed for sign-off cycles with stakeholders."),
        "Hybrid":        ("🔀 Hybrid Tip", "Use Agile for data pipeline work and Waterfall gates for stakeholder milestones. This balances delivery speed with governance requirements."),
        "SAFe":          ("🏢 SAFe Tip", "Define your Solution Train dependencies in PI Planning Week 1. Cross-team blockers are the #1 SAFe delivery risk."),
    }
    tip_title, tip_text = tips.get(methodology, tips["Agile / Scrum"])
    tip_html = (
        "<div style='background:linear-gradient(135deg,#e8f7ee 0%,#d8f0e4 100%);"
        "border:1.5px solid rgba(45,122,80,0.28);border-left:4px solid #3d8c5e;"
        "border-radius:0 12px 12px 0;padding:0.8rem 0.9rem;margin-bottom:0.7rem;'>"
        "<div style='font-size:9px;font-weight:800;color:#2d7a50;text-transform:uppercase;"
        "letter-spacing:0.14em;margin-bottom:0.3rem;'>✨ " + tip_title + "</div>"
        "<div style='font-size:11px;color:#1a3a2a;line-height:1.6;font-weight:500;'>" + tip_text + "</div>"
        "</div>"
    )
    st.markdown(tip_html, unsafe_allow_html=True)

    # 4. Methodology Quick Reference
    method_info = {
        "Agile / Scrum": {"icon":"🏃","color":"#2d7a50","cadence":"2-week sprints","ceremonies":"Daily standup, Sprint Review, Retro","best_for":"Iterative data products"},
        "Waterfall":     {"icon":"📊","color":"#2d5fa8","cadence":"Sequential phases","ceremonies":"Phase gate reviews","best_for":"Fixed-scope migrations"},
        "Hybrid":        {"icon":"🔀","color":"#6a3a9f","cadence":"Agile dev + Waterfall gates","ceremonies":"Sprint reviews + phase sign-offs","best_for":"Enterprise data programs"},
        "SAFe":          {"icon":"🏢","color":"#b07a00","cadence":"PI (8-12 weeks)","ceremonies":"PI Planning, ART Sync","best_for":"Large multi-team programs"},
    }
    mi = method_info.get(methodology, method_info["Agile / Scrum"])
    method_color = mi['color']
    st.markdown(
        "<div style='background:rgba(255,255,255,0.78);border:1.5px solid rgba(45,122,80,0.16);"
        "border-left:3px solid " + method_color + ";border-radius:12px;"
        "padding:0.9rem 1rem;margin-bottom:0.7rem;box-shadow:0 2px 10px rgba(0,0,0,0.05);'>"
        "<div style='display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem;'>"
        "<span style='font-size:1.1rem;'>" + mi['icon'] + "</span>"
        "<span style='font-size:10px;font-weight:800;color:" + method_color + ";text-transform:uppercase;letter-spacing:0.12em;'>" + methodology + "</span>"
        "</div>"
        "<div style='display:flex;flex-direction:column;gap:0.3rem;'>"
        "<div style='font-size:10px;color:#3a6a4a;'><span style='font-weight:700;color:#1a3a2a;'>Cadence:</span> " + mi['cadence'] + "</div>"
        "<div style='font-size:10px;color:#3a6a4a;'><span style='font-weight:700;color:#1a3a2a;'>Ceremonies:</span> " + mi['ceremonies'] + "</div>"
        "<div style='font-size:10px;color:#3a6a4a;'><span style='font-weight:700;color:#1a3a2a;'>Best for:</span> " + mi['best_for'] + "</div>"
        "</div></div>",
        unsafe_allow_html=True
    )

    st.markdown("---")
    generate_button = st.button("🚀 Generate Project Plan", type="primary")


# ═══════════════════════════════════════════════════════════════
# PAST PROJECTS PANEL
# ═══════════════════════════════════════════════════════════════
true_count     = len(past_projects_df)
csv_count      = len(st.session_state.csv_projects)
db_names_set   = set(past_projects_df['project_name'].tolist())
imported_names = st.session_state.imported_csv_names

if true_count > 0 or csv_count > 0:
    col_l, col_r = st.columns([2, 1])
    with col_l:
        st.markdown("<div class='section-label'>Institutional Memory</div>", unsafe_allow_html=True)
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        db_label  = f"{true_count} Completed Project{'s' if true_count!=1 else ''} in Database"
        csv_label = f" + {csv_count} CSV Plan{'s' if csv_count!=1 else ''}" if csv_count else ""
        st.markdown(f"<h2>📚 {db_label}{csv_label}</h2>", unsafe_allow_html=True)
        st.markdown("<div style='font-size:11px;color:rgba(255,255,255,0.35);margin-bottom:0.6rem;'>Cortex selects the most relevant past projects as templates.</div>", unsafe_allow_html=True)

        project_names = past_projects_df['project_name'].tolist()
        csv_display   = []
        csv_map       = {}
        for p in st.session_state.csv_projects:
            nm = p.get("project_name","CSV Project")
            is_in_db = (nm in db_names_set) or (nm in imported_names)
            lbl = f"📎 {nm} ✓ Imported" if is_in_db else f"📎 {nm} · Pending Import"
            csv_display.append(lbl); csv_map[lbl] = p

        browse_options = ["— Select to view —"] + csv_display + project_names
        if len(browse_options) == 1:
            st.info("No projects loaded yet.")
        else:
            selected_proj = st.selectbox("Browse a project:", browse_options)
            if selected_proj and selected_proj != "— Select to view —":
                if selected_proj in csv_map:
                    proj_data = csv_map[selected_proj]
                    nm = proj_data.get('project_name','CSV Project')
                    is_in_db = (nm in db_names_set) or (nm in imported_names)
                    badge_html = "<span class='csv-badge-imported'>✓ In Database</span>" if is_in_db else "<span class='csv-badge-pending'>Pending Import</span>"
                    with st.expander(f"📁 {nm}", expanded=True):
                        st.markdown(f"<div style='font-size:11px;color:rgba(255,255,255,0.5);margin-bottom:.5rem;'>📎 CSV Upload {badge_html}</div>", unsafe_allow_html=True)
                        d1, d2 = st.columns(2)
                        with d1: st.markdown(f"**Description:** {proj_data.get('description','N/A')}")
                        with d2: st.markdown(f"**Start:** {proj_data.get('start_date','—')} &nbsp; **End:** {proj_data.get('end_date','—')}")
                        st.markdown("**WBS Preview:**"); st.code(proj_data.get("wbs_summary","")[:600], language=None)
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

    st.session_state.qa_history     = []
    st.session_state.wbs_structured = None
    st.session_state.plan_methodology = methodology
    st.session_state.plan            = None
    st.session_state.sow_requirements = None
    st.session_state.past_patterns   = None

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
            '<div class="pulse-loader"><div class="pulse-rings"><span></span><span></span><span></span><span></span></div>'
            '<div class="pulse-label">Cortex Project Intel</div>'
            '<div class="pulse-sublabel">' + (msg or "Processing...") + '</div>'
            '<div class="pulse-steps">' + items + '</div></div>',
            unsafe_allow_html=True
        )

    is_agile = any(k in methodology.lower() for k in ['agile','scrum','safe'])
    sprint_len = 2; num_sprints = max(1, duration_weeks // sprint_len)
    compliance_str = ", ".join(compliance_reqs) if compliance_reqs else "None"
    proj_context = (
        f"Project: {new_project_name}\nCustomer: {customer_name}\nType: {project_type}\n"
        f"Description: {new_project_desc[:300]}\nTechnologies: {new_project_tech}\n"
        f"Team: {team_size} people | Experience: {team_experience}\nRoles: {team_roster}\n"
        f"Duration: {duration_weeks} weeks | Methodology: {methodology}\n"
        f"Delivery Priority: {delivery_priority}\nRisk Appetite: {risk_appetite}\n"
        f"Data Sensitivity: {has_data_sensitivity}\nCompliance: {compliance_str}"
    )

    show_step(0, "Reading " + sow_pdf.name + " ...")
    sow_call = (
        "You are a senior business analyst reading a Statement of Work (SOW).\n\n"
        "Project context:\n" + proj_context + "\n\nAnalyse the SOW below. Output EXACTLY these 8 numbered sections:\n\n"
        "1. PROJECT OBJECTIVE\n2. KEY DELIVERABLES\n3. ACCEPTANCE CRITERIA\n"
        "4. FUNCTIONAL REQUIREMENTS\n5. NON-FUNCTIONAL REQUIREMENTS\n6. OUT OF SCOPE\n"
        "7. CONSTRAINTS & DEPENDENCIES\n8. RISKS & ASSUMPTIONS IN THE SOW\n\n"
        "SOW TEXT:\n" + sow_text[:5000]
    )
    sow_analysis = cortex_call(sow_call, "SOW Analysis")
    st.session_state.sow_requirements = sow_analysis

    show_step(1, "Scanning past projects...")
    candidates = [{"id": int(r['project_id']), "name": r['project_name'], "desc": str(r.get('description',''))[:80]}
                  for _, r in past_projects_df.iterrows()]
    for ci, cp in enumerate(st.session_state.csv_projects):
        candidates.append({"id": -(ci+1), "name": cp["project_name"], "desc": cp.get("description","")[:80]})

    sel_raw = cortex_call("Select up to 4 past projects most similar to:\n" + proj_context[:400] +
                          "\nCandidates:\n" + json.dumps(candidates[:25])[:1200] +
                          "\nReturn ONLY a JSON array of IDs: [1, 4, 7]", "Template Selection")
    selected_ids = []
    try:
        raw_ids = json.loads(sel_raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip())
        selected_ids = [int(x) for x in raw_ids if str(x).strip().isdigit()][:4]
    except: selected_ids = [c["id"] for c in candidates[:3]]

    pattern_context = ""; used_projects = []
    csv_id_map = {-(i+1): p for i, p in enumerate(st.session_state.csv_projects)}
    for pid in selected_ids:
        match = past_projects_df[past_projects_df['project_id'].astype(str) == str(pid)]
        if pid < 0 and pid in csv_id_map:
            p = csv_id_map[pid]
            pattern_context += "\n-- " + p['project_name'] + " (CSV) --\nWBS: " + p['wbs_summary'][:300] + "\n"
            used_projects.append(p['project_name'])
        elif not match.empty:
            r = match.iloc[0]
            pattern_context += ("\n-- " + r['project_name'] + " --\nRAID: " + str(r.get('risk_log_summary',''))[:280] +
                                "\nTests: " + str(r.get('test_cases_summary',''))[:200] +
                                "\nDeployment: " + str(r.get('deployment_plan',''))[:180] + "\n")
            used_projects.append(r['project_name'])

    if not pattern_context: pattern_context = "No past project data. Use PMO best practices."

    patterns_call = (
        "You are a PMO expert extracting reusable patterns.\n\nProject context:\n" + proj_context +
        "\n\nPast project data:\n" + pattern_context[:3000] +
        "\n\nOutput EXACTLY these four sections:\n"
        "### A. TOP RISKS\nExactly 8 pipe-delimited rows: CATEGORY | TYPE | DESCRIPTION | LIKELIHOOD | IMPACT | MITIGATION | OWNER\n"
        "### B. TEST PATTERNS\nExactly 8 pipe-delimited rows: ID | PRIORITY | TYPE | SCENARIO | PRECONDITIONS | STEPS | EXPECTED RESULT\n"
        "### C. TIMELINE LESSONS\nExactly 5 bullet points with actionable lessons.\n"
        "### D. TEAM STRUCTURE\nRoles as pipe-delimited rows: Role Title | Headcount | Key Responsibilities | Skills Required"
    )
    past_patterns = cortex_call(patterns_call, "Pattern Extraction")
    st.session_state.past_patterns = past_patterns

    show_step(2, "Decomposing SOW into deliverables...")
    sprint_div = ("Divide into " + str(num_sprints) + " Sprints of 2 weeks each." if is_agile else "Divide into phases.")
    wbs_call = (
        "You are a PMO analyst creating a Work Breakdown Structure. "
        "Read the SOW analysis below and produce a detailed sprint plan.\n\n"
        "CRITICAL RULES:\n"
        "- 'stories' must contain SPECIFIC, REAL tasks derived from the SOW deliverables — not generic text.\n"
        "- Each story should be an action: 'Build X', 'Configure Y', 'Test Z', 'Deploy W'.\n"
        "- Use EXACT deliverable names from the SOW analysis in goals and stories.\n"
        "- Produce exactly " + str(num_sprints) + " " + ("sprints" if is_agile else "phases") + ".\n"
        "- Do NOT use placeholder text like 'Task 1', 'Define', 'Build', 'Test' as standalone items.\n\n"
        "Project: " + new_project_name + " | Customer: " + customer_name + "\n"
        "Technologies: " + new_project_tech + "\n"
        "Team: " + team_roster + "\n\n"
        "SOW Analysis:\n" + sow_analysis[:2800] + "\n\n"
        + sprint_div + "\n\n"
        "Return ONLY a valid JSON array (no markdown, no extra text):\n"
        '[{"sprint_num":1,"name":"Sprint 1: <specific title>","weeks":"Week 1-2",'
        '"goal":"<specific deliverable from SOW>","sow_ref":"<SOW section referenced>",'
        '"stories":["<specific task 1 from SOW>","<specific task 2>","<specific task 3>","<specific task 4>"],'
        '"deliverable":"<specific acceptance criterion from SOW>"}]'
    )
    raw_wbs = cortex_call(wbs_call, "WBS Generation")
    wbs_sprints_gen = []
    if raw_wbs:
        try:
            clean = raw_wbs.strip()
            # Strip markdown fences
            for fence in ["```json", "```JSON", "```"]:
                if clean.startswith(fence): clean = clean[len(fence):]
                if clean.endswith(fence):   clean = clean[:-len(fence)]
            clean = clean.strip()
            # Find JSON array bounds
            start = clean.find('['); end = clean.rfind(']')
            if start != -1 and end != -1:
                clean = clean[start:end+1]
            wbs_sprints_gen = json.loads(clean)
            if not isinstance(wbs_sprints_gen, list):
                wbs_sprints_gen = []
            else:
                # Validate quality — reject if stories are generic placeholders
                placeholders = {'define','build','test','task 1','task 2','task 3','task 4',
                                'specific task 1','specific task 2','tbd','n/a',''}
                total_real = sum(
                    1 for sp in wbs_sprints_gen
                    for story in sp.get('stories', [])
                    if str(story).lower().strip() not in placeholders and len(str(story)) > 10
                )
                if total_real < len(wbs_sprints_gen) * 2:
                    # Too many placeholders — discard and re-extract from SOW analysis
                    wbs_sprints_gen = []
        except Exception:
            wbs_sprints_gen = []

    # If primary call failed or returned junk, extract from SOW analysis text
    if not wbs_sprints_gen:
        wbs_sprints_gen = parse_wbs_into_sprints(sow_analysis, duration_weeks, methodology)
    st.session_state.wbs_structured = wbs_sprints_gen

    show_step(3, "Applying past project lessons...")
    wbs_for_prompt = "\n".join([
        "  " + s.get('name','') + " (" + s.get('weeks','') + "): " + s.get('goal','') + " => " + s.get('deliverable','')
        for s in wbs_sprints_gen
    ])[:1200]

    show_step(4, "Assembling final plan document...")
    assembly_call = (
        "You are a senior PMO consultant writing the final project plan.\n\n"
        "Project context:\n" + proj_context + "\n\n"
        "=== SOW SCOPE ===\n" + sow_analysis[:1800] + "\n\n"
        "=== WBS ===\n" + wbs_for_prompt + "\n\n"
        "=== PAST PROJECT PATTERNS ===\n" + past_patterns[:1200] + "\n\n"
        "Write EXACTLY these 7 sections with EXACT ## headers:\n\n"
        "## EXECUTIVE SUMMARY\n3 paragraphs covering deliverables, past patterns, and KPIs.\n\n"
        "## RAID LOG\nMin 10 pipe-delimited rows: CATEGORY | TYPE | DESCRIPTION | LIKELIHOOD | IMPACT | MITIGATION | OWNER\n\n"
        "## TEST CASES\nMin 10 pipe-delimited rows: ID | PRIORITY | TYPE | SCENARIO | PRECONDITIONS | STEPS | EXPECTED RESULT\n\n"
        "## DELIVERY TIMELINE\nPipe-delimited rows: WEEK | SPRINT/PHASE | MILESTONE | OWNER | STATUS | RISK FLAG\n\n"
        "## DEPLOYMENT STRATEGY\nProse. Phased go-live, rollback, sign-off gates.\n\n"
        "## SOW TRACEABILITY MATRIX\nPipe-delimited rows: SOW REQUIREMENT | SPRINT/PHASE | KEY TASKS | ACCEPTANCE CRITERIA | STATUS\n\n"
        "## LESSONS FROM PAST PROJECTS\nBullets. Each with past project reference and change made to this plan.\n\n"
        "Sections with pipe-delimited format: data rows ONLY, no headers, no blanks within."
    )
    full_plan = cortex_call(assembly_call, "Plan Assembly")
    st.session_state.plan      = full_plan or "Plan assembly failed. SOW analysis and WBS still available."
    st.session_state.plan_name = new_project_name
    loader.empty(); st.rerun()

# ═══════════════════════════════════════════════════════════════
# DISPLAY PLAN
# ═══════════════════════════════════════════════════════════════
if st.session_state.plan:
    plan      = st.session_state.plan
    plan_name = st.session_state.plan_name
    meth      = st.session_state.plan_methodology or "Agile / Scrum"
    sprints   = st.session_state.wbs_structured or []
    is_agile  = any(k in meth.lower() for k in ['agile','scrum','safe'])

    st.markdown("<div class='section-label'>Delivery Impact</div>", unsafe_allow_html=True)
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    mc1,mc2,mc3,mc4 = st.columns(4)
    with mc1: st.metric("Planning Time", "Weeks → Minutes", "90% reduction")
    with mc2: st.metric("Risk Identification", "Reactive → Predictive", "From past projects")
    with mc3: st.metric("Templates Used", min(len(past_projects_df), 4), "matched by Cortex")
    with mc4: st.metric("Sprint / Phase Count", len(sprints) if sprints else "—")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-label' style='margin-top:0.25rem;'>Generated Project Plan</div>", unsafe_allow_html=True)

    tab_plan,tab_wbs,tab_sow,tab_qa,tab_risks,tab_export = st.tabs([
        "📋 Full Plan","🏃 WBS / Sprints","📄 SOW Analysis","💬 Ask the Plan","⚠️ Risk Analyser","📤 Export"])

    with tab_plan:
        st.markdown(f"<h2>📋 {plan_name}</h2>", unsafe_allow_html=True)

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
                      "Performance":"#f0a500","Functional":"#a05be8","Regression":"#888","Uat":"#4caf78","UAT":"#4caf78"}
            ST_BG  = {"To Do":"rgba(140,140,140,0.12)","Planned":"rgba(240,165,0,0.12)",
                      "In Progress":"rgba(91,140,232,0.12)","Done":"rgba(76,175,120,0.12)"}
            ST_COL = {"To Do":"#888","Planned":"#f0a500","In Progress":"#5b8ce8","Done":"#4caf78"}

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
                sec = SECTIONS.get(sec_key, {}); headers = sec.get("headers", [])
                colfn = sec.get("colfn", lambda ci, v: "background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.75);")
                accent = sec.get("col","#4caf78")
                th = "".join("<th style='padding:0.5rem 0.85rem;text-align:left;font-size:10px;font-weight:700;letter-spacing:0.07em;color:%s;white-space:nowrap;background:#0a1a10;border-bottom:2px solid %s44;border-right:1px solid rgba(255,255,255,0.04);'>%s</th>" % (accent,accent,h) for h in headers)
                body = ""
                for row in rows:
                    while len(row) < len(headers): row.append("")
                    row = row[:len(headers)]
                    cells = ""
                    for ci, cell in enumerate(row):
                        v = str(cell).strip(); sty = colfn(ci, v)
                        cells += "<td style='padding:0.45rem 0.85rem;font-size:11px;vertical-align:top;line-height:1.6;border-bottom:1px solid rgba(255,255,255,0.04);border-right:1px solid rgba(255,255,255,0.04);%s'>%s</td>" % (sty, v)
                    body += "<tr>%s</tr>" % cells
                return "<div style='overflow-x:auto;margin:0.6rem 0 1.5rem 0;border-radius:8px;border:1px solid %s33;'><table style='width:100%%;border-collapse:collapse;'><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div>" % (accent,th,body)

            def make_header(sec_key, title):
                sec = SECTIONS.get(sec_key, {"icon":"▸","col":"#4caf78"})
                return "<div style='display:flex;align-items:center;gap:0.55rem;margin:1.5rem 0 0.6rem 0;padding:0.55rem 0.9rem;background:rgba(255,255,255,0.025);border-radius:8px;border-left:3px solid %s;'><span style='font-size:1rem;'>%s</span><span style='font-size:12px;font-weight:800;color:%s;text-transform:uppercase;letter-spacing:0.1em;'>%s</span></div>" % (sec["col"],sec["icon"],sec["col"],title)

            lines = plan_text.split("\n"); output = ""; cur_sec = None; tbl_rows = []; in_tbl = False
            for line in lines:
                s = line.strip()
                if s.startswith("##") or s.startswith("# "):
                    if in_tbl and tbl_rows and cur_sec: output += make_table(cur_sec, tbl_rows)
                    tbl_rows = []; in_tbl = False; cur_sec = None
                    title = s.lstrip("#").strip()
                    matched = next((k for k in SECTIONS if k in title.upper()), None)
                    cur_sec = matched; output += make_header(matched or title.upper(), title)
                    if matched and SECTIONS[matched]["table"]: in_tbl = True
                elif in_tbl and "|" in s:
                    parts = [p.strip() for p in s.split("|") if p.strip()]
                    if parts and not all(set(p) <= set("-: ") for p in parts):
                        hdrs = SECTIONS.get(cur_sec,{}).get("headers",[])
                        if parts[0].upper() not in [h.upper() for h in hdrs]: tbl_rows.append(parts)
                elif in_tbl and s == "":
                    if tbl_rows and cur_sec: output += make_table(cur_sec, tbl_rows); tbl_rows = []
                    in_tbl = False
                elif not in_tbl and s:
                    if s.startswith("- ") or s.startswith("* ") or s.startswith("• "):
                        c = re.sub(r"^[-*•]\s*","",s)
                        output += "<div style='display:flex;gap:0.5rem;padding:0.22rem 0.2rem;color:rgba(255,255,255,0.75);font-size:12px;line-height:1.7;'><span style='color:#4caf78;flex-shrink:0;font-size:0.75rem;margin-top:0.1rem;'>▸</span><span>%s</span></div>" % c
                    elif re.match(r"^\d+\.\s", s):
                        m2 = re.match(r"^(\d+)\.\s*(.*)", s)
                        if m2: output += "<div style='display:flex;gap:0.5rem;padding:0.22rem 0.2rem;color:rgba(255,255,255,0.78);font-size:12px;line-height:1.7;'><span style='color:#4caf78;font-weight:700;flex-shrink:0;min-width:1.4rem;'>%s.</span><span>%s</span></div>" % (m2.group(1),m2.group(2))
                    else:
                        output += "<div style='color:rgba(255,255,255,0.7);font-size:12px;line-height:1.85;padding:0.1rem 0.2rem;'>%s</div>" % s
            if in_tbl and tbl_rows and cur_sec: output += make_table(cur_sec, tbl_rows)
            return output

        rendered = render_plan_with_tables(plan)
        st.markdown("<div style='background:#112218;border:1px solid rgba(76,175,120,0.2);border-radius:12px;padding:1.5rem 1.75rem;'>" + rendered + "</div>", unsafe_allow_html=True)
        st.success("✅ Plan generated — WBS from SOW · RAID & Tests from institutional memory.")

    with tab_wbs:
        label = "Sprint Board" if is_agile else "Work Breakdown Structure"
        st.markdown(f"<h2>🏃 {label} — {plan_name}</h2>", unsafe_allow_html=True)

        # ── Self-healing: if stored sprints have thin/placeholder stories, re-extract from plan ──
        placeholders = {'define','build','test','task 1','task 2','task 3','tbd','n/a','iteration 1'}
        def is_thin(sprint_list):
            if not sprint_list: return True
            total = sum(len(sp.get('stories',[])) for sp in sprint_list)
            real  = sum(
                1 for sp in sprint_list for s in sp.get('stories',[])
                if str(s).lower().strip() not in placeholders and len(str(s)) > 10
            )
            return total == 0 or real < total * 0.5

        if is_thin(sprints):
            with st.spinner("Extracting detailed tasks from plan…"):
                sprints = parse_wbs_into_sprints(plan, duration_weeks if 'duration_weeks' in dir() else 12, meth)
                st.session_state.wbs_structured = sprints

        if not sprints:
            st.info("No sprint data — regenerate the plan.")
        else:
            sum_cols = st.columns(min(len(sprints), 6))
            for i, sp in enumerate(sprints[:6]):
                with sum_cols[i]:
                    sp_name_short = sp.get('name','').split(':')[0] if ':' in sp.get('name','') else sp.get('name','')
                    st.markdown(f"""<div style="background:rgba(76,175,120,0.08);border:1px solid rgba(76,175,120,0.25);border-radius:8px;padding:0.6rem;text-align:center;">
                        <div style="font-size:9px;font-weight:700;color:#4caf78;text-transform:uppercase;letter-spacing:0.1em;">{sp.get('weeks','')}</div>
                        <div style="font-size:11px;font-weight:700;color:rgba(255,255,255,0.85);margin-top:0.2rem;line-height:1.3;">{sp_name_short}</div>
                    </div>""", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)
            scol1, scol2 = st.columns(2)
            for i, sp in enumerate(sprints):
                raw_tasks = sp.get("stories", sp.get("tasks", []))
                display_tasks = []
                for t in raw_tasks:
                    val = str(t.get("name", t.get("text", str(t)))) if isinstance(t, dict) else str(t)
                    val = val.strip()
                    if val and val.lower() not in placeholders and len(val) > 3:
                        display_tasks.append(val)
                with (scol1 if i % 2 == 0 else scol2):
                    bullet = '🔵' if is_agile else '▸'
                    deliverable = str(sp.get("deliverable", ""))
                    sow_ref     = str(sp.get("sow_ref", ""))
                    sp_name     = str(sp.get("name", ""))
                    sp_weeks    = str(sp.get("weeks", ""))
                    sp_goal     = str(sp.get("goal", ""))

                    # Build HTML as plain string concatenation — no nested f-strings or triple-quote issues
                    card = "<div style='background:var(--forest-mid);border:1px solid var(--border-green);border-radius:var(--radius-md);padding:0.9rem 1rem;margin-bottom:0.6rem;border-left:3px solid var(--green-bright);'>"
                    card += "<div style='font-size:12px;font-weight:700;color:var(--green-bright);margin-bottom:0.35rem;'>" + sp_name + " &nbsp;·&nbsp; " + sp_weeks + "</div>"
                    if sow_ref:
                        card += "<div style='font-size:9px;color:rgba(76,175,120,0.55);font-weight:600;margin-bottom:0.25rem;text-transform:uppercase;letter-spacing:0.08em;'>📎 " + sow_ref + "</div>"
                    card += "<div style='font-size:11px;color:var(--white-90);font-weight:600;margin-bottom:0.5rem;'>🎯 " + sp_goal + "</div>"
                    if display_tasks:
                        for t in display_tasks:
                            card += ("<div style='font-size:11px;color:var(--text-secondary);padding:0.22rem 0;"
                                     "border-bottom:1px solid rgba(255,255,255,0.04);display:flex;gap:0.5rem;align-items:flex-start;'>"
                                     "<span style='color:var(--green-bright);flex-shrink:0;margin-top:0.05rem;'>" + bullet + "</span>"
                                     "<span>" + t + "</span></div>")
                    else:
                        card += "<div style='font-size:11px;color:rgba(255,255,255,0.25);font-style:italic;'>No tasks extracted</div>"
                    if deliverable:
                        card += ("<div style='font-size:10px;color:#4caf78;margin-top:0.5rem;font-weight:600;"
                                 "padding-top:0.4rem;border-top:1px solid rgba(76,175,120,0.15);'>✓ " + deliverable + "</div>")
                    card += "</div>"
                    st.markdown(card, unsafe_allow_html=True)

    with tab_sow:
        st.markdown("<h2>📄 SOW Analysis</h2>", unsafe_allow_html=True)
        sow_req = st.session_state.sow_requirements or ""
        if sow_req:
            SOW_ICONS = {
                "PROJECT OBJECTIVE": ("🎯","#4caf78"), "KEY DELIVERABLES": ("📦","#4caf78"),
                "ACCEPTANCE CRITERIA": ("✅","#4caf78"), "FUNCTIONAL REQUIREMENTS": ("⚙️","#5bc8e8"),
                "NON-FUNCTIONAL REQUIREMENTS": ("🔧","#5bc8e8"), "OUT OF SCOPE": ("🚫","#e08c5e"),
                "CONSTRAINTS": ("⛓️","#e0c45e"), "RISKS & ASSUMPTIONS": ("⚠️","#e07070"),
                "RISKS AND ASSUMPTIONS": ("⚠️","#e07070"),
            }
            def render_sow_analysis(text):
                lines = text.split('\n'); html = ""; in_section = False
                for line in lines:
                    s = line.strip()
                    if not s:
                        if in_section: html += "<div style='height:0.35rem;'></div>"
                        continue
                    is_header = False; header_text = ""
                    if re.match(r'^\d+\.', s): header_text = re.sub(r'^\d+\.\s*','',s).strip(); is_header = True
                    elif s.startswith('##') or s.startswith('# '): header_text = s.lstrip('#').strip(); is_header = True
                    if is_header:
                        if in_section: html += "</div>"
                        ht_upper = header_text.upper()
                        icon, color = "📌", "#4caf78"
                        for key, (ic, co) in SOW_ICONS.items():
                            if key in ht_upper: icon, color = ic, co; break
                        html += (f"<div style='margin-bottom:0.9rem;'><div style='display:flex;align-items:center;gap:0.55rem;padding:0.6rem 0.9rem;margin-bottom:0.5rem;background:rgba(255,255,255,0.03);border-radius:8px;border-left:3px solid {color};'>"
                                 f"<span style='font-size:1rem;'>{icon}</span><span style='font-size:12px;font-weight:700;color:{color};text-transform:uppercase;letter-spacing:0.09em;'>{header_text}</span></div><div style='padding-left:0.5rem;'>")
                        in_section = True
                    elif s.startswith('-') or s.startswith('•') or s.startswith('*'):
                        content = re.sub(r'^[-•*]\s*','',s)
                        html += (f"<div style='display:flex;gap:0.5rem;padding:0.22rem 0;color:rgba(255,255,255,0.78);font-size:12px;line-height:1.65;'>"
                                 f"<span style='color:#4caf78;flex-shrink:0;margin-top:0.1rem;'>▸</span><span>{content}</span></div>")
                    else:
                        html += f"<div style='color:rgba(255,255,255,0.72);font-size:12px;line-height:1.8;padding:0.12rem 0;'>{s}</div>"
                if in_section: html += "</div></div>"
                return html
            st.markdown("<div style='background:#112218;border:1px solid rgba(76,175,120,0.18);border-radius:12px;padding:1.5rem 1.75rem;'>" + render_sow_analysis(sow_req) + "</div>", unsafe_allow_html=True)
        else:
            st.info("SOW analysis not available — regenerate the plan.")

    with tab_qa:
        st.markdown("<h2>💬 Ask Cortex About This Plan</h2>", unsafe_allow_html=True)
        suggestions = ["What are the top 3 risks?","Write a kick-off email.","Sprint 1 in detail?","Generate a RACI matrix.","What compliance tasks are needed?","Rewrite exec summary simply.","What is the critical path?","Suggest risk mitigations."]
        sc1,sc2,sc3 = st.columns(3)
        for i, s in enumerate(suggestions):
            with [sc1,sc2,sc3][i%3]:
                if st.button(s, key=f"sugg_{i}"): st.session_state['pending_question'] = s
        st.markdown("<hr>", unsafe_allow_html=True)
        if 'qa_input_value' not in st.session_state: st.session_state['qa_input_value'] = ''
        if st.session_state.get('pending_question'): st.session_state['qa_input_value'] = st.session_state.pop('pending_question')
        user_q = st.text_input("Your question:", value=st.session_state['qa_input_value'], placeholder="e.g. What dependencies could block Sprint 2?")
        if st.button("🤖 Ask Cortex", key="ask_btn"):
            q = (user_q or '').strip()
            if not q: st.warning("Please type a question first.")
            else:
                sow_ctx = (st.session_state.sow_requirements or "")[:600]
                wbs_ctx = "\n".join([s.get('name','') + " (" + s.get('weeks','') + "): " + s.get('goal','') for s in (st.session_state.wbs_structured or [])])[:500]
                qa_prompt = ("You are a senior PMO consultant. Answer using the project context.\n\nProject: " + plan_name +
                             "\nSOW:\n" + sow_ctx + "\n\nWBS:\n" + wbs_ctx + "\n\nPlan:\n" + plan[:1800] + "\n\nQuestion: " + q +
                             "\n\nAnswer specifically and practically.")
                with st.spinner("Cortex is thinking…"):
                    ans = cortex_call(qa_prompt, "Q&A")
                if ans:
                    st.session_state.qa_history.append({"q": q, "a": ans})
                    st.session_state['qa_input_value'] = ''; st.rerun()
                else: st.error("No response.")
        if st.session_state.qa_history:
            st.markdown("<div style='font-size:10px;font-weight:700;color:rgba(255,255,255,0.3);text-transform:uppercase;letter-spacing:0.12em;margin:0.75rem 0 0.5rem;border-bottom:1px solid rgba(255,255,255,0.06);'>💬 Conversation History</div>", unsafe_allow_html=True)
            for qa in reversed(st.session_state.qa_history):
                st.markdown("<div style='background:rgba(76,175,120,0.08);border:1px solid rgba(76,175,120,0.25);border-left:3px solid #4caf78;border-radius:8px;padding:0.65rem 1rem;margin-bottom:0.3rem;'><div style='font-size:9px;font-weight:700;color:rgba(76,175,120,0.55);text-transform:uppercase;margin-bottom:0.2rem;'>You asked</div><div style='font-size:12px;color:#f0f5f2;font-weight:600;'>❓ " + qa['q'] + "</div></div>", unsafe_allow_html=True)
                st.markdown("<div style='background:#172a1e;border:1px solid rgba(255,255,255,0.07);border-left:3px solid #5b8ce8;border-radius:0 8px 8px 8px;padding:0.85rem 1.1rem;margin-bottom:1rem;'><div style='font-size:9px;font-weight:700;color:rgba(91,140,232,0.6);text-transform:uppercase;margin-bottom:0.5rem;'>🤖 Cortex</div><div style='font-size:12px;color:rgba(255,255,255,0.82);line-height:1.8;'>" + qa['a'].replace('\n','<br>') + "</div></div>", unsafe_allow_html=True)
            if st.button("🗑️ Clear History", key="clear_qa"): st.session_state.qa_history = []; st.rerun()

    with tab_risks:
        st.markdown("<h2>⚠️ Risk Analyser</h2>", unsafe_allow_html=True)
        if st.button("🔍 Extract & Score Risks with Cortex"):
            rp = (f"Extract all risks from this plan for \"{plan_name}\".\nPlan:\n{plan[:2500]}\n\n"
                  f"Return ONLY valid JSON array:\n[{{\"risk\":\"title\",\"type\":\"Technical/Resource/Timeline/Budget/Compliance\","
                  f"\"likelihood\":\"High/Medium/Low\",\"impact\":\"High/Medium/Low\",\"mitigation\":\"one sentence\",\"owner\":\"role\"}}]")
            with st.spinner(""):
                raw = cortex_call(rp, "Risk Analyser")
                try:
                    raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip(); risks = json.loads(raw)
                except:
                    risks = [{"risk":"Data access permissions","type":"Technical","likelihood":"High","impact":"High","mitigation":"Pre-provision IAM roles before Sprint 1","owner":"Platform Engineer"},
                             {"risk":"Scope creep","type":"Timeline","likelihood":"Medium","impact":"High","mitigation":"Lock scope after Sprint 2","owner":"PM"},
                             {"risk":"Key person dependency","type":"Resource","likelihood":"Medium","impact":"Medium","mitigation":"Cross-train 2 members","owner":"Tech Lead"}]
            lbg={"High":"rgba(224,92,92,0.12)","Medium":"rgba(240,165,0,0.1)","Low":"rgba(76,175,120,0.1)"}
            lco={"High":"rgba(224,92,92,1)","Medium":"rgba(240,165,0,1)","Low":"#4caf78"}
            for r in risks:
                lh=r.get('likelihood','Medium'); im=r.get('impact','Medium')
                bg=lbg.get(lh,lbg['Medium']); co=lco.get(lh,lco['Medium'])
                st.markdown(f"<div style='background:{bg};border:1px solid {co}33;border-radius:10px;padding:0.85rem 1rem;margin-bottom:0.5rem;'><div style='display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:0.3rem;'><div style='font-weight:700;font-size:12px;color:rgba(255,255,255,0.9);'>{r.get('risk','')}</div><div style='display:flex;gap:0.35rem;'><span style='background:{lbg.get(lh)};color:{lco.get(lh)};border:1px solid {lco.get(lh)}44;font-size:9px;font-weight:700;padding:0.15rem 0.5rem;border-radius:4px;text-transform:uppercase;'>L:{lh}</span><span style='background:{lbg.get(im)};color:{lco.get(im)};border:1px solid {lco.get(im)}44;font-size:9px;font-weight:700;padding:0.15rem 0.5rem;border-radius:4px;text-transform:uppercase;'>I:{im}</span><span style='background:rgba(255,255,255,0.07);color:rgba(255,255,255,0.5);font-size:9px;font-weight:700;padding:0.15rem 0.5rem;border-radius:4px;'>{r.get('type','')}</span></div></div><div style='font-size:11px;color:rgba(255,255,255,0.6);margin-bottom:0.2rem;'>🛡️ {r.get('mitigation','')}</div><div style='font-size:10px;color:rgba(255,255,255,0.3);'>Owner: {r.get('owner','')}</div></div>", unsafe_allow_html=True)

    with tab_export:
        st.markdown("<h2>📤 Export & Save</h2>", unsafe_allow_html=True)
        ec1,ec2,ec3 = st.columns(3)
        with ec1:
            st.markdown("<div class='plan-section-title'>💾 Save to Snowflake</div>", unsafe_allow_html=True)
            save_status = st.selectbox("Status", ["In Progress","Planning","On Hold"])
            st.markdown("<div style='background:rgba(240,165,0,0.08);border:1px solid rgba(240,165,0,0.3);border-left:3px solid #f0a500;border-radius:8px;padding:0.7rem 0.9rem;margin-bottom:0.6rem;'><div style='font-size:10px;font-weight:700;color:#f0a500;text-transform:uppercase;margin-bottom:0.35rem;'>⚠️ Review Gate</div><div style='font-size:11px;color:rgba(255,255,255,0.75);'>Has the Project Plan been reviewed before saving?</div></div>", unsafe_allow_html=True)
            review_confirmed = st.radio("Plan Reviewed?", options=["— Select —","✅ Yes — Plan reviewed, save it","❌ No — Not yet reviewed"], index=0, key="save_review_gate", label_visibility="collapsed")
            if review_confirmed == "✅ Yes — Plan reviewed, save it":
                if st.button("💾 Save to Projects Table", key="save_btn"):
                    try:
                        esc=(plan or '').replace("'","''"); pn=plan_name.replace("'","''")
                        wbs_part=esc[:3000]; depl_part=esc[3000:5000] if len(esc)>3000 else ''
                        session.sql(f"INSERT INTO projects (project_name,description,status,wbs_summary,risk_log_summary,test_cases_summary,deployment_plan) VALUES('{pn}','AI-generated plan — reviewed','{save_status}','{wbs_part}','','','{depl_part}')").collect()
                        st.success("✅ Project saved to DB.")
                        st.session_state.past_projects_df = load_past_projects(); st.session_state.imported_csv_names.add(plan_name); st.rerun()
                    except Exception as e: st.error(f"Save failed: {e}")
            elif review_confirmed == "❌ No — Not yet reviewed":
                st.markdown("<div style='background:rgba(224,92,92,0.1);border:1px solid rgba(224,92,92,0.3);border-radius:8px;padding:0.6rem 0.9rem;'><div style='font-size:11px;color:#e05c5c;font-weight:600;'>🚫 Save blocked.</div><div style='font-size:11px;color:rgba(255,255,255,0.6);'>Please review before saving.</div></div>", unsafe_allow_html=True)
        with ec2:
            st.markdown("<div class='plan-section-title'>📄 Download Text</div>", unsafe_allow_html=True)
            st.download_button("⬇ Download .txt", data=plan or "", file_name=f"{plan_name.replace(' ','_')}_plan.txt", mime="text/plain")
        with ec3:
            st.markdown("<div class='plan-section-title'>📊 Download Excel</div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:11px;color:rgba(255,255,255,0.4);margin-bottom:0.5rem;'>5 sheets: Summary · WBS/Sprints · RAID Log · Test Cases · Timeline</div>", unsafe_allow_html=True)
            if st.button("📎 Build Excel Workbook"):
                with st.spinner("Building Excel..."):
                    sp = sprints if sprints else parse_wbs_into_sprints(plan, 12, meth)
                    excel_bytes = build_excel(plan, plan_name, sp, meth)
                st.download_button(label="⬇ Download .xlsx", data=excel_bytes, file_name=f"{plan_name.replace(' ','_')}_ProjectPlan.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with st.expander("🔍 Preview plan text"):
            safe_plan = (plan or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            st.markdown("<div class='plan-preview-wrap'><pre>" + safe_plan + "</pre></div>", unsafe_allow_html=True)

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

st.markdown("""<div class='footer'>Powered by <span>Snowflake Cortex</span> &nbsp;·&nbsp; PMO Intelligence &nbsp;·&nbsp; Institutional Memory → Faster Delivery</div>""", unsafe_allow_html=True)
