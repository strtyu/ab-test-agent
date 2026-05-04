from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from ab_agent.bigquery.query_builder import _strip_channel
from ab_agent.core.models import ABTestConfig
from ab_agent.stats.engine import ABS_METRICS, BREAKDOWN_DIMS, REL_METRICS
from ab_agent.visualization.chart_library import _shorten, calc_delta, fmt_value, metric_direction

# ── Row builder ──────────────────────────────────────────────────────────────

_KNOWN_COLS = {
    "user_id", "timestamp", "split", "geo", "channel", "payment_method",
    "subscription", "utm_source", "upsell_order", "ups_view", "ups_ttp",
    "ups_purched", "purch_amount", "purch_count", "unsub12h", "ticket_count",
    "diff_ms", "version", "upsell_group",
}


def build_rows_for_dashboard(ctrl_df: pd.DataFrame, test_df: pd.DataFrame) -> List[Dict]:
    rows = []
    for grp, df in [("ctrl", ctrl_df), ("test", test_df)]:
        extra_cols = [c for c in df.columns if c not in _KNOWN_COLS]
        for _, r in df.iterrows():
            date_val = ""
            ts = r.get("timestamp")
            if ts is not None and pd.notna(ts):
                try:
                    date_val = pd.to_datetime(ts).strftime("%Y-%m-%d")
                except Exception:
                    pass

            def sv(col):
                v = r.get(col)
                s = str(v) if v is not None and pd.notna(v) else ""
                return "" if s in ("nan", "None", "NaT", "<NA>") else s[:60]

            def nv(col, default=0.0):
                v = r.get(col)
                try:
                    f = float(v) if v is not None and pd.notna(v) else default
                    return default if f != f else f
                except (TypeError, ValueError):
                    return default

            row = {
                "uid":   sv("user_id"),
                "grp":   grp,
                "split": sv("split"),
                "date":  date_val,
                "geo":   sv("geo"),
                "ch":    sv("channel"),
                "pay":   sv("payment_method"),
                "sub":   sv("subscription"),
                "utm":   sv("utm_source"),
                "ord":   sv("upsell_order"),
                "view":  int(nv("ups_view")),
                "ttp":   int(nv("ups_ttp")),
                "purch": int(nv("ups_purched")),
                "amt":   nv("purch_amount"),
                "cnt":   nv("purch_count"),
                "unsub": int(nv("unsub12h")),
                "tick":  int(nv("ticket_count") != 0),
                "diff":  nv("diff_ms", None) if pd.notna(r.get("diff_ms", None)) else None,
            }
            if extra_cols:
                row["extra"] = {c: nv(c) for c in extra_cols}
            rows.append(row)
    return rows


# ── HTML template ─────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TMPL_TEST_NAME</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif;
     background:#F1F5F9;color:#1E293B;font-size:13px}
.hdr{background:#0F1B35;padding:14px 24px;display:flex;align-items:center;
     justify-content:space-between;gap:16px;flex-wrap:wrap}
.hdr h1{color:#fff;font-size:17px;font-weight:700;margin:0}
.hdr p{color:#94A3B8;font-size:11px;margin-top:3px}
.back-btn{display:inline-flex;align-items:center;gap:5px;color:#94A3B8;
          font-size:12px;text-decoration:none;padding:5px 10px;border:1px solid #2D3F5E;
          border-radius:6px;transition:all .12s;white-space:nowrap;flex-shrink:0}
.back-btn:hover{color:#fff;border-color:#4A6FA5;background:rgba(255,255,255,.06)}
.legend{display:flex;gap:18px;flex-shrink:0}
.leg{display:flex;align-items:center;gap:6px;color:#CBD5E1;font-size:12px;white-space:nowrap}
.dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.layout{display:flex;min-height:calc(100vh - 52px)}
.main{flex:1;padding:16px 20px;min-width:0;overflow-x:auto}
.sidebar{width:232px;flex-shrink:0;background:#fff;border-left:1px solid #E2E8F0;
         padding:16px 14px;overflow-y:auto;max-height:calc(100vh - 52px);
         position:sticky;top:0;align-self:flex-start}
.controls-bar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;
              margin-bottom:12px;background:#fff;border:1px solid #E2E8F0;
              border-radius:8px;padding:10px 14px}
.ctrl-group{display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.ctrl-sep{width:1px;height:20px;background:#E2E8F0;margin:0 4px}
.ctrl-lbl{font-size:10px;font-weight:700;color:#94A3B8;text-transform:uppercase;
          letter-spacing:.05em;white-space:nowrap}
.mode-btn,.dim-btn{padding:4px 11px;border:1px solid #CBD5E1;border-radius:20px;
                   background:#F8FAFC;color:#64748B;font-size:11px;font-weight:500;
                   cursor:pointer;transition:all .12s;white-space:nowrap}
.mode-btn:hover,.dim-btn:hover{border-color:#1664F5;color:#1664F5;background:#EBF3FF}
.mode-btn.active{border-color:#1664F5;background:#1664F5;color:#fff}
.dim-btn.active{border-color:#7C3AED;background:#7C3AED;color:#fff}
.row-count{font-size:11px;color:#94A3B8;white-space:nowrap}
.card{background:#fff;border-radius:8px;border:1px solid #E2E8F0;
      box-shadow:0 1px 3px rgba(0,0,0,.04);overflow:hidden;margin-bottom:14px}
.card-hdr{padding:10px 16px;font-size:11px;font-weight:700;color:#334155;
          border-bottom:1px solid #F1F5F9;display:flex;align-items:center;gap:7px}
.card-hdr::before{content:'';width:3px;height:12px;background:#1664F5;border-radius:2px}
.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:520px}
thead th{background:#0F1B35;color:#fff;padding:8px 12px;text-align:center;
         font-size:10px;font-weight:600;letter-spacing:.04em;white-space:nowrap}
thead th.rlh{text-align:left;min-width:160px}
tbody td{padding:7px 12px;text-align:center;border-bottom:1px solid #F1F5F9;white-space:nowrap;font-size:12px}
tbody td.rlh{text-align:left;min-width:160px;display:flex;align-items:center;gap:6px;font-weight:600}
tbody tr:last-child td{border-bottom:none}
.row-c td{background:#F0F6FF}
.row-t td{background:#FFF8F0}
.row-d td{background:#FAFAFA;font-size:11px}
.row-sep td{height:6px;background:#F1F5F9;border:none;padding:0}
.no-data{padding:32px;text-align:center;color:#94A3B8}
/* Sidebar */
.sf-group{margin-bottom:16px}
.sf-lbl{font-size:10px;font-weight:700;color:#94A3B8;text-transform:uppercase;
        letter-spacing:.05em;margin-bottom:6px;display:flex;justify-content:space-between;
        align-items:center}
.sf-lbl button{background:none;border:none;color:#1664F5;font-size:10px;cursor:pointer;padding:0}
.ms-list{max-height:140px;overflow-y:auto;border:1px solid #E2E8F0;border-radius:6px;
         background:#F8FAFC}
.ms-item{display:flex;align-items:center;gap:7px;padding:5px 9px;cursor:pointer;
         font-size:12px;color:#334155;border-bottom:1px solid #F1F5F9}
.ms-item:last-child{border-bottom:none}
.ms-item:hover{background:#EBF3FF}
.ms-item input{cursor:pointer;accent-color:#1664F5;flex-shrink:0}
.ms-empty{font-size:11px;color:#94A3B8;padding:8px;text-align:center}
/* Date slider */
.date-display{font-size:11px;color:#475569;margin-bottom:8px;min-height:16px}
.range-wrap{position:relative;height:28px;margin:0 4px}
.range-track{position:absolute;top:50%;width:100%;height:4px;
             background:#E2E8F0;border-radius:2px;transform:translateY(-50%)}
.range-fill{position:absolute;top:50%;height:4px;background:#1664F5;
            border-radius:2px;transform:translateY(-50%)}
.range-wrap input[type=range]{position:absolute;width:100%;top:50%;
  transform:translateY(-50%);-webkit-appearance:none;appearance:none;
  background:transparent;height:28px;pointer-events:none;outline:none;margin:0}
.range-wrap input[type=range]::-webkit-slider-thumb{pointer-events:all;
  -webkit-appearance:none;width:16px;height:16px;border-radius:50%;
  background:#1664F5;border:2px solid #fff;box-shadow:0 1px 3px rgba(0,0,0,.2);cursor:pointer}
.range-wrap input[type=range]::-moz-range-thumb{pointer-events:all;
  width:14px;height:14px;border-radius:50%;background:#1664F5;
  border:2px solid #fff;box-shadow:0 1px 3px rgba(0,0,0,.2);cursor:pointer}
.date-ticks{display:flex;justify-content:space-between;font-size:10px;
            color:#94A3B8;margin-top:4px}
.clear-btn{width:100%;padding:7px;border:1px solid #CBD5E1;border-radius:6px;
           background:#F8FAFC;color:#64748B;font-size:12px;cursor:pointer;margin-top:4px}
.clear-btn:hover{border-color:#1664F5;color:#1664F5}
.footer{text-align:center;padding:12px;color:#94A3B8;font-size:11px;
        border-top:1px solid #E2E8F0;background:#fff}
.bg{background:#DCFCE7;color:#15803D;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}
.bb{background:#FEE2E2;color:#B91C1C;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}
.bn{background:#F1F5F9;color:#64748B;padding:2px 7px;border-radius:4px;font-size:11px}
/* ── Chat ── */
.chat-fab{position:fixed;bottom:24px;right:250px;background:#1664F5;color:#fff;
  border:none;border-radius:24px;padding:9px 16px;font-size:12px;font-weight:600;
  cursor:pointer;box-shadow:0 4px 12px rgba(22,100,245,.35);z-index:200;
  display:flex;align-items:center;gap:6px;transition:background .12s}
.chat-fab:hover{background:#1255D6}
.chat-panel{position:fixed;bottom:70px;right:250px;width:380px;max-height:520px;
  background:#fff;border-radius:12px;border:1px solid #E2E8F0;
  box-shadow:0 8px 32px rgba(0,0,0,.14);display:none;flex-direction:column;z-index:199}
.chat-panel.open{display:flex}
.chat-phdr{padding:12px 16px;border-bottom:1px solid #F1F5F9;display:flex;
  justify-content:space-between;align-items:center;font-weight:600;font-size:13px;color:#0F1B35}
.chat-close{background:none;border:none;color:#94A3B8;font-size:16px;cursor:pointer;
  padding:0 2px;line-height:1}
.chat-close:hover{color:#1E293B}
.chat-msgs{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:8px;min-height:120px}
.chat-msg{max-width:90%;padding:8px 11px;border-radius:10px;font-size:12px;line-height:1.5;word-break:break-word}
.chat-msg.user{align-self:flex-end;background:#1664F5;color:#fff;border-radius:10px 10px 2px 10px}
.chat-msg.ai{align-self:flex-start;background:#F1F5F9;color:#1E293B;border-radius:10px 10px 10px 2px}
.chat-msg.thinking{align-self:flex-start;color:#94A3B8;font-size:11px;font-style:italic;background:none;padding:4px 0}
.chat-irow{padding:10px;border-top:1px solid #F1F5F9;display:flex;gap:8px;align-items:flex-end}
.chat-irow textarea{flex:1;border:1px solid #CBD5E1;border-radius:8px;padding:7px 10px;
  font-size:12px;resize:none;outline:none;font-family:inherit;max-height:80px}
.chat-irow textarea:focus{border-color:#1664F5}
#chat-send{padding:8px 12px;background:#1664F5;color:#fff;border:none;border-radius:8px;
  cursor:pointer;font-size:14px;flex-shrink:0;transition:background .12s}
#chat-send:hover{background:#1255D6}
#chat-send:disabled{background:#CBD5E1;cursor:default}
.chat-modes{display:flex;border-bottom:1px solid #E2E8F0}
.chat-modes .mode-btn{flex:1;padding:7px 4px;font-size:10.5px;font-weight:600;border:none;background:none;
  cursor:pointer;color:#94A3B8;border-bottom:2px solid transparent;transition:all .15s;line-height:1.3}
.chat-modes .mode-btn.active{color:#1664F5;border-bottom-color:#1664F5;background:none;border-color:transparent}
.chat-modes .mode-btn:hover:not(.active){color:#475569}
.qr-wrap{overflow-x:auto;max-width:100%;margin-top:2px}
.qr-table{border-collapse:collapse;font-size:10.5px;min-width:100%}
.qr-table th{background:#E2E8F0;padding:4px 7px;text-align:left;white-space:nowrap;font-weight:700;color:#334155}
.qr-table td{padding:3px 7px;border-bottom:1px solid #F1F5F9;color:#475569;white-space:nowrap}
.qr-table tr:last-child td{border-bottom:none}
/* ── Add metric modal ── */
.mm-overlay{position:fixed;inset:0;background:rgba(0,0,0,.4);display:none;
  align-items:center;justify-content:center;z-index:300}
.mm-overlay.open{display:flex}
.mm-box{background:#fff;border-radius:12px;padding:24px;max-width:420px;width:90%;
  box-shadow:0 8px 32px rgba(0,0,0,.18)}
.mm-box h3{font-size:15px;font-weight:700;margin-bottom:12px;color:#0F1B35}
.mm-row{margin-bottom:10px;font-size:13px;color:#334155}
.mm-code{background:#F8FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:8px;
  font-size:11px;color:#475569;word-break:break-all;font-family:monospace}
.mm-cb{display:flex;align-items:center;gap:8px;cursor:pointer;font-size:12px;color:#334155;
  user-select:none;margin-top:4px}
.mm-cb input{accent-color:#1664F5;cursor:pointer}
.mm-btns{display:flex;gap:8px;margin-top:16px}
.mm-ok{flex:1;padding:9px;background:#1664F5;color:#fff;border:none;border-radius:8px;
  font-size:13px;font-weight:600;cursor:pointer}
.mm-ok:hover{background:#1255D6}
.mm-cancel{flex:1;padding:9px;background:#F1F5F9;color:#64748B;border:1px solid #E2E8F0;
  border-radius:8px;font-size:13px;cursor:pointer}
.mm-cancel:hover{background:#E2E8F0}
</style>
</head>
<body>
<div class="hdr">
  <a class="back-btn" href="javascript:history.back()">&#8592; Back</a>
  <div><h1>TMPL_TEST_NAME</h1><p>Release: TMPL_RELEASE UTC</p></div>
  <div class="legend">
    <div class="leg"><div class="dot" style="background:#1664F5"></div>Control: TMPL_CTRL</div>
    <div class="leg"><div class="dot" style="background:#F77F00"></div>Test: TMPL_TEST</div>
  </div>
</div>

<div class="layout">
<div class="main">
  <div class="controls-bar">
    <div class="ctrl-group">
      <span class="ctrl-lbl">View&nbsp;by</span>
      <button class="mode-btn active" data-mode="group">Ctrl / Test</button>
      <button class="mode-btn" data-mode="version">By Version</button>
    </div>
    <div class="ctrl-sep"></div>
    <div class="ctrl-group">
      <span class="ctrl-lbl">+&nbsp;Split&nbsp;by</span>
      <button class="dim-btn" data-dim="">None</button>
      <button class="dim-btn" data-dim="ord">Order</button>
      <button class="dim-btn" data-dim="geo">Geo</button>
      <button class="dim-btn" data-dim="ch">Channel</button>
      <button class="dim-btn" data-dim="sub">Subscription</button>
    </div>
    <div class="ctrl-group" style="margin-left:auto">
      <span class="row-count" id="row-count"></span>
    </div>
  </div>

  <div class="card"><div class="card-hdr">Absolute Metrics</div>
    <div class="tbl-wrap" id="t-abs"></div></div>
  <div class="card"><div class="card-hdr">Relative Metrics</div>
    <div class="tbl-wrap" id="t-rel"></div></div>
</div>

<div class="sidebar">
  <div class="sf-group" id="sfg-date">
    <div class="sf-lbl">Date range</div>
    <div class="date-display" id="date-disp">All dates</div>
    <div class="range-wrap" id="range-wrap">
      <div class="range-track"></div>
      <div class="range-fill" id="range-fill"></div>
      <input type="range" id="dmin" min="0" value="0">
      <input type="range" id="dmax" value="100">
    </div>
    <div class="date-ticks"><span id="d-lo-lbl"></span><span id="d-hi-lbl"></span></div>
  </div>

  <div class="sf-group" id="sfg-geo">
    <div class="sf-lbl">Geo <button onclick="clearFilter('geo')">clear</button></div>
    <div class="ms-list" id="ms-geo"></div>
  </div>
  <div class="sf-group" id="sfg-ch">
    <div class="sf-lbl">Channel <button onclick="clearFilter('ch')">clear</button></div>
    <div class="ms-list" id="ms-ch"></div>
  </div>
  <div class="sf-group" id="sfg-pay">
    <div class="sf-lbl">Payment <button onclick="clearFilter('pay')">clear</button></div>
    <div class="ms-list" id="ms-pay"></div>
  </div>
  <div class="sf-group" id="sfg-sub">
    <div class="sf-lbl">Subscription <button onclick="clearFilter('sub')">clear</button></div>
    <div class="ms-list" id="ms-sub"></div>
  </div>
  <div class="sf-group" id="sfg-utm">
    <div class="sf-lbl">UTM Source <button onclick="clearFilter('utm')">clear</button></div>
    <div class="ms-list" id="ms-utm"></div>
  </div>

  <button class="clear-btn" onclick="clearAll()">Clear all filters</button>
</div>
</div>

<div class="footer">
  <span class="bg">▲ test better</span> &nbsp;
  <span class="bb">▼ test worse</span> &nbsp;
  <span class="bn">— no data</span>
</div>

<script>
const ROWS=TMPL_ROWS, CTRL_V=TMPL_CTRL_VERSIONS, TEST_V=TMPL_TEST_VERSIONS;
const CTRL_L="TMPL_CTRL", TEST_L="TMPL_TEST";
const TEST_ID="TMPL_TEST_ID";

const ABS_M=[
  {k:"view_u",  l:"Viewers",        f:"int",   hi:true},
  {k:"ttp_u",   l:"TTP clicks",     f:"int",   hi:true},
  {k:"purch_u", l:"Purchases",      f:"int",   hi:true},
  {k:"revenue", l:"Revenue",        f:"money", hi:true},
  {k:"purch_n", l:"Purch count",    f:"int",   hi:true},
  {k:"unsub_u", l:"Unsub \u226412h",f:"int",   hi:false},
  {k:"tick_u",  l:"Tickets",        f:"int",   hi:false},
  {k:"med_ttp", l:"Median TTP (s)", f:"f1",    hi:false},
];
const REL_M=[
  {k:"ttp_r",   l:"TTP rate",     f:"pct", hi:true},
  {k:"close_r", l:"Close rate",   f:"pct", hi:true},
  {k:"cvr",     l:"CVR",          f:"pct", hi:true},
  {k:"ppv",     l:"Purch/Viewer", f:"f4",  hi:true},
  {k:"unsub_r", l:"Unsub rate",   f:"pct", hi:false},
  {k:"tick_r",  l:"Ticket rate",  f:"pct", hi:false},
];

const CUSTOM_M_DEFS=TMPL_CUSTOM_METRICS;
CUSTOM_M_DEFS.forEach(cm=>{
  (cm.type==='abs'?ABS_M:REL_M).push({k:cm.k,l:cm.l,f:cm.f,hi:cm.hi});
});

// State
let mode="group", dim="", dateMin=0, dateMax=0;
const filt={geo:new Set(),ch:new Set(),pay:new Set(),sub:new Set(),utm:new Set()};

// Sorted unique dates
const allDates=[...new Set(ROWS.map(r=>r.date).filter(d=>d))].sort();
dateMax=allDates.length>0?allDates.length-1:0;

// ── Date slider ──────────────────────────────────────────────────────────────
(function initSlider(){
  const wrap=document.getElementById('range-wrap');
  const lo=document.getElementById('dmin'), hi=document.getElementById('dmax');
  const fill=document.getElementById('range-fill');
  const disp=document.getElementById('date-disp');
  const loLbl=document.getElementById('d-lo-lbl'), hiLbl=document.getElementById('d-hi-lbl');
  const n=allDates.length;
  if(!n){document.getElementById('sfg-date').style.display='none';return;}
  lo.max=hi.max=n-1; lo.value=0; hi.value=n-1;

  function upd(){
    let l=+lo.value,h=+hi.value;
    if(l>h){if(document.activeElement===lo)lo.value=l=h;else hi.value=h=l;}
    dateMin=l;dateMax=h;
    const pct=v=>((v/Math.max(n-1,1))*100).toFixed(1)+'%';
    fill.style.left=pct(l);fill.style.width=((h-l)/Math.max(n-1,1)*100).toFixed(1)+'%';
    const ld=allDates[l]||'',hd=allDates[h]||'';
    disp.textContent=ld===hd?ld:`${ld} – ${hd}`;
    loLbl.textContent=ld;hiLbl.textContent=hd;
    render();
  }
  lo.addEventListener('input',upd);hi.addEventListener('input',upd);
  upd();
})();

// ── Multi-select filters ─────────────────────────────────────────────────────
const FLDS=[
  {id:'ms-geo',sfg:'sfg-geo',  field:'geo'},
  {id:'ms-ch', sfg:'sfg-ch',   field:'ch'},
  {id:'ms-pay',sfg:'sfg-pay',  field:'pay'},
  {id:'ms-sub',sfg:'sfg-sub',  field:'sub'},
  {id:'ms-utm',sfg:'sfg-utm',  field:'utm'},
];
(function initFilters(){
  FLDS.forEach(({id,sfg,field})=>{
    const vals=[...new Set(ROWS.map(r=>r[field]).filter(v=>v&&v!=='nan'))].sort();
    const el=document.getElementById(id);
    if(!vals.length){document.getElementById(sfg).style.display='none';return;}
    el.innerHTML=vals.map(v=>
      `<label class="ms-item"><input type="checkbox" data-f="${field}" value="${v}" onchange="onFilt(this)"> <span>${v}</span></label>`
    ).join('');
  });
})();

function onFilt(cb){
  const f=cb.dataset.f;
  cb.checked?filt[f].add(cb.value):filt[f].delete(cb.value);
  render();
}
function clearFilter(f){
  filt[f].clear();
  document.querySelectorAll(`input[data-f="${f}"]`).forEach(cb=>cb.checked=false);
  render();
}
function clearAll(){
  Object.keys(filt).forEach(k=>{filt[k].clear();});
  document.querySelectorAll('.ms-item input').forEach(cb=>cb.checked=false);
  const lo=document.getElementById('dmin'),hi=document.getElementById('dmax');
  lo.value=0;hi.value=allDates.length-1;lo.dispatchEvent(new Event('input'));
}

// ── Mode / dim controls ──────────────────────────────────────────────────────
document.querySelectorAll('.controls-bar .mode-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.controls-bar .mode-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');mode=btn.dataset.mode;render();
  });
});
document.querySelectorAll('.dim-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    if(btn.dataset.dim===dim&&dim!==''){
      dim='';document.querySelectorAll('.dim-btn').forEach(b=>b.classList.remove('active'));
      document.querySelector('.dim-btn[data-dim=""]').classList.add('active');
    }else{
      document.querySelectorAll('.dim-btn').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');dim=btn.dataset.dim;
    }
    render();
  });
});
document.querySelector('.dim-btn[data-dim=""]').classList.add('active');

// ── Filtering ────────────────────────────────────────────────────────────────
function getRows(){
  const loD=allDates[dateMin],hiD=allDates[dateMax];
  return ROWS.filter(r=>{
    if(loD&&r.date&&r.date<loD)return false;
    if(hiD&&r.date&&r.date>hiD)return false;
    for(const[f,s]of Object.entries(filt)){if(s.size>0&&!s.has(r[f]))return false;}
    return true;
  });
}

// ── Metrics ──────────────────────────────────────────────────────────────────
function median(arr){
  const s=[...arr].sort((a,b)=>a-b),m=s.length>>1;
  return s.length%2?s[m]:(s[m-1]+s[m])/2;
}
function calcM(rows){
  if(!rows.length)return null;
  const u={};
  rows.forEach(r=>{
    if(!r.uid)return;
    if(!u[r.uid])u[r.uid]={v:0,t:0,p:0,s:0,tk:0,a:0,n:0,ds:[],ex:{}};
    const x=u[r.uid];
    x.v=Math.max(x.v,r.view||0);x.t=Math.max(x.t,r.ttp||0);
    x.p=Math.max(x.p,r.purch||0);x.s=Math.max(x.s,r.unsub||0);
    x.tk=Math.max(x.tk,r.tick||0);x.a+=r.amt||0;x.n+=r.cnt||0;
    if(r.diff!=null)x.ds.push(r.diff);
    if(r.extra)Object.entries(r.extra).forEach(([k,v])=>{x.ex[k]=(x.ex[k]||0)+v;});
  });
  const us=Object.values(u);
  const vU=us.filter(x=>x.v).length, tU=us.filter(x=>x.t).length,
        pU=us.filter(x=>x.p).length, sU=us.filter(x=>x.s&&x.p).length,
        tkU=us.filter(x=>x.tk).length;
  const rev=us.reduce((s,x)=>s+x.a,0), cnt=us.reduce((s,x)=>s+x.n,0);
  const diffs=us.flatMap(x=>x.ds);
  const med=diffs.length?median(diffs)/1000:null;
  const d=(a,b)=>b>0?a/b:null;
  const base={view_u:vU,ttp_u:tU,purch_u:pU,revenue:rev,purch_n:cnt,
         unsub_u:sU,tick_u:tkU,med_ttp:med,
         ttp_r:d(tU,vU),close_r:d(pU,tU),cvr:d(pU,vU),
         ppv:d(cnt,vU),unsub_r:d(sU,pU),tick_r:d(tkU,pU)};
  // aggregate extra columns from custom SQL
  const extraKeys=new Set(us.flatMap(x=>Object.keys(x.ex)));
  extraKeys.forEach(k=>{
    base[k+'_u']=us.filter(x=>(x.ex[k]||0)>0).length;
    base[k+'_sum']=us.reduce((s,x)=>s+(x.ex[k]||0),0);
  });
  CUSTOM_M_DEFS.forEach(cm=>{try{base[cm.k]=Function('m','return ('+cm.expr+')')(base);}catch(e){base[cm.k]=null;}});
  return base;
}

// ── Formatting ───────────────────────────────────────────────────────────────
function fv(v,f){
  if(v==null)return'—';
  if(f==='int')return Math.round(v).toLocaleString('en-US');
  if(f==='money')return'$'+v.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  if(f==='f1')return v.toFixed(1);if(f==='f4')return v.toFixed(4);
  if(f==='pct')return(v*100).toFixed(2)+'%';return String(v);
}
function fd(c,t,f){
  if(c==null||t==null)return['—','—'];
  const d=t-c,sg=d>=0?'+':'';let ds;
  if(f==='pct')ds=sg+(d*100).toFixed(2)+'pp';
  else if(f==='money')ds=(d>=0?'+$':'-$')+Math.abs(d).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  else if(f==='int')ds=sg+Math.round(d).toLocaleString('en-US');
  else if(f==='f1')ds=sg+d.toFixed(1);else if(f==='f4')ds=sg+d.toFixed(4);else ds='—';
  const dp=(c&&c!==0)?(d/c*100>=0?'+':'')+(d/c*100).toFixed(1)+'%':'—';
  return[ds,dp];
}
function dStyle(d){
  if(!d||d==='neutral')return'color:#94A3B8';
  if(d==='good')return'background:rgba(22,163,74,.1);color:#15803D;font-weight:600';
  return'background:rgba(220,38,38,.1);color:#B91C1C;font-weight:600';
}
function dir(c,t,hi){
  if(c==null||t==null)return null;
  if(t>c)return hi?'good':'bad';if(t<c)return hi?'bad':'good';return'neutral';
}

// ── Table builder ─────────────────────────────────────────────────────────────
function dotSpan(color){
  return`<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:${color};flex-shrink:0"></span>`;
}
function valRow(label,color,m,metrics,cls){
  let h=`<tr class="${cls}"><td class="rlh">${dotSpan(color)}${label}</td>`;
  metrics.forEach(mt=>h+=`<td>${m?fv(m[mt.k],mt.f):'—'}</td>`);
  return h+'</tr>';
}
function deltaRows(label,cM,tM,metrics){
  const dirs=metrics.map(mt=>dir(cM?.[mt.k],tM?.[mt.k],mt.hi));
  let abs=`<tr class="row-d"><td class="rlh">${dotSpan('#94A3B8')}Δ ${label}</td>`,
      pct=`<tr class="row-d"><td class="rlh">${dotSpan('#94A3B8')}Δ% ${label}</td>`;
  metrics.forEach((mt,i)=>{
    const[da,dp]=fd(cM?.[mt.k],tM?.[mt.k],mt.f);
    const st=dStyle(dirs[i]);
    abs+=`<td style="${st}">${da}</td>`;pct+=`<td style="${st}">${dp}</td>`;
  });
  return abs+'</tr>'+pct+'</tr>';
}
function sepRow(n){
  return`<tr class="row-sep"><td colspan="${n+1}"></td></tr>`;
}

function buildTable(rows,metrics){
  let h='<table><thead><tr><th class="rlh">Group</th>';
  metrics.forEach(m=>h+=`<th>${m.l}</th>`);
  h+='</tr></thead><tbody>';

  if(!rows.length){return'<div class="no-data">No data for current filters</div>';}

  const n=metrics.length;

  if(mode==='group'){
    const cRows=rows.filter(r=>r.grp==='ctrl');
    const tRows=rows.filter(r=>r.grp==='test');

    if(!dim){
      const cM=calcM(cRows),tM=calcM(tRows);
      h+=valRow(CTRL_L,'#1664F5',cM,metrics,'row-c');
      h+=valRow(TEST_L,'#F77F00',tM,metrics,'row-t');
      h+=deltaRows('',cM,tM,metrics);
    }else{
      const dimVals=[...new Set(rows.map(r=>r[dim]).filter(v=>v&&v!=='nan'))].sort();
      dimVals.forEach((dv,i)=>{
        const cf=cRows.filter(r=>r[dim]===dv), tf=tRows.filter(r=>r[dim]===dv);
        const cM=calcM(cf),tM=calcM(tf);
        if(i>0)h+=sepRow(n);
        h+=valRow(`${CTRL_L} (${dv})`,'#1664F5',cM,metrics,'row-c');
        h+=valRow(`${TEST_L} (${dv})`,'#F77F00',tM,metrics,'row-t');
        h+=deltaRows(dv,cM,tM,metrics);
      });
    }
  }else{
    // by version
    const versions=[...new Set(rows.map(r=>r.split).filter(v=>v))];
    const ctrlSet=new Set(CTRL_V),testSet=new Set(TEST_V);
    const allV=[...versions.filter(v=>ctrlSet.has(v)),...versions.filter(v=>testSet.has(v)),...versions.filter(v=>!ctrlSet.has(v)&&!testSet.has(v))];

    if(!dim){
      allV.forEach((ver,i)=>{
        const vr=rows.filter(r=>r.split===ver);
        const color=ctrlSet.has(ver)?'#1664F5':'#F77F00';
        const cls=ctrlSet.has(ver)?'row-c':'row-t';
        h+=valRow(ver,color,calcM(vr),metrics,cls);
      });
    }else{
      const dimVals=[...new Set(rows.map(r=>r[dim]).filter(v=>v&&v!=='nan'))].sort();
      allV.forEach((ver,i)=>{
        const vr=rows.filter(r=>r.split===ver);
        const color=ctrlSet.has(ver)?'#1664F5':'#F77F00';
        const cls=ctrlSet.has(ver)?'row-c':'row-t';
        if(i>0)h+=sepRow(n);
        dimVals.forEach(dv=>{
          const dr=vr.filter(r=>r[dim]===dv);
          h+=valRow(`${ver} (${dv})`,color,calcM(dr),metrics,cls);
        });
      });
    }
  }

  return h+'</tbody></table>';
}

function render(){
  const rows=getRows();
  document.getElementById('row-count').textContent=rows.length.toLocaleString('en-US')+' rows';
  document.getElementById('t-abs').innerHTML=buildTable(rows,ABS_M);
  document.getElementById('t-rel').innerHTML=buildTable(rows,REL_M);
}

render();

// ── Chat ──────────────────────────────────────────────────────────────────────
let chatOpen=false, pendingMetric=null, pendingSql=null, pendingMetricAfterSql=null;
let pendingFieldExpr=null, pendingMetricAfterField=null;
let currentMode='analysis';
const historyByMode={analysis:[],metrics:[],diagnostics:[]};
function _chatKey(){return 'ab_chat_'+TEST_ID;}
function saveChatHistory(){
  try{localStorage.setItem(_chatKey(),JSON.stringify({history:historyByMode,mode:currentMode}));}catch(e){}
}
const MODE_GREET={
  analysis:'Hi! I can see the current test data. Ask me anything about the results.',
  metrics:'I can help manage metrics on this dashboard \u2014 add new ones or remove existing ones. What would you like to do?',
  diagnostics:'I can help diagnose data issues \u2014 missing events, wrong counts, null values, etc. What symptom are you seeing? I\u2019ll suggest what to check.'
};
function toggleChat(){
  chatOpen=!chatOpen;
  const p=document.getElementById('chat-panel');
  p.classList.toggle('open',chatOpen);
  document.getElementById('chat-fab').textContent=chatOpen?'\u2715 Close':'\ud83d\udcac Ask AI';
}
function switchMode(m){
  currentMode=m;
  document.querySelectorAll('.chat-modes .mode-btn').forEach(b=>b.classList.toggle('active',b.dataset.mode===m));
  const c=document.getElementById('chat-msgs');
  c.innerHTML='<div class="chat-msg ai">'+escHtml(MODE_GREET[m])+'</div>';
  historyByMode[m].forEach(function(msg){
    const div=document.createElement('div');
    div.className='chat-msg '+(msg.role==='user'?'user':'ai');
    if(msg.role==='assistant'){div.innerHTML=md2html(msg.content);}
    else{div.textContent=msg.content;}
    c.appendChild(div);
  });
  c.scrollTop=9999;
  saveChatHistory();
}
function escHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function md2html(t){
  const lines=t.split('\n');
  let out='',inUl=false;
  for(let raw of lines){
    const l=raw.trimEnd();
    const hm=l.match(/^(#{1,3})\s+(.*)/);
    const bullet=l.match(/^[-*\u2022]\s+(.*)/);
    if(hm){
      if(inUl){out+='</ul>';inUl=false;}
      const sz=hm[1].length===1?'14px':hm[1].length===2?'13px':'12.5px';
      out+='<b style="font-size:'+sz+';display:block;margin:5px 0 2px">'+fmtInline(escHtml(hm[2]))+'</b>';
    }else if(/^-{3,}$|^\*{3,}$|^_{3,}$/.test(l)){
      if(inUl){out+='</ul>';inUl=false;}
      out+='<hr style="border:none;border-top:1px solid #CBD5E1;margin:5px 0">';
    }else if(bullet){
      if(!inUl){out+='<ul style="margin:4px 0 4px 16px;padding:0">';inUl=true;}
      out+='<li>'+fmtInline(escHtml(bullet[1]))+'</li>';
    }else{
      if(inUl){out+='</ul>';inUl=false;}
      if(l===''){out+='<br>';}
      else{out+='<span>'+fmtInline(escHtml(l))+'</span><br>';}
    }
  }
  if(inUl)out+='</ul>';
  return out;
}
function fmtInline(s){
  return s.replace(/\*\*(.+?)\*\*/g,'<b>$1</b>').replace(/\*(.+?)\*/g,'<i>$1</i>');
}
function appendMsg(role,text){
  const d=document.createElement('div');
  d.className='chat-msg '+role;
  if(role==='ai')d.innerHTML=md2html(text);
  else d.textContent=text;
  const c=document.getElementById('chat-msgs');
  c.appendChild(d);c.scrollTop=c.scrollHeight;
}
function appendQueryTable(result){
  const wrap=document.getElementById('chat-msgs');
  const d=document.createElement('div');
  d.className='chat-msg ai';
  if(!result.ok){d.textContent='Query error: '+(result.error||'unknown');wrap.appendChild(d);wrap.scrollTop=9999;return;}
  const {columns,rows}=result;
  let h='<div class="qr-wrap"><table class="qr-table"><thead><tr>';
  h+=columns.map(c=>'<th>'+escHtml(String(c))+'</th>').join('');
  h+='</tr></thead><tbody>';
  rows.forEach(r=>{h+='<tr>'+r.map(v=>'<td>'+(v!=null?escHtml(String(v)):'&#8212;')+'</td>').join('')+'</tr>';});
  h+='</tbody></table></div>';
  if(rows.length===500)h+='<span style="font-size:10px;color:#94A3B8">(limited to 500 rows)</span>';
  d.innerHTML=h;wrap.appendChild(d);wrap.scrollTop=9999;
}
function removeThinking(){const t=document.querySelector('.chat-msg.thinking');if(t)t.remove();}
async function callChatAPI(message,metricsOverride){
  const vr=getRows();
  const cM=calcM(vr.filter(r=>r.grp==='ctrl'));
  const tM=calcM(vr.filter(r=>r.grp==='test'));
  const res=await fetch('/api/tests/'+TEST_ID+'/chat',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({
      message:message,
      history:historyByMode[currentMode].slice(-20),
      metrics_summary:metricsOverride||{ctrl:cM,test:tM},
      mode:currentMode,
      custom_metrics:CUSTOM_M_DEFS
    })
  });
  return res.json();
}
async function handleActions(actions){
  const sqlAct=actions.find(a=>a.type==='update_sql');
  const fieldAct=actions.find(a=>a.type==='add_sql_field');
  const metricAct=actions.find(a=>a.type==='add_metric');
  const queryAct=actions.find(a=>a.type==='run_query');
  if(fieldAct){
    pendingFieldExpr=fieldAct.field_expr;
    pendingMetricAfterField=metricAct?metricAct.metric_def:null;
    openFieldModal(fieldAct.field_expr);
  }else if(sqlAct){
    pendingSql=sqlAct.sql;
    pendingMetricAfterSql=metricAct?metricAct.metric_def:null;
    openSqlModal(sqlAct.sql);
  }else if(metricAct){
    pendingMetric=metricAct.metric_def;
    openMetricModal(metricAct.metric_def);
  }
  const removeActs=actions.filter(a=>a.type==='remove_metric');
  for(const ra of removeActs) await handleRemoveMetric(ra.name,ra.display||ra.name);
  if(queryAct) await runDiagnosticQuery(queryAct.sql);
}
async function runDiagnosticQuery(sql){
  const wrap=document.getElementById('chat-msgs');
  const thk=document.createElement('div');
  thk.className='chat-msg thinking';thk.textContent='Running query\u2026';
  wrap.appendChild(thk);wrap.scrollTop=9999;
  let result;
  try{
    const res=await fetch('/api/tests/'+TEST_ID+'/run-diagnostic',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({sql:sql})
    });
    result=await res.json();
  }catch(e){result={ok:false,error:e.message};}
  thk.remove();
  appendQueryTable(result);
  const summary=result.ok
    ?('Query returned '+result.rows.length+' row(s). Columns: '+result.columns.join(', ')+'.\n'
      +result.rows.slice(0,8).map(r=>r.join(' | ')).join('\n'))
    :('Query error: '+result.error);
  const thk2=document.createElement('div');
  thk2.className='chat-msg thinking';thk2.textContent='Analyzing\u2026';
  wrap.appendChild(thk2);wrap.scrollTop=9999;
  try{
    const data=await callChatAPI('[Query result]\n'+summary,{});
    thk2.remove();
    historyByMode[currentMode].push({role:'user',content:'[Query result]\n'+summary});
    if(data.reply)appendMsg('ai',data.reply);
    historyByMode[currentMode].push({role:'assistant',content:data.reply||''});
    saveChatHistory();
    await handleActions(data.actions||(data.action?[data.action]:[]));
  }catch(e){thk2.remove();appendMsg('ai','Error: '+e.message);}
}
async function sendChat(){
  const inp=document.getElementById('chat-input');
  const msg=inp.value.trim();if(!msg)return;
  inp.value='';
  appendMsg('user',msg);
  historyByMode[currentMode].push({role:'user',content:msg});
  saveChatHistory();
  const btn=document.getElementById('chat-send');
  btn.disabled=true;
  const thk=document.createElement('div');
  thk.className='chat-msg thinking';thk.textContent='Thinking\u2026';
  document.getElementById('chat-msgs').appendChild(thk);
  document.getElementById('chat-msgs').scrollTop=9999;
  try{
    const data=await callChatAPI(msg,null);
    removeThinking();
    if(data.reply)appendMsg('ai',data.reply);
    historyByMode[currentMode].push({role:'assistant',content:data.reply||''});
    saveChatHistory();
    await handleActions(data.actions||(data.action?[data.action]:[]));
  }catch(e){removeThinking();appendMsg('ai','Error: '+e.message);}
  finally{btn.disabled=false;}
}
// ── Add metric modal ──────────────────────────────────────────────────────────
function openMetricModal(m){
  document.getElementById('mm-name').textContent=m.display+' ('+m.name+')';
  document.getElementById('mm-expr').textContent=m.expr;
  document.getElementById('mm-overlay').classList.add('open');
}
async function confirmAddMetric(){
  if(!pendingMetric)return;
  const asDefault=document.getElementById('mm-default').checked;
  document.getElementById('mm-overlay').classList.remove('open');
  try{
    const r=await fetch('/api/tests/'+TEST_ID+'/add-metric',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({metric:pendingMetric,as_default:asDefault})
    });
    const d=await r.json();
    if(d.ok){
      const succMsg='\u2705 Metric "'+pendingMetric.display+'" added!';
      historyByMode[currentMode].push({role:'assistant',content:succMsg});
      saveChatHistory();
      appendMsg('ai',succMsg+' Reloading\u2026');
      setTimeout(()=>window.location.reload(),1500);
    }else{appendMsg('ai','Failed: '+(d.error||'unknown'));}
  }catch(e){appendMsg('ai','Error: '+e.message);}
  pendingMetric=null;
}
function cancelAddMetric(){
  document.getElementById('mm-overlay').classList.remove('open');
  pendingMetric=null;
}
// ── Add SQL field modal ───────────────────────────────────────────────────────
function openFieldModal(expr){
  document.getElementById('field-preview').textContent=expr;
  document.getElementById('field-overlay').classList.add('open');
}
async function confirmAddField(){
  if(!pendingFieldExpr)return;
  const expr=pendingFieldExpr,afterMetric=pendingMetricAfterField;
  pendingFieldExpr=null;pendingMetricAfterField=null;
  document.getElementById('field-overlay').classList.remove('open');
  try{
    const r=await fetch('/api/tests/'+TEST_ID+'/inject-sql-field',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({field_expr:expr})
    });
    const d=await r.json();
    if(d.ok){
      appendMsg('ai','✅ SQL field added. Click **Refresh Now** on the test page to pull fresh data with this field.');
      if(afterMetric){pendingMetric=afterMetric;openMetricModal(afterMetric);}
    }else{appendMsg('ai','Failed: '+(d.error||'unknown'));}
  }catch(e){appendMsg('ai','Error: '+e.message);}
}
function cancelAddField(){
  document.getElementById('field-overlay').classList.remove('open');
  pendingFieldExpr=null;pendingMetricAfterField=null;
}
// ── Update SQL modal ──────────────────────────────────────────────────────────
function openSqlModal(sql){
  document.getElementById('sql-preview').textContent=sql;
  document.getElementById('sql-overlay').classList.add('open');
}
async function confirmUpdateSql(){
  if(!pendingSql)return;
  const sql=pendingSql,afterMetric=pendingMetricAfterSql;
  pendingSql=null;pendingMetricAfterSql=null;
  document.getElementById('sql-overlay').classList.remove('open');
  try{
    const r=await fetch('/api/tests/'+TEST_ID+'/update-sql',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({sql:sql})
    });
    const d=await r.json();
    if(d.ok){
      appendMsg('ai','\u2705 SQL updated. Click **Refresh** on the test page to reload data with the new query.');
      if(afterMetric){pendingMetric=afterMetric;openMetricModal(afterMetric);}
    }else{appendMsg('ai','Failed to save SQL: '+(d.error||'unknown'));}
  }catch(e){appendMsg('ai','Error: '+e.message);}
}
function cancelUpdateSql(){
  document.getElementById('sql-overlay').classList.remove('open');
  pendingSql=null;pendingMetricAfterSql=null;
}
// ── Remove metric ─────────────────────────────────────────────────────────────
async function handleRemoveMetric(name,display){
  if(!confirm('Remove metric "'+display+'" from all dashboards?'))return;
  try{
    const r=await fetch('/api/tests/'+TEST_ID+'/remove-metric',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({name:name,display:display})
    });
    const d=await r.json();
    if(d.ok){
      const rmMsg='\u2705 Metric "'+display+'" removed.';
      historyByMode[currentMode].push({role:'assistant',content:rmMsg});
      saveChatHistory();
      appendMsg('ai',rmMsg+' Updating dashboard\u2026');
      // Trigger server-side re-render so the column disappears immediately on reload
      await fetch('/api/tests/'+TEST_ID+'/rerender-dashboard',{method:'POST'});
      setTimeout(()=>window.location.reload(),800);
    }else{appendMsg('ai','Failed: '+(d.error||'unknown'));}
  }catch(e){appendMsg('ai','Error: '+e.message);}
}
</script>
<button class="chat-fab" id="chat-fab" onclick="toggleChat()">&#128172; Ask AI</button>
<div class="chat-panel" id="chat-panel">
  <div class="chat-phdr">
    <span>AI Assistant</span>
    <button class="chat-close" onclick="toggleChat()">&#10005;</button>
  </div>
  <div class="chat-modes">
    <button class="mode-btn active" data-mode="analysis" onclick="switchMode('analysis')">&#128202; Analysis</button>
    <button class="mode-btn" data-mode="metrics" onclick="switchMode('metrics')">&#128207; Metrics</button>
    <button class="mode-btn" data-mode="diagnostics" onclick="switchMode('diagnostics')">&#128269; Diagnostics</button>
  </div>
  <div class="chat-msgs" id="chat-msgs">
    <div class="chat-msg ai">Hi! I can see the current test data. Ask me anything about the results.</div>
  </div>
  <div class="chat-irow">
    <textarea id="chat-input" rows="2" placeholder="Ask about the results&#8230; (Enter to send)"
      onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendChat();}"></textarea>
    <button id="chat-send" onclick="sendChat()">&#8594;</button>
  </div>
</div>
<div class="mm-overlay" id="mm-overlay">
  <div class="mm-box">
    <h3>Add Custom Metric</h3>
    <div class="mm-row"><strong id="mm-name"></strong></div>
    <div class="mm-row mm-code" id="mm-expr"></div>
    <label class="mm-cb mm-row">
      <input type="checkbox" id="mm-default"> Add as default for all new tests
    </label>
    <div class="mm-btns">
      <button class="mm-ok" onclick="confirmAddMetric()">Add metric</button>
      <button class="mm-cancel" onclick="cancelAddMetric()">Cancel</button>
    </div>
  </div>
</div>
<div class="mm-overlay" id="field-overlay">
  <div class="mm-box" style="max-width:520px">
    <h3>Add SQL Field</h3>
    <p style="font-size:12px;color:#64748B;margin-bottom:8px">The AI wants to add this field to the SELECT clause. After confirming, click <strong>Refresh Now</strong> on the test page to reload data.</p>
    <div class="mm-row mm-code" id="field-preview" style="max-height:200px;overflow-y:auto;font-size:11px;white-space:pre-wrap;word-break:break-all"></div>
    <div class="mm-btns">
      <button class="mm-ok" onclick="confirmAddField()">Add Field</button>
      <button class="mm-cancel" onclick="cancelAddField()">Cancel</button>
    </div>
  </div>
</div>
<div class="mm-overlay" id="sql-overlay">
  <div class="mm-box" style="max-width:680px">
    <h3>Update SQL Query</h3>
    <p style="font-size:12px;color:#64748B;margin-bottom:8px">The AI wants to replace the SQL query to add new data columns. After saving, click <strong>Refresh</strong> on the test page to reload data.</p>
    <div class="mm-row mm-code" id="sql-preview" style="max-height:320px;overflow-y:auto;font-size:11px;white-space:pre-wrap;word-break:break-all"></div>
    <div class="mm-btns">
      <button class="mm-ok" onclick="confirmUpdateSql()">Apply SQL</button>
      <button class="mm-cancel" onclick="cancelUpdateSql()">Cancel</button>
    </div>
  </div>
</div>
<script>
(function(){
  try{
    var s=localStorage.getItem('ab_chat_'+TEST_ID);
    if(s){
      var d=JSON.parse(s);
      var savedHistory=d.history||d;
      var savedMode=d.mode||'analysis';
      Object.assign(historyByMode,savedHistory);
      switchMode(savedMode);
      var hasHistory=Object.values(savedHistory).some(function(arr){return Array.isArray(arr)&&arr.length>0;});
      if(hasHistory){toggleChat();}
    }else{
      switchMode('analysis');
    }
  }catch(e){switchMode('analysis');}
})();
</script>
</body>
</html>"""


# ── Python helpers ────────────────────────────────────────────────────────────

def _filt(df: pd.DataFrame, col: str, val: str) -> pd.DataFrame:
    if col not in df.columns:
        return df.iloc[0:0]
    return df[df[col].astype(str) == val]


def compute_slices(
    ctrl_df: pd.DataFrame,
    test_df: pd.DataFrame,
    calc_fn,
) -> Tuple[Dict, Dict[str, List[str]]]:
    from ab_agent.stats.engine import serialize_metrics

    slices: Dict[str, Dict] = {
        "": {
            "ctrl": serialize_metrics(calc_fn(ctrl_df)),
            "test": serialize_metrics(calc_fn(test_df)),
        }
    }
    dim_values: Dict[str, List[str]] = {}
    for col, _ in BREAKDOWN_DIMS:
        vals: set = set()
        for df in (ctrl_df, test_df):
            if col in df.columns:
                vals |= set(df[col].dropna().astype(str).unique())
        vals -= {"", "None", "nan"}
        if vals:
            dim_values[col] = sorted(vals, key=lambda x: (not x.lstrip("-").isdigit(), x))

    for col, vals in dim_values.items():
        for v in vals:
            key = f"{col}={v}"
            slices[key] = {
                "ctrl": serialize_metrics(calc_fn(_filt(ctrl_df, col, v))),
                "test": serialize_metrics(calc_fn(_filt(test_df, col, v))),
            }

    cols = list(dim_values.keys())
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            c1, c2 = cols[i], cols[j]
            for v1 in dim_values[c1]:
                for v2 in dim_values[c2]:
                    key = "|".join(sorted([f"{c1}={v1}", f"{c2}={v2}"]))
                    cf = _filt(_filt(ctrl_df, c1, v1), c2, v2)
                    tf = _filt(_filt(test_df, c1, v1), c2, v2)
                    slices[key] = {
                        "ctrl": serialize_metrics(calc_fn(cf)),
                        "test": serialize_metrics(calc_fn(tf)),
                    }

    return slices, dim_values


def render_html_dashboard_string(
    rows: List[Dict],
    config: ABTestConfig,
    ctrl_versions_clean: List[str],
    test_versions_clean: List[str],
    test_id: str = "",
    custom_metrics: List[Dict] = None,
) -> str:
    ctrl_short = _shorten(", ".join(ctrl_versions_clean), 50)
    test_short = _shorten(", ".join(test_versions_clean), 50)

    # Build custom metric JS objects: {k, l, f, hi, type, expr}
    cm_js = []
    for cm in (custom_metrics or []):
        cm_js.append({
            "k": cm.get("name", ""),
            "l": cm.get("display_name", cm.get("name", "")),
            "f": cm.get("format", "f4"),
            "hi": bool(cm.get("higher_is_better", True)),
            "type": cm.get("metric_type", "rel"),
            "expr": cm.get("js_expr", "null"),
        })

    rows_json = json.dumps(rows, ensure_ascii=False).replace("</", "<\\/")

    return (
        _HTML
        .replace("TMPL_TEST_NAME",     config.test_name)
        .replace("TMPL_RELEASE",       config.release_date.strftime("%Y-%m-%d %H:%M"))
        .replace("TMPL_CTRL_VERSIONS", json.dumps(ctrl_versions_clean, ensure_ascii=False))
        .replace("TMPL_TEST_VERSIONS", json.dumps(test_versions_clean, ensure_ascii=False))
        .replace("TMPL_TEST_ID",       test_id)
        .replace("TMPL_CTRL",          ctrl_short)
        .replace("TMPL_TEST",          test_short)
        .replace("TMPL_ROWS",          rows_json)
        .replace("TMPL_CUSTOM_METRICS", json.dumps(cm_js, ensure_ascii=False))
    )


def render_html_dashboard(
    slices: Dict,
    dim_values: Dict[str, List[str]],
    config: ABTestConfig,
    path,
) -> "Path":
    """Legacy file-write version — kept for compatibility."""
    from ab_agent.bigquery.query_builder import _strip_channel
    ctrl_clean = [_strip_channel(v) for v in config.control.versions]
    test_clean = [_strip_channel(v) for v in config.test.versions]
    html = render_html_dashboard_string([], config, ctrl_clean, test_clean)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(html, encoding="utf-8")
    return p
