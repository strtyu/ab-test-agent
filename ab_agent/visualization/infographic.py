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

def build_rows_for_dashboard(ctrl_df: pd.DataFrame, test_df: pd.DataFrame) -> List[Dict]:
    rows = []
    for grp, df in [("ctrl", ctrl_df), ("test", test_df)]:
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

            rows.append({
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
            })
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
</style>
</head>
<body>
<div class="hdr">
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
document.querySelectorAll('.mode-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.mode-btn').forEach(b=>b.classList.remove('active'));
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
    if(!u[r.uid])u[r.uid]={v:0,t:0,p:0,s:0,tk:0,a:0,n:0,ds:[]};
    const x=u[r.uid];
    x.v=Math.max(x.v,r.view||0);x.t=Math.max(x.t,r.ttp||0);
    x.p=Math.max(x.p,r.purch||0);x.s=Math.max(x.s,r.unsub||0);
    x.tk=Math.max(x.tk,r.tick||0);x.a+=r.amt||0;x.n+=r.cnt||0;
    if(r.diff!=null)x.ds.push(r.diff);
  });
  const us=Object.values(u);
  const vU=us.filter(x=>x.v).length, tU=us.filter(x=>x.t).length,
        pU=us.filter(x=>x.p).length, sU=us.filter(x=>x.s&&x.p).length,
        tkU=us.filter(x=>x.tk).length;
  const rev=us.reduce((s,x)=>s+x.a,0), cnt=us.reduce((s,x)=>s+x.n,0);
  const diffs=us.flatMap(x=>x.ds);
  const med=diffs.length?median(diffs)/1000:null;
  const d=(a,b)=>b>0?a/b:null;
  return{view_u:vU,ttp_u:tU,purch_u:pU,revenue:rev,purch_n:cnt,
         unsub_u:sU,tick_u:tkU,med_ttp:med,
         ttp_r:d(tU,vU),close_r:d(pU,tU),cvr:d(pU,vU),
         ppv:d(cnt,vU),unsub_r:d(sU,pU),tick_r:d(tkU,pU)};
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
) -> str:
    ctrl_short = _shorten(", ".join(ctrl_versions_clean), 50)
    test_short = _shorten(", ".join(test_versions_clean), 50)

    return (
        _HTML
        .replace("TMPL_TEST_NAME", config.test_name)
        .replace("TMPL_RELEASE",   config.release_date.strftime("%Y-%m-%d %H:%M"))
        .replace("TMPL_CTRL",      ctrl_short)
        .replace("TMPL_TEST",      test_short)
        .replace("TMPL_ROWS",      json.dumps(rows, ensure_ascii=False))
        .replace("TMPL_CTRL_VERSIONS", json.dumps(ctrl_versions_clean, ensure_ascii=False))
        .replace("TMPL_TEST_VERSIONS", json.dumps(test_versions_clean, ensure_ascii=False))
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
