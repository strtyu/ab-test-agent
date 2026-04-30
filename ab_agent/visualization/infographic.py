from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from ab_agent.core.models import ABTestConfig
from ab_agent.stats.engine import ABS_METRICS, BREAKDOWN_DIMS, REL_METRICS
from ab_agent.visualization.chart_library import (
    _shorten, calc_delta, fmt_value, metric_direction,
)

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TMPL_TEST_NAME</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', sans-serif;
       background: #F1F5F9; color: #1E293B; font-size: 13px; }
.hdr { background: #0F1B35; padding: 16px 28px; display: flex; align-items: center;
       justify-content: space-between; gap: 16px; }
.hdr h1 { color: #fff; font-size: 18px; font-weight: 700; }
.hdr p  { color: #94A3B8; font-size: 11px; margin-top: 4px; }
.legend { display: flex; gap: 20px; flex-shrink: 0; }
.leg    { display: flex; align-items: center; gap: 7px; color: #CBD5E1; font-size: 12px; white-space: nowrap; }
.dot    { width: 8px; height: 8px; border-radius: 50%; }
.fbar { background: #fff; border-bottom: 1px solid #E2E8F0; padding: 12px 28px;
        display: flex; align-items: flex-start; gap: 28px; flex-wrap: wrap; }
.fg   { display: flex; flex-direction: column; gap: 5px; }
.fg label { font-size: 10px; font-weight: 700; color: #94A3B8; text-transform: uppercase; letter-spacing: .06em; }
.fg select { padding: 6px 10px; border: 1px solid #CBD5E1; border-radius: 6px; font-size: 12px;
             color: #1E293B; background: #F8FAFC; cursor: pointer; min-width: 150px;
             appearance: none; background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%2394A3B8' d='M6 8L1 3h10z'/%3E%3C/svg%3E");
             background-repeat: no-repeat; background-position: right 8px center; padding-right: 26px; }
.fg select:focus { outline: 2px solid #1664F5; outline-offset: 1px; border-color: #1664F5; }
.pills { display: flex; gap: 5px; flex-wrap: wrap; margin-top: 1px; min-height: 28px; align-items: center; }
.pill  { padding: 3px 11px; border-radius: 20px; border: 1px solid #CBD5E1; font-size: 12px;
         font-weight: 500; cursor: pointer; background: #F8FAFC; color: #64748B;
         transition: all .12s; white-space: nowrap; line-height: 1.5; }
.pill:hover  { border-color: #1664F5; color: #1664F5; background: #EBF3FF; }
.pill.on     { border-color: #1664F5; background: #1664F5; color: #fff; }
.content { padding: 20px 28px; display: flex; flex-direction: column; gap: 16px; max-width: 1600px; }
.card        { background: #fff; border-radius: 8px; border: 1px solid #E2E8F0;
               box-shadow: 0 1px 3px rgba(0,0,0,.04); overflow: hidden; }
.card-title  { padding: 12px 18px; font-size: 12px; font-weight: 700; color: #334155;
               border-bottom: 1px solid #F1F5F9; display: flex; align-items: center; gap: 8px; }
.card-title::before { content: ''; width: 3px; height: 13px; background: #1664F5;
                      border-radius: 2px; flex-shrink: 0; }
.tbl-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; min-width: 560px; }
thead th { background: #0F1B35; color: #fff; padding: 9px 13px; text-align: center;
           font-size: 11px; font-weight: 600; letter-spacing: .04em; white-space: nowrap; }
thead th.rlh { text-align: left; min-width: 155px; }
tbody td { padding: 8px 13px; text-align: center; border-bottom: 1px solid #F1F5F9; white-space: nowrap; }
tbody td.rlh { text-align: left; min-width: 155px; display: flex; align-items: center; gap: 7px;
               font-size: 12px; font-weight: 600; }
tbody tr:last-child td { border-bottom: none; }
.row-c td { background: #F0F6FF; }
.row-t td { background: #FFF8F0; }
.row-d td { background: #FAFAFA; }
.no-data { padding: 40px; text-align: center; color: #94A3B8; }
.footer { text-align: center; padding: 14px; color: #94A3B8; font-size: 11px;
          border-top: 1px solid #E2E8F0; background: #fff; }
.bg { background: #DCFCE7; color: #15803D; padding: 2px 7px; border-radius: 4px; font-size: 11px; font-weight: 600; }
.bb { background: #FEE2E2; color: #B91C1C; padding: 2px 7px; border-radius: 4px; font-size: 11px; font-weight: 600; }
.bn { background: #F1F5F9; color: #64748B; padding: 2px 7px; border-radius: 4px; font-size: 11px; }
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
<div class="fbar">
  <div class="fg">
    <label>Breakdown</label>
    <select id="d1" onchange="onD1()"><option value="">— All data —</option></select>
    <div id="p1" class="pills"></div>
  </div>
  <div class="fg" id="d2wrap" style="display:none">
    <label>Sub-breakdown</label>
    <select id="d2" onchange="onD2()"><option value="">— None —</option></select>
    <div id="p2" class="pills"></div>
  </div>
</div>
<div class="content">
  <div class="card"><div class="card-title">Absolute Values</div><div class="tbl-wrap" id="tAbs"></div></div>
  <div class="card"><div class="card-title">Relative Values</div><div class="tbl-wrap" id="tRel"></div></div>
</div>
<div class="footer">
  <span class="bg">▲ test better</span> &nbsp;
  <span class="bb">▼ test worse</span> &nbsp;
  <span class="bn">— no data</span>
</div>
<script>
const DATA=TMPL_DATA,DV=TMPL_DIM_VAL,DL=TMPL_DIM_LBL,CTRL_L="TMPL_CTRL",TEST_L="TMPL_TEST",ABS=TMPL_ABS,REL=TMPL_REL;
let s={d1:'',v1:'',d2:'',v2:''};
(function init(){const s1=document.getElementById('d1'),s2=document.getElementById('d2');Object.keys(DL).forEach(k=>{s1.add(new Option(DL[k],k));s2.add(new Option(DL[k],k));});render();})();
function onD1(){s.d1=document.getElementById('d1').value;s.v1='';s.d2='';s.v2='';document.getElementById('d2').value='';pills('p1',s.d1,1);pills('p2','',2);document.getElementById('d2wrap').style.display=s.d1?'flex':'none';render();}
function onD2(){s.d2=document.getElementById('d2').value;s.v2='';pills('p2',s.d2,2);render();}
function pills(id,dim,n){const c=document.getElementById(id);c.innerHTML='';if(!dim)return;(DV[dim]||[]).forEach(v=>{const b=document.createElement('button');b.className='pill'+((n===1?s.v1:s.v2)===v?' on':'');b.textContent=v;b.onclick=()=>{if(n===1)s.v1=s.v1===v?'':v;else s.v2=s.v2===v?'':v;c.querySelectorAll('.pill').forEach(x=>x.classList.remove('on'));if((n===1?s.v1:s.v2)===v)b.classList.add('on');render();};c.appendChild(b);});}
function key(){const p=[];if(s.d1&&s.v1)p.push(s.d1+'='+s.v1);if(s.d2&&s.v2)p.push(s.d2+'='+s.v2);return p.sort().join('|');}
function fv(v,f){if(v==null)return'—';if(f==='int')return Math.round(v).toLocaleString('en-US');if(f==='money')return'$'+v.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});if(f==='f1')return v.toFixed(1);if(f==='f4')return v.toFixed(4);if(f==='pct')return(v*100).toFixed(2)+'%';return String(v);}
function fd(c,t,f){if(c==null||t==null)return['—','—'];const d=t-c,sg=d>=0?'+':'';let ds;if(f==='pct')ds=sg+(d*100).toFixed(2)+'pp';else if(f==='money')ds=(d>=0?'+$':'-$')+Math.abs(d).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});else if(f==='int')ds=sg+Math.round(d).toLocaleString('en-US');else if(f==='f1')ds=sg+d.toFixed(1);else if(f==='f4')ds=sg+d.toFixed(4);else ds='—';const dp=(c&&c!==0)?(d/c*100>=0?'+':'')+(d/c*100).toFixed(1)+'%':'—';return[ds,dp];}
function dir(c,t,h){if(c==null||t==null)return null;if(t>c)return h?'good':'bad';if(t<c)return h?'bad':'good';return'neutral';}
function dStyle(d){if(d==='good')return'background:rgba(22,163,74,.1);color:#15803D;font-weight:600';if(d==='bad')return'background:rgba(220,38,38,.1);color:#B91C1C;font-weight:600';return'color:#94A3B8';}
function buildTable(metrics,ctrl,test){const dirs=metrics.map(m=>dir(ctrl[m.key],test[m.key],m.higher));let h='<table><thead><tr><th class="rlh">Metric</th>';metrics.forEach(m=>{h+=`<th>${m.label}</th>`;});h+='</tr></thead><tbody>';const rowDef=[{lbl:CTRL_L,dot:'#1664F5',cls:'row-c',vals:metrics.map(m=>fv(ctrl[m.key],m.fmt)),styles:metrics.map(()=>'color:#1E293B')},{lbl:TEST_L,dot:'#F77F00',cls:'row-t',vals:metrics.map(m=>fv(test[m.key],m.fmt)),styles:metrics.map(()=>'color:#1E293B')},{lbl:'Δ absolute',dot:'#94A3B8',cls:'row-d',vals:metrics.map(m=>fd(ctrl[m.key],test[m.key],m.fmt)[0]),styles:dirs.map(d=>dStyle(d))},{lbl:'Δ %',dot:'#94A3B8',cls:'row-d',vals:metrics.map(m=>fd(ctrl[m.key],test[m.key],m.fmt)[1]),styles:dirs.map(d=>dStyle(d))}];rowDef.forEach(row=>{h+=`<tr class="${row.cls}"><td class="rlh"><span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:${row.dot};flex-shrink:0"></span>${row.lbl}</td>`;row.vals.forEach((v,i)=>{h+=`<td style="${row.styles[i]}">${v}</td>`;});h+='</tr>';});h+='</tbody></table>';return h;}
function render(){const k=key(),d=DATA[k];if(!d){const msg='<div class="no-data">No data for this filter combination</div>';document.getElementById('tAbs').innerHTML=msg;document.getElementById('tRel').innerHTML=msg;return;}document.getElementById('tAbs').innerHTML=buildTable(ABS,d.ctrl,d.test);document.getElementById('tRel').innerHTML=buildTable(REL,d.ctrl,d.test);}
</script>
</body>
</html>"""


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


def render_html_dashboard(
    slices: Dict,
    dim_values: Dict[str, List[str]],
    config: ABTestConfig,
    path: Path,
) -> Path:
    ctrl_short = _shorten(" + ".join(config.control.versions), 45)
    test_short = _shorten(" + ".join(config.test.versions), 45)
    dim_lbl_map = dict(BREAKDOWN_DIMS)
    dim_labels  = {k: dim_lbl_map[k] for k in dim_values if k in dim_lbl_map}

    abs_js = json.dumps([{"key": k, "label": l, "fmt": f, "higher": h} for k, l, f, h in ABS_METRICS])
    rel_js = json.dumps([{"key": k, "label": l, "fmt": f, "higher": h} for k, l, f, h in REL_METRICS])

    html = (
        _HTML
        .replace("TMPL_TEST_NAME", config.test_name)
        .replace("TMPL_RELEASE",   config.release_date.strftime("%Y-%m-%d %H:%M"))
        .replace("TMPL_CTRL",      ctrl_short)
        .replace("TMPL_TEST",      test_short)
        .replace("TMPL_DATA",      json.dumps(slices, ensure_ascii=False))
        .replace("TMPL_DIM_VAL",   json.dumps(dim_values, ensure_ascii=False))
        .replace("TMPL_DIM_LBL",   json.dumps(dim_labels, ensure_ascii=False))
        .replace("TMPL_ABS",       abs_js)
        .replace("TMPL_REL",       rel_js)
    )
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path


def render_html_dashboard_string(
    slices: Dict,
    dim_values: Dict[str, List[str]],
    config: ABTestConfig,
) -> str:
    """Same as render_html_dashboard but returns HTML string instead of writing to file."""
    ctrl_short = _shorten(" + ".join(config.control.versions), 45)
    test_short = _shorten(" + ".join(config.test.versions), 45)
    dim_lbl_map = dict(BREAKDOWN_DIMS)
    dim_labels  = {k: dim_lbl_map[k] for k in dim_values if k in dim_lbl_map}

    abs_js = json.dumps([{"key": k, "label": l, "fmt": f, "higher": h} for k, l, f, h in ABS_METRICS])
    rel_js = json.dumps([{"key": k, "label": l, "fmt": f, "higher": h} for k, l, f, h in REL_METRICS])

    return (
        _HTML
        .replace("TMPL_TEST_NAME", config.test_name)
        .replace("TMPL_RELEASE",   config.release_date.strftime("%Y-%m-%d %H:%M"))
        .replace("TMPL_CTRL",      ctrl_short)
        .replace("TMPL_TEST",      test_short)
        .replace("TMPL_DATA",      json.dumps(slices, ensure_ascii=False))
        .replace("TMPL_DIM_VAL",   json.dumps(dim_values, ensure_ascii=False))
        .replace("TMPL_DIM_LBL",   json.dumps(dim_labels, ensure_ascii=False))
        .replace("TMPL_ABS",       abs_js)
        .replace("TMPL_REL",       rel_js)
    )
