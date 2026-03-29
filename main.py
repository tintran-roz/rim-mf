#!/usr/bin/env python3
"""
================================================================
RCA BEEPER MIDDLEWARE v3.8 — SQLite Dispatch History
================================================================
Zero dependencies. Python 3.6+ stdlib only.
New: SQLite DB for dispatch history (survives PDA refresh/restart)
================================================================
"""
import os,sys,json,uuid,time,logging,threading,socket,traceback,sqlite3
from datetime import datetime
from http.server import HTTPServer,BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.request import Request,urlopen
from urllib.error import URLError,HTTPError
from urllib.parse import urlparse

socket.setdefaulttimeout(15)
logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(name)-10s] %(levelname)-5s %(message)s",datefmt="%H:%M:%S")
log=logging.getLogger("RCA")

class ThreadedHTTPServer(ThreadingMixIn,HTTPServer):
    daemon_threads=True;allow_reuse_address=True;request_queue_size=50
    def handle_error(s,req,addr): log.debug("Conn err %s",addr)

CFG_FILE=os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.json")
DB_FILE=os.path.join(os.path.dirname(os.path.abspath(__file__)),"rca_beeper.db")

def load_config():
    try:
        if os.path.exists(CFG_FILE):
            with open(CFG_FILE,"r",encoding="utf-8") as f: return json.load(f)
    except Exception as e: log.error("Config err: %s",e)
    return {"rcs":{"host":"127.0.0.1","port":8182},"server":{"port":8990},"timing":{},"stations":[],"kitting_layout":[]}
CFG=load_config()

def safe_http_post(url,payload,timeout=10):
    try:
        body=json.dumps(payload).encode("utf-8")
        req=Request(url,data=body,headers={"Content-Type":"application/json"})
        with urlopen(req,timeout=timeout) as resp: return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e: return {"code":str(e.code),"message":str(e.reason)}
    except URLError as e: return {"code":"-2","message":"Connect failed"}
    except socket.timeout: return {"code":"-1","message":"Timeout"}
    except Exception as e: return {"code":"-3","message":str(e)[:60]}

# ==================== DATABASE ====================
class DB:
    def __init__(s,path):
        s.path=path; s.lock=threading.Lock(); s._init()
    def _conn(s):
        c=sqlite3.connect(s.path,timeout=5); c.row_factory=sqlite3.Row; return c
    def _init(s):
        with s._conn() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS dispatch_history(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT NOT NULL,
                kitting TEXT, source TEXT, rack TEXT,
                station TEXT, buffer TEXT, task_type TEXT,
                task_id TEXT, state TEXT DEFAULT 'created',
                message TEXT, error TEXT
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS station_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT NOT NULL,
                station_id TEXT, event TEXT, ok INTEGER,
                detail TEXT
            )''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_dh_time ON dispatch_history(time)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_dh_station ON dispatch_history(station)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_se_time ON station_events(time)')
        log.info("DB: %s",s.path)

    def add_dispatch(s,kitting,source,rack,station,buffer,task_type,task_id,state,message=""):
        with s.lock:
            with s._conn() as c:
                c.execute('INSERT INTO dispatch_history(time,kitting,source,rack,station,buffer,task_type,task_id,state,message) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),kitting,source,rack,station,buffer,task_type,task_id or "",state,message))
                return c.execute('SELECT last_insert_rowid()').fetchone()[0]

    def update_dispatch_state(s,task_id,state,message=""):
        if not task_id: return
        with s.lock:
            with s._conn() as c:
                c.execute('UPDATE dispatch_history SET state=?,message=? WHERE task_id=? AND state NOT IN ("complete","cancelled")',
                    (state,message,task_id))

    def cancel_dispatch_by_id(s,dispatch_id):
        """Cancel a dispatch by its DB row id"""
        with s.lock:
            with s._conn() as c:
                row=c.execute('SELECT * FROM dispatch_history WHERE id=?',(dispatch_id,)).fetchone()
                if not row: return None
                d=dict(row)
                c.execute('UPDATE dispatch_history SET state="cancelled",message="Cancelled from PDA" WHERE id=? AND state NOT IN ("complete","cancelled")',(dispatch_id,))
                return d

    def get_active_task_ids(s):
        """Get task_ids of all active dispatches for RCS polling"""
        with s._conn() as c:
            rows=c.execute("SELECT task_id,station,source FROM dispatch_history WHERE state IN ('created','executing') AND task_id!='' ORDER BY id DESC").fetchall()
            return [dict(r) for r in rows]

    def get_history(s,limit=50):
        with s._conn() as c:
            rows=c.execute('SELECT * FROM dispatch_history ORDER BY id DESC LIMIT ?',(limit,)).fetchall()
            return [dict(r) for r in rows]

    def add_event(s,station_id,event,ok,detail=""):
        with s.lock:
            with s._conn() as c:
                c.execute('INSERT INTO station_events(time,station_id,event,ok,detail) VALUES(?,?,?,?,?)',
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),station_id,event,1 if ok else 0,detail[:200]))

    def get_events(s,limit=50):
        with s._conn() as c:
            rows=c.execute('SELECT * FROM station_events ORDER BY id DESC LIMIT ?',(limit,)).fetchall()
            return [dict(r) for r in rows]

    def get_active_dispatches(s):
        """Get all dispatches with active state — keyed by source position"""
        with s._conn() as c:
            rows=c.execute("SELECT * FROM dispatch_history WHERE state IN ('created','executing') ORDER BY id DESC").fetchall()
            return [dict(r) for r in rows]

db=DB(DB_FILE)

# ==================== RCS CLIENT ====================
class RCS:
    def __init__(s,c):
        s.base="http://{}:{}/rcms/services/rest/hikRpcService".format(c.get("host","127.0.0.1"),c.get("port",8182))
        s.cc=c.get("client_code","RCA_BEEPER");s.tc=c.get("token_code","");s.to=c.get("timeout",10)
        log.info("RCS: %s",s.base)
    def _rc(s): return "BPR{}{}".format(datetime.now().strftime("%Y%m%d%H%M%S"),uuid.uuid4().hex[:6].upper())
    def _bp(s,rc=None): return {"reqCode":rc or s._rc(),"reqTime":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"clientCode":s.cc,"tokenCode":s.tc}
    def gen_task(s,task_typ,positions,priority="1",data_field=""):
        rc=s._rc();p=dict(s._bp(rc));p["taskTyp"]=task_typ;p["positionCodePath"]=positions;p["priority"]=priority
        if data_field: p["data"]=data_field
        log.info("genTask: type=%s pos=%d",task_typ,len(positions))
        r=safe_http_post("{}/genAgvSchedulingTask".format(s.base),p,s.to);c=str(r.get("code","-1"))
        return {"success":c=="0","code":c,"message":r.get("message",""),"task_id":str(r.get("data","")) if r.get("data") else "","req_code":rc}
    def cancel_task(s,tc,force="1"):
        p=dict(s._bp());p["taskCode"]=tc;p["forceCancel"]=force
        r=safe_http_post("{}/cancelTask".format(s.base),p,s.to)
        return {"success":str(r.get("code","-1"))=="0","message":r.get("message","")}
    def query_task_status(s,tcs):
        p=dict(s._bp());p["taskCodes"]=tcs
        r=safe_http_post("{}/queryTaskStatus".format(s.base),p,s.to)
        return {"success":str(r.get("code","-1"))=="0","data":r.get("data",[])}

# ==================== STATION MANAGER ====================
IDLE="idle";CREATED="created";EXECUTING="executing";COMPLETE="complete";ERROR="error"
MAX_USAGE_HISTORY=20
def fmt_dur(secs):
    if secs is None or secs<0: return "-"
    secs=int(secs)
    if secs<60: return "{}s".format(secs)
    if secs<3600: return "{}m{}s".format(secs//60,secs%60)
    return "{}h{}m".format(secs//3600,(secs%3600)//60)

class Manager:
    def __init__(s,rcs,timing):
        s.rcs=rcs;s.timing=timing;s.stations={};s.task_map={};s.events=[];s.lock=threading.Lock()
    def register(s,cfg):
        s.stations[cfg["station_id"]]={
            "cfg":cfg,"state":IDLE,"task_id":None,"agv":None,
            "last_call":None,"last_complete":None,"last_error":"",
            "calls":0,"ok":0,"fail":0,
            "esp_ip":None,"esp_online":False,"esp_hb":None,
            "deliver_time":None,"press_time":None,"last_usage_sec":None,
            "usage_history":[],"total_usage_sec":0,"usage_count":0,
        }
    def _evt(s,sid,event,ok=True,detail=""):
        try:
            with s.lock:
                s.events.append({"time":datetime.now().strftime("%H:%M:%S"),"sid":sid,"event":event,"ok":ok,"detail":detail[:80]})
                if len(s.events)>500: s.events=s.events[-500:]
            log.log(logging.INFO if ok else logging.WARNING,"[%s] %s - %s",sid,event,detail[:80])
            db.add_event(sid,event,ok,detail)
        except: pass
    def _ss(s,st,state,msg=""):
        try:
            old=st["state"];st["state"]=state
            if state==ERROR: st["last_error"]=msg
            s._evt(st["cfg"]["station_id"],"{} -> {}".format(old,state),detail=msg)
            # Update dispatch DB state
            if st["task_id"]: db.update_dispatch_state(st["task_id"],state,msg)
        except: pass

    # ---------- BUTTON PRESS (MF01) ----------
    def handle_press(s,station_id,esp_ip=""):
        try:
            st=s.stations.get(station_id)
            if not st: return {"accepted":False,"state":"error","message":"Unknown: "+station_id}
            if not st["cfg"].get("enabled",True): return {"accepted":False,"state":"error","message":"Disabled"}
            if esp_ip: st["esp_ip"]=esp_ip
            st["esp_online"]=True;st["esp_hb"]=time.time();st["calls"]+=1
            if st["state"]!=IDLE:
                ms={CREATED:"Da goi AMR",EXECUTING:"AMR dang chay",COMPLETE:"Vua xong",ERROR:st["last_error"]}
                st["fail"]+=1;s._evt(station_id,"REJECTED",False,"state="+st["state"])
                return {"accepted":False,"state":st["state"],"message":ms.get(st["state"],"Busy")}
            now=time.time();st["press_time"]=now
            if st["deliver_time"] is not None:
                usage=now-st["deliver_time"];st["last_usage_sec"]=usage;st["total_usage_sec"]+=usage;st["usage_count"]+=1
                st["usage_history"].append({"time":datetime.now().strftime("%H:%M:%S"),"duration_sec":round(usage,1),"duration_fmt":fmt_dur(usage)})
                if len(st["usage_history"])>MAX_USAGE_HISTORY: st["usage_history"]=st["usage_history"][-MAX_USAGE_HISTORY:]
                s._evt(station_id,"RACK USED",True,"Usage: {}".format(fmt_dur(usage)))
            cfg=st["cfg"];positions=cfg.get("task_positions",[])
            if not positions: st["fail"]+=1;return {"accepted":False,"state":"error","message":"No positions"}
            s._ss(st,CREATED,"Calling RCS");st["last_call"]=time.time()
            r=s.rcs.gen_task(task_typ=cfg.get("task_type","MF01"),positions=positions,priority=cfg.get("task_priority","1"),data_field=json.dumps({"stationId":station_id}))
            if r["success"]:
                st["task_id"]=r["task_id"];st["ok"]+=1
                if r["task_id"]: s.task_map[r["task_id"]]=station_id
                s._ss(st,CREATED,"Task: "+r["task_id"]);s._push_async(st)
                db.add_dispatch(cfg.get("kitting",""),cfg.get("buffer",""),"",station_id,cfg.get("buffer",""),"MF01",r["task_id"],"created","Button call")
                return {"accepted":True,"state":"created","message":"Task {} created".format(r["task_id"])}
            else:
                st["fail"]+=1;err="RCS [{}]: {}".format(r["code"],r["message"]);s._ss(st,ERROR,err);s._push_async(st)
                return {"accepted":False,"state":"error","message":err}
        except Exception as e:
            log.error("press err: %s",traceback.format_exc())
            return {"accepted":False,"state":"error","message":"Internal error"}

    def get_state(s,sid,esp_ip=""):
        try:
            st=s.stations.get(sid)
            if not st: return {"state":"error","message":"Unknown"}
            if esp_ip: st["esp_ip"]=esp_ip
            st["esp_online"]=True;st["esp_hb"]=time.time()
            ms={IDLE:"San sang",CREATED:"Task {} - cho AMR".format(st["task_id"] or ""),
                EXECUTING:"AMR {} dang chay".format(st["agv"] or ""),COMPLETE:"Hoan thanh!",ERROR:st["last_error"]}
            return {"state":st["state"],"message":ms.get(st["state"],"")}
        except: return {"state":"error","message":"Internal error"}

    # ---------- agvCallback ----------
    def handle_callback(s,data):
        try:
            tc=data.get("taskCode","");method=data.get("method","");robot=data.get("robotCode","");custom=data.get("data","")
            sid=s.task_map.get(tc,"")
            if not sid and custom:
                try: sid=json.loads(custom).get("stationId","")
                except: pass
            if not sid:
                for st in s.stations.values():
                    if st["task_id"]==tc: sid=st["cfg"]["station_id"];break
            if not sid: return {"code":"0","message":"OK"}
            st=s.stations.get(sid)
            if not st: return {"code":"0","message":"OK"}
            if method=="start": st["agv"]=robot;s._ss(st,EXECUTING,"AMR {} started".format(robot));s._push_async(st)
            elif method=="outbin":
                if st["state"]!=EXECUTING: st["agv"]=robot;s._ss(st,EXECUTING,"AMR {} moving".format(robot));s._push_async(st)
            elif method=="end":
                s._ss(st,COMPLETE,"Done by AMR {}".format(robot));st["last_complete"]=time.time()
                st["deliver_time"]=time.time();s._push_async(st)
            elif method=="cancel":
                s._ss(st,ERROR,"Cancelled by RCS");s._push_async(st)
                # Explicitly update DB in case _ss didn't catch it
                if tc: db.update_dispatch_state(tc,"cancelled","Cancelled (agvCallback)")
            return {"code":"0","message":"successful","reqCode":data.get("reqCode","")}
        except Exception as e:
            log.error("Callback err: %s",e);return {"code":"0","message":"OK"}

    def _push_async(s,st):
        if not st["esp_ip"]: return
        sid=st["cfg"]["station_id"];ip=st["esp_ip"]
        try: threading.Thread(target=s._do_push,args=(sid,ip),daemon=True).start()
        except: pass
    def _do_push(s,sid,ip):
        try: safe_http_post("http://{}/api/push-state".format(ip),s.get_state(sid),timeout=3)
        except: pass

    # ---------- KITTING DISPATCH (MF02) ----------
    def handle_dispatch(s,data):
        try:
            kit_id=data.get("kitting_id","");src=data.get("source_position","")
            rack=data.get("rack_code","");dest=data.get("dest_station","")
            buf=data.get("dest_buffer","")
            if not all([kit_id,src,rack,dest]): return {"success":False,"message":"Missing fields"}
            # Check if source position already has active task
            active=db.get_active_dispatches()
            for ad in active:
                if ad.get("source")==src:
                    return {"success":False,"message":"Vi tri {} dang co task ({})".format(src,ad.get("state","?"))}
            st=s.stations.get(dest)
            if not st: return {"success":False,"message":"Unknown: "+dest}
            if not st["cfg"].get("enabled",True): return {"success":False,"message":"Disabled"}
            if st["state"]!=IDLE: return {"success":False,"message":"{} busy ({})".format(dest,st["state"])}
            kd=CFG.get("kitting_dispatch",{})
            task_typ=kd.get("task_type","MF02");priority=kd.get("task_priority","1");pt=kd.get("position_type","00")
            buffer=buf or st["cfg"].get("buffer",dest+"B")
            positions=[{"positionCode":src,"type":pt},{"positionCode":buffer,"type":pt}]
            s._ss(st,CREATED,"PDA: {} -> {} (MF02)".format(src,buffer));st["last_call"]=time.time()
            data_json=json.dumps({"stationId":dest,"source":src,"buffer":buffer,"rack":rack,"kitting":kit_id,"via":"PDA"})
            r=s.rcs.gen_task(task_typ=task_typ,positions=positions,priority=priority,data_field=data_json)
            if r["success"]:
                st["task_id"]=r["task_id"];st["ok"]+=1;st["calls"]+=1
                if r["task_id"]: s.task_map[r["task_id"]]=dest
                s._ss(st,CREATED,"PDA Task: "+r["task_id"]);s._push_async(st)
                db.add_dispatch(kit_id,src,rack,dest,buffer,task_typ,r["task_id"],"created","PDA dispatch")
                return {"success":True,"message":"{} -> {} ({})".format(src,buffer,task_typ),"task_id":r["task_id"]}
            else:
                st["fail"]+=1;st["calls"]+=1
                err="RCS [{}]: {}".format(r["code"],r["message"]);s._ss(st,ERROR,err);s._push_async(st)
                db.add_dispatch(kit_id,src,rack,dest,buffer,task_typ,"","error",err)
                return {"success":False,"message":err}
        except Exception as e:
            log.error("Dispatch err: %s",traceback.format_exc())
            return {"success":False,"message":"Error: "+str(e)[:60]}

    # ---------- ADMIN ----------
    def force_reset(s,sid):
        try:
            st=s.stations.get(sid)
            if not st: return {"success":False,"message":"Not found"}
            # Update DB before clearing task_id
            if st["task_id"]: db.update_dispatch_state(st["task_id"],"cancelled","Admin reset")
            if st["task_id"] and st["task_id"] in s.task_map: del s.task_map[st["task_id"]]
            s._ss(st,IDLE,"Admin reset");st["task_id"]=None;st["agv"]=None;st["last_error"]="";s._push_async(st)
            return {"success":True,"message":"Reset OK"}
        except Exception as e: return {"success":False,"message":str(e)}
    def cancel_current(s,sid):
        try:
            st=s.stations.get(sid)
            if not st: return {"success":False,"message":"Not found"}
            if st["task_id"]:
                tc=st["task_id"];threading.Thread(target=lambda:s.rcs.cancel_task(tc),daemon=True).start()
            return s.force_reset(sid)
        except Exception as e: return {"success":False,"message":str(e)}

    def cancel_dispatch(s,dispatch_id):
        """Cancel a dispatch from PDA — cancel task on RCS + update DB + reset station"""
        try:
            d=db.cancel_dispatch_by_id(dispatch_id)
            if not d: return {"success":False,"message":"Dispatch #{} not found".format(dispatch_id)}
            if d["state"] in ("complete","cancelled"):
                return {"success":False,"message":"Already {} — cannot cancel".format(d["state"])}
            # Cancel on RCS
            task_id=d.get("task_id","")
            if task_id:
                try: s.rcs.cancel_task(task_id)
                except: pass
                db.update_dispatch_state(task_id,"cancelled","Cancelled from PDA")
            # Reset station if it still has this task
            sta_id=d.get("station","")
            if sta_id:
                st=s.stations.get(sta_id)
                if st and st["task_id"]==task_id:
                    if st["task_id"] in s.task_map: del s.task_map[st["task_id"]]
                    s._ss(st,IDLE,"PDA cancel");st["task_id"]=None;st["agv"]=None;s._push_async(st)
            s._evt(sta_id or "?","PDA CANCEL",True,"Dispatch #{} cancelled".format(dispatch_id))
            return {"success":True,"message":"Dispatch #{} cancelled".format(dispatch_id)}
        except Exception as e:
            log.error("Cancel dispatch err: %s",e)
            return {"success":False,"message":str(e)[:60]}

    # ---------- BACKGROUND ----------
    def start_background(s):
        def wd():
            while True:
                try: s._bg()
                except Exception as e: log.error("BG crash: %s",e);time.sleep(5)
        threading.Thread(target=wd,daemon=True).start()
    def _bg(s):
        tm=s.timing;ch=tm.get("complete_hold_sec",10);eh=tm.get("error_hold_sec",15)
        tt=tm.get("task_timeout_sec",600);hbt=tm.get("heartbeat_timeout_sec",30)
        piv=tm.get("task_poll_interval_sec",30);lp=time.time()
        while True:
            now=time.time()
            for st in list(s.stations.values()):
                try:
                    if st["state"]==COMPLETE and st["last_complete"] and now-st["last_complete"]>=ch:
                        if st["task_id"] and st["task_id"] in s.task_map:
                            try: del s.task_map[st["task_id"]]
                            except: pass
                        s._ss(st,IDLE,"Auto-reset");st["task_id"]=None;st["agv"]=None;s._push_async(st)
                    elif st["state"]==ERROR and st["last_call"] and now-st["last_call"]>=eh:
                        s._ss(st,IDLE,"Error reset");st["task_id"]=None;st["agv"]=None;s._push_async(st)
                    elif st["state"] in (CREATED,EXECUTING) and st["last_call"] and now-st["last_call"]>=tt:
                        st["fail"]+=1;s._ss(st,ERROR,"Timeout");s._push_async(st)
                    if st["esp_hb"]: st["esp_online"]=(now-st["esp_hb"])<hbt
                except: pass
            if now-lp>=piv:
                lp=now
                try: s._poll()
                except: pass
            time.sleep(1)
    def _poll(s):
        # 1. Poll tasks from station memory (existing logic)
        active=[st["task_id"] for st in s.stations.values() if st["state"] in (CREATED,EXECUTING) and st["task_id"]]
        # 2. ALSO poll orphan tasks from DB (tasks where station was reset but DB still active)
        db_active=db.get_active_task_ids()
        for da in db_active:
            tid=da.get("task_id","")
            if tid and tid not in active: active.append(tid)
        if not active: return
        r=s.rcs.query_task_status(active)
        if not r["success"] or not r["data"]: return
        for t in r["data"]:
            try:
                tc=t.get("taskCode","");ts=t.get("taskStatus","");agv=t.get("agvCode","")
                # Update station memory if station still has this task
                sid=s.task_map.get(tc)
                if sid:
                    st=s.stations.get(sid)
                    if st:
                        if ts=="2" and st["state"]==CREATED: st["agv"]=agv;s._ss(st,EXECUTING,"AMR {} (polled)".format(agv));s._push_async(st)
                        elif ts=="9" and st["state"]!=COMPLETE: s._ss(st,COMPLETE,"Done (polled)");st["last_complete"]=time.time();st["deliver_time"]=time.time();s._push_async(st)
                        elif ts=="5" and st["state"] not in (IDLE,ERROR): s._ss(st,ERROR,"Cancelled (polled)");s._push_async(st)
                # ALWAYS update DB directly (catches orphan tasks too)
                if ts=="9": db.update_dispatch_state(tc,"complete","Completed (RCS)")
                elif ts=="5": db.update_dispatch_state(tc,"cancelled","Cancelled on RCS")
                elif ts=="2": db.update_dispatch_state(tc,"executing","AMR {}".format(agv))
                elif ts=="0": db.update_dispatch_state(tc,"error","Send exception (RCS)")
            except: pass

    # ---------- DATA ----------
    def station_data(s,sid):
        st=s.stations.get(sid)
        if not st: return None
        now=time.time()
        iu=None
        if st["state"]==IDLE and st["deliver_time"] is not None: iu=now-st["deliver_time"]
        avg=st["total_usage_sec"]/st["usage_count"] if st["usage_count"]>0 else None
        return {"id":sid,"name":st["cfg"]["station_name"],"state":st["state"],"task":st["task_id"],
            "agv":st["agv"],"err":st["last_error"] if st["state"]==ERROR else "",
            "calls":st["calls"],"ok":st["ok"],"fail":st["fail"],
            "ip":st["esp_ip"],"online":st["esp_online"],
            "kitting":st["cfg"].get("kitting",""),"buffer":st["cfg"].get("buffer",sid+"B"),
            "in_use_sec":iu,"in_use_fmt":fmt_dur(iu) if iu else "-",
            "last_usage_fmt":fmt_dur(st["last_usage_sec"]) if st["last_usage_sec"] else "-",
            "avg_usage_fmt":fmt_dur(avg) if avg else "-","usage_count":st["usage_count"]}
    def overview(s):
        ss=list(s.stations.values())
        tu=sum(st["total_usage_sec"] for st in ss);tc=sum(st["usage_count"] for st in ss)
        return {"total":len(ss),"online":sum(1 for x in ss if x["esp_online"]),
            "idle":sum(1 for x in ss if x["state"]==IDLE),
            "active":sum(1 for x in ss if x["state"] in (CREATED,EXECUTING)),
            "errors":sum(1 for x in ss if x["state"]==ERROR),
            "calls":sum(x["calls"] for x in ss),
            "ok_total":sum(x["ok"] for x in ss),"fail_total":sum(x["fail"] for x in ss),
            "avg_usage_fmt":fmt_dur(tu/tc) if tc>0 else "-","total_usage_cycles":tc}
    def recent(s,n=30): return list(reversed(s.events[-n:]))

# ==================== INIT ====================
rcs=RCS(CFG.get("rcs",{}))
mgr=Manager(rcs,CFG.get("timing",{}))
for sc in CFG.get("stations",[]): mgr.register(sc)
log.info("Loaded %d stations",len(mgr.stations))

# ==================== HTTP ====================
class H(BaseHTTPRequestHandler):
    timeout=30
    def log_message(s,f,*a): pass
    def _json(s,d,code=200):
        try:
            b=json.dumps(d,ensure_ascii=False).encode("utf-8");s.send_response(code)
            s.send_header("Content-Type","application/json; charset=utf-8")
            s.send_header("Access-Control-Allow-Origin","*");s.send_header("Content-Length",len(b))
            s.end_headers();s.wfile.write(b)
        except: pass
    def _html(s,h):
        try:
            b=h.encode("utf-8");s.send_response(200);s.send_header("Content-Type","text/html; charset=utf-8")
            s.send_header("Access-Control-Allow-Origin","*");s.send_header("Content-Length",len(b))
            s.end_headers();s.wfile.write(b)
        except: pass
    def _body(s):
        try:
            n=int(s.headers.get("Content-Length",0))
            if 0<n<1000000: return json.loads(s.rfile.read(n).decode("utf-8"))
        except: pass
        return {}
    def _ip(s):
        try: return s.client_address[0]
        except: return ""
    def _serve_file(s,fname):
        try:
            fp=os.path.join(os.path.dirname(os.path.abspath(__file__)),fname)
            if os.path.exists(fp):
                with open(fp,"r",encoding="utf-8") as f: s._html(f.read())
            else: s._html("<h1>{} not found</h1>".format(fname))
        except Exception as e: s._html("<h1>Error</h1><pre>{}</pre>".format(e))
    def do_GET(s):
        try:
            p=urlparse(s.path).path
            if p=="/": s._html(dashboard())
            elif p=="/pda": s._serve_file("pda.html")
            elif p.startswith("/api/v1/beeper/") and p.endswith("/state"):
                sid=p.replace("/api/v1/beeper/","").replace("/state","");s._json(mgr.get_state(sid,s._ip()))
            elif p=="/api/v1/admin/stations": s._json([mgr.station_data(sid) for sid in mgr.stations if mgr.station_data(sid)])
            elif p=="/api/v1/admin/overview": s._json(mgr.overview())
            elif p=="/api/v1/admin/events": s._json(mgr.recent(50))
            elif p.startswith("/api/v1/admin/usage/"):
                sid=p.split("/")[-1];d=mgr.station_data(sid)
                s._json(d if d else {})
            elif p=="/api/v1/pda/config":
                s._json({"kitting_layout":CFG.get("kitting_layout",[]),"kitting_dispatch":CFG.get("kitting_dispatch",{}),
                    "stations":[mgr.station_data(sid) for sid in mgr.stations if mgr.station_data(sid)]})
            elif p=="/api/v1/kitting/history":
                s._json(db.get_history(50))
            elif p=="/api/v1/kitting/active":
                active=db.get_active_dispatches()
                by_pos={};by_sta={}
                for d in active:
                    if d.get("source"): by_pos[d["source"]]=d
                    if d.get("station"): by_sta[d["station"]]=d
                s._json({"by_position":by_pos,"by_station":by_sta,"count":len(active)})
            elif p=="/health":
                o=mgr.overview();s._json({"status":"ok","stations":o["total"],"online":o["online"],
                    "uptime":int(time.time()-START_TIME),"threads":threading.active_count()})
            else: s._json({"error":"Not found"},404)
        except Exception as e:
            log.error("GET err: %s",e)
            try: s._json({"error":"Internal"},500)
            except: pass
    def do_POST(s):
        try:
            p=urlparse(s.path).path;d=s._body()
            if p=="/api/v1/beeper/press": s._json(mgr.handle_press(d.get("station_id",""),s._ip()))
            elif p in ("/api/v1/rcs/agvCallback","/api/v1/rcs/agvCallbackService/agvCallback"):
                s._json(mgr.handle_callback(d))
            elif p.startswith("/api/v1/admin/reset/"): s._json(mgr.force_reset(p.split("/")[-1]))
            elif p.startswith("/api/v1/admin/cancel/"): s._json(mgr.cancel_current(p.split("/")[-1]))
            elif p=="/api/v1/kitting/dispatch": s._json(mgr.handle_dispatch(d))
            elif p=="/api/v1/kitting/cancel": s._json(mgr.cancel_dispatch(d.get("dispatch_id",0)))
            else: s._json({"error":"Not found"},404)
        except Exception as e:
            log.error("POST err: %s",e)
            try: s._json({"error":"Internal"},500)
            except: pass
    def do_OPTIONS(s):
        try:
            s.send_response(204);s.send_header("Access-Control-Allow-Origin","*")
            s.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS")
            s.send_header("Access-Control-Allow-Headers","Content-Type");s.end_headers()
        except: pass

# ==================== DASHBOARD ====================
def dashboard():
    try:
        o=mgr.overview();events=mgr.recent(15);rc=CFG.get("rcs",{});layout=CFG.get("kitting_layout",[])
        SC={"idle":"#3d4f5f","created":"#f39c12","executing":"#2e86de","complete":"#27ae60","error":"#e74c3c"}
        BG={"idle":"#1a2332","created":"#4a3800","executing":"#0a2a4a","complete":"#0a3a1a","error":"#3a0a0a"}
        LBL={"idle":"IDLE","created":"CALLING","executing":"AMR \u25b6","complete":"DONE \u2713","error":"ERROR \u2717"}
        def sbox(sid):
            d=mgr.station_data(sid)
            if not d: return '<div class="sn empty">'+sid+'</div>'
            st=d["state"];sc=SC.get(st,"#3d4f5f");bg=BG.get(st,"#1a2332");lbl=LBL.get(st,"?")
            dot='<span class="dot-on"></span>' if d["online"] else '<span class="dot-off"></span>'
            extra=""
            if d["agv"]: extra='<div class="sx">AMR '+str(d["agv"])+'</div>'
            elif d["task"]: extra='<div class="sx">'+str(d["task"])[:16]+'</div>'
            elif d["err"]: extra='<div class="sx" style="color:#e74c3c">'+str(d["err"])[:20]+'</div>'
            rack=""
            if st==IDLE and d["in_use_sec"] and d["in_use_sec"]>0:
                sec=int(d["in_use_sec"]);tc="#27ae60" if sec<1800 else ("#f39c12" if sec<3600 else "#e74c3c")
                rack='<div class="rack-timer" style="color:{}">\u23f1 {}</div>'.format(tc,d["in_use_fmt"])
            elif d["last_usage_fmt"]!="-": rack='<div class="rack-last">Last: {}</div>'.format(d["last_usage_fmt"])
            return '''<div class="sn" style="border-color:{sc};background:{bg}" onclick="showMenu('{sid}')" title="{name} | Avg: {avg}">
<div class="sn-hdr">{dot}<b>{sid}</b></div><div class="sn-state" style="color:{sc}">{lbl}</div>{extra}{rack}
<div class="sn-stats">{calls} | <span style="color:#27ae60">\u2713{ok_}</span> <span style="color:#e74c3c">\u2717{fail}</span></div>
</div>'''.format(sc=sc,bg=bg,sid=sid,name=d["name"],dot=dot,lbl=lbl,extra=extra,rack=rack,calls=d["calls"],ok_=d["ok"],fail=d["fail"],avg=d["avg_usage_fmt"])
        def kbox(kit):
            kid=kit["kitting_id"];kname=kit["kitting_name"];lines=kit["lines"]
            active=0;total=0
            for line in lines:
                for sid in line:
                    d=mgr.station_data(sid)
                    if d: total+=1
                    if d and d["state"] in (CREATED,EXECUTING): active+=1
            lh=""
            for line in lines:
                nodes="".join(['<div class="arrow">\u2192</div>'+sbox(sid) for sid in line])
                lh+='<div class="flow-line">{}</div>'.format(nodes)
            return '''<div class="kit-group"><div class="kit-box"><div class="kit-name">{kn}</div><div class="kit-id">{kid}</div>
<div class="kit-stats">{t} tr\u1ea1m | {a} active</div></div><div class="kit-lines">{l}</div></div>'''.format(kn=kname,kid=kid,t=total,a=active,l=lh)
        flow="".join([kbox(k) for k in layout])
        rows=""
        for ev in events:
            c="#27ae60" if ev["ok"] else "#e74c3c"
            rows+='<tr style="color:{}"><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(c,ev["time"],ev["sid"],ev["event"],ev["detail"][:55])
        if not events: rows='<tr><td colspan="4" style="color:#3d4f5f">No events</td></tr>'
        up=int(time.time()-START_TIME);uh=up//3600;um=(up%3600)//60
        return '''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RCA Dashboard</title>
<style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:'Segoe UI',system-ui,sans-serif;background:#0a0e17;color:#c8d6e5;min-height:100vh}}
.hdr{{background:linear-gradient(135deg,#0d1525,#1a1a3e);padding:14px 20px;display:flex;justify-content:space-between;align-items:center;border-bottom:2px solid #1e3a5f;flex-wrap:wrap;gap:8px}}
.hdr h1{{font-size:1.1em;font-weight:800;color:#fff;letter-spacing:2px}}.hdr .sub{{color:#576574;font-size:.7em}}
.pills{{display:flex;gap:6px;flex-wrap:wrap}}.pill{{padding:4px 12px;border-radius:12px;font-size:.7em;font-weight:700;text-align:center;min-width:50px}}
.pill .pv{{font-size:1.3em;display:block}}.p-w{{background:#1a2332;color:#fff}}.p-g{{background:#0a2e1a;color:#27ae60}}.p-b{{background:#0a1a2e;color:#2e86de}}
.p-y{{background:#2e2a0a;color:#f39c12}}.p-r{{background:#2e0a0a;color:#e74c3c}}.p-c{{background:#0a2e2e;color:#00cec9}}
.cnt{{padding:12px 16px;max-width:1600px;margin:0 auto}}.sec-title{{font-size:.7em;color:#576574;text-transform:uppercase;letter-spacing:2px;margin:14px 0 8px;padding-bottom:4px;border-bottom:1px solid #1e2738}}
.kit-group{{margin-bottom:14px;background:#0d1117;border:1px solid #1e2738;border-radius:10px;padding:12px;display:flex;align-items:flex-start;gap:8px;flex-wrap:wrap}}
.kit-box{{background:linear-gradient(135deg,#1a3a5f,#0d2240);border:2px solid #2e86de;border-radius:8px;padding:10px 14px;min-width:110px;text-align:center;flex-shrink:0}}
.kit-name{{font-weight:800;color:#fff;font-size:.9em}}.kit-id{{font-family:monospace;font-size:.6em;color:#576574}}.kit-stats{{font-size:.6em;color:#2e86de;margin-top:3px;font-weight:600}}
.kit-lines{{flex:1}}.flow-line{{display:flex;align-items:center;margin:3px 0;gap:2px;flex-wrap:nowrap}}
.arrow{{color:#3d4f5f;font-size:.85em;font-weight:700;padding:0 1px;min-width:16px;text-align:center}}
.sn{{border:2px solid #3d4f5f;border-radius:6px;padding:5px 7px;min-width:100px;cursor:pointer;transition:all .15s;text-align:center;flex-shrink:0}}
.sn:hover{{transform:scale(1.05);box-shadow:0 0 10px rgba(46,134,222,.3)}}
.sn-hdr{{font-size:.7em;display:flex;align-items:center;justify-content:center;gap:3px}}.sn-hdr b{{color:#fff;font-size:.9em}}
.sn-state{{font-family:monospace;font-size:.72em;font-weight:800;margin:2px 0;letter-spacing:.5px}}
.sx{{font-size:.58em;color:#576574;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:92px}}
.sn-stats{{font-size:.55em;color:#576574}}.rack-timer{{font-family:monospace;font-size:.68em;font-weight:700;margin:1px 0}}.rack-last{{font-family:monospace;font-size:.55em;color:#576574}}
.dot-on{{width:5px;height:5px;border-radius:50%;background:#27ae60;box-shadow:0 0 4px #27ae60;display:inline-block}}
.dot-off{{width:5px;height:5px;border-radius:50%;background:#e74c3c;display:inline-block}}
.legend{{display:flex;gap:10px;margin:6px 0;flex-wrap:wrap}}.leg-item{{display:flex;align-items:center;gap:4px;font-size:.62em}}.leg-dot{{width:10px;height:10px;border-radius:2px}}
.menu-overlay{{display:none;position:fixed;top:0;left:0;width:100%;height:100%;z-index:100}}
.menu{{position:fixed;background:#1a2332;border:1px solid #2e86de;border-radius:8px;padding:8px;z-index:101;display:none;min-width:140px;box-shadow:0 4px 20px rgba(0,0,0,.5)}}
.menu a{{display:block;padding:6px 12px;color:#c8d6e5;text-decoration:none;font-size:.78em;border-radius:4px;font-weight:600}}.menu a:hover{{background:#2e86de20}}
.menu .m-title{{color:#576574;font-size:.65em;padding:4px 12px;text-transform:uppercase;letter-spacing:1px}}
table{{width:100%;border-collapse:collapse;font-size:.66em;margin-top:4px}}
th{{text-align:left;padding:3px 5px;color:#576574;border-bottom:1px solid #1e2738;font-size:.7em;text-transform:uppercase}}
td{{padding:3px 5px;border-bottom:1px solid #0d111780;font-family:monospace}}
.ft{{text-align:center;padding:8px;font-size:.6em;color:#3d4f5f;margin-top:10px}}.ft a{{color:#00cec9;text-decoration:none;font-weight:700}}
@media(max-width:1000px){{.kit-group{{flex-direction:column}}.flow-line{{overflow-x:auto}}.sn{{min-width:80px}}}}
</style></head><body>
<div class="hdr"><div><h1>RCA BEEPER DASHBOARD</h1>
<div class="sub">v3.8 \u2014 RCS @ {host}:{port} \u2014 Up: {uh}h{um}m</div></div>
<div class="pills">
<div class="pill p-w"><span class="pv">{total}</span>Stations</div>
<div class="pill p-g"><span class="pv">{online}</span>Online</div>
<div class="pill p-b"><span class="pv">{idle}</span>Idle</div>
<div class="pill p-y"><span class="pv">{active}</span>Active</div>
<div class="pill p-r"><span class="pv">{errors}</span>Error</div>
<div class="pill p-c"><span class="pv">{avg_usage_fmt}</span>Avg Use</div>
<div class="pill p-c"><span class="pv">{total_usage_cycles}</span>Cycles</div>
</div></div>
<div class="cnt">
<div class="legend">
<div class="leg-item"><div class="leg-dot" style="background:#3d4f5f"></div>Idle</div>
<div class="leg-item"><div class="leg-dot" style="background:#f39c12"></div>Calling</div>
<div class="leg-item"><div class="leg-dot" style="background:#2e86de"></div>AMR</div>
<div class="leg-item"><div class="leg-dot" style="background:#27ae60"></div>Done</div>
<div class="leg-item"><div class="leg-dot" style="background:#e74c3c"></div>Error</div>
<div class="leg-item">\u23f1 Rack timer</div>
</div>
<div class="sec-title">Kitting \u2192 Station Flow</div>{flow}
<div class="sec-title">Recent Events</div>
<table><tr><th>Time</th><th>Station</th><th>Event</th><th>Detail</th></tr>{rows}</table>
<div class="ft"><a href="/pda">&#x1F4F1; PDA Kitting App</a> \u2014 RCA v3.8</div>
</div>
<div class="menu-overlay" id="menuOverlay" onclick="hideMenu()"></div>
<div class="menu" id="ctxMenu"><div class="m-title" id="menuTitle">Station</div>
<a style="color:#2e86de" href="#" onclick="doAct('reset')">\u21ba Reset</a>
<a style="color:#e74c3c" href="#" onclick="doAct('cancel')">\u2717 Cancel</a></div>
<script>var curSid='';
function showMenu(sid){{curSid=sid;var m=document.getElementById('ctxMenu'),o=document.getElementById('menuOverlay');
document.getElementById('menuTitle').textContent=sid;m.style.display='block';o.style.display='block';
var e=event||window.event;m.style.left=Math.min(e.clientX,innerWidth-160)+'px';m.style.top=Math.min(e.clientY,innerHeight-100)+'px'}}
function hideMenu(){{document.getElementById('ctxMenu').style.display='none';document.getElementById('menuOverlay').style.display='none'}}
function doAct(a){{hideMenu();if(!confirm(a+' '+curSid+'?'))return;
fetch('/api/v1/admin/'+a+'/'+curSid,{{method:'POST'}}).then(function(r){{return r.json()}}).then(function(d){{alert(d.message||JSON.stringify(d));location.reload()}}).catch(function(e){{alert(e)}})}}
setTimeout(function(){{location.reload()}},4000);</script></body></html>'''.format(
            host=rc.get("host","?"),port=rc.get("port","?"),flow=flow,rows=rows,uh=uh,um=um,**o)
    except Exception as e:
        log.error("Dashboard err: %s",traceback.format_exc())
        return "<html><body><h1>Error</h1><pre>{}</pre></body></html>".format(str(e))

# ==================== MAIN ====================
START_TIME=time.time()
def main():
    port=CFG.get("server",{}).get("port",8990);host=CFG.get("server",{}).get("host","0.0.0.0")
    for i,a in enumerate(sys.argv):
        if a=="--port" and i+1<len(sys.argv): port=int(sys.argv[i+1])
    mgr.start_background()
    srv=ThreadedHTTPServer((host,port),H)
    log.info("")
    log.info("============================================")
    log.info("  RCA BEEPER MIDDLEWARE v3.8")
    log.info("  Dashboard: http://%s:%s",host,port)
    log.info("  PDA:       http://%s:%s/pda",host,port)
    log.info("  Stations: %d  |  Kitting: %d",len(mgr.stations),len(CFG.get("kitting_layout",[])))
    log.info("  RCS: %s",rcs.base)
    log.info("  DB: %s",DB_FILE)
    log.info("============================================")
    try: srv.serve_forever()
    except KeyboardInterrupt: log.info("Shutdown");srv.shutdown()

if __name__=="__main__": main()
