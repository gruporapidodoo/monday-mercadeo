"""Tablero de Mercadeo Grupo Rapid: servidor pequeño.
Guarda el tablero en Postgres (Render) para que no se pierda cuando el servicio reinicia,
lo comparte entre Hilda e Irian con contraseña y publica un calendario .ics por persona para Google Calendar."""
import atexit, datetime, hashlib, hmac, json, os, threading, time
from flask import Flask, Response, jsonify, request, send_from_directory

import psycopg
DATABASE_URL = os.environ["DATABASE_URL"]
APP_PASSWORD = os.environ["APP_PASSWORD"]
SECRET = os.environ["SECRET"].encode()
CAL_KEY = os.environ["CAL_KEY"]

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024
lock = threading.Lock()
S = {"ver": 0, "data": None, "dirty": False}


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def load():
    with db() as c:
        c.execute("create table if not exists tablero (id text primary key, ver int not null, doc jsonb not null, actualizado timestamptz default now())")
        c.execute("create table if not exists tablero_historial (ver int, doc jsonb, guardado timestamptz default now())")
        row = c.execute("select ver, doc from tablero where id='mercadeo'").fetchone()
    if row: S.update(ver=row[0], data=row[1])


def save():
    with lock:
        if not S["dirty"]: return
        ver, doc = S["ver"], json.dumps(S["data"], ensure_ascii=False); S["dirty"] = False
    try:
        with db() as c:
            c.execute("update tablero set ver=%s, doc=%s::jsonb, actualizado=now() where id='mercadeo'", (ver, doc))
            if ver % 50 == 0:   # copia de respaldo cada 50 cambios, se guardan las últimas 60
                c.execute("insert into tablero_historial (ver, doc) values (%s, %s::jsonb)", (ver, doc))
                c.execute("delete from tablero_historial where ver not in (select ver from tablero_historial order by guardado desc limit 60)")
    except Exception:
        with lock: S["dirty"] = True
        raise


def writer():
    while True:
        time.sleep(2)
        try: save()
        except Exception as e: print("guardar:", e, flush=True)


load()
threading.Thread(target=writer, daemon=True).start()
atexit.register(save)


def token(): return hmac.new(SECRET, b"mercadeo-ok", hashlib.sha256).hexdigest()
def authed(): return hmac.compare_digest(request.cookies.get("mk", ""), token())


@app.get("/")
def index():
    r = send_from_directory("static", "index.html"); r.headers["Cache-Control"] = "no-cache"; return r


@app.get("/healthz")
def health(): return "ok"


@app.post("/api/login")
def login():
    pw = (request.get_json(silent=True) or {}).get("pw", "")
    if not hmac.compare_digest(pw.encode(), APP_PASSWORD.encode()):
        time.sleep(1); return jsonify(error="no"), 401
    r = jsonify(ok=True)
    r.set_cookie("mk", token(), max_age=60 * 60 * 24 * 180, httponly=True, secure=True, samesite="Lax")
    return r


@app.get("/api/state")
def get_state():
    if not authed(): return jsonify(error="auth"), 401
    if S["data"] is None: return jsonify(error="vacío"), 503
    v = request.args.get("v")
    if v is not None and v == str(S["ver"]): return "", 204
    return jsonify(ver=S["ver"], data=S["data"], calKey=CAL_KEY)


@app.post("/api/seed")
def seed():
    """Carga inicial: solo funciona si el tablero está vacío."""
    if not authed(): return jsonify(error="auth"), 401
    b = request.get_json(silent=True) or {}
    if not isinstance(b.get("data"), dict) or "boards" not in b["data"]: return jsonify(error="datos"), 400
    with lock:
        if S["data"] is not None: return jsonify(error="ya tiene datos"), 409
        with db() as c:
            c.execute("insert into tablero (id, ver, doc) values ('mercadeo', 1, %s::jsonb) on conflict (id) do nothing", (json.dumps(b["data"], ensure_ascii=False),))
        S.update(ver=1, data=b["data"])
    return jsonify(ver=1)


@app.post("/api/state")
def post_state():
    if not authed(): return jsonify(error="auth"), 401
    b = request.get_json(silent=True) or {}
    if not isinstance(b.get("data"), dict) or "boards" not in b["data"]: return jsonify(error="datos"), 400
    with lock:
        if S["data"] is None: return jsonify(error="vacío"), 503
        if b.get("base") != S["ver"]: return jsonify(ver=S["ver"], data=S["data"]), 409
        S["ver"] += 1; S["data"] = b["data"]; S["dirty"] = True
        return jsonify(ver=S["ver"])


def esc(t): return str(t).replace("\\", "\\\\").replace(",", "\\,").replace(";", "\\;").replace("\n", "\\n")


@app.get("/cal/<pid>.ics")
def cal(pid):
    if not hmac.compare_digest(request.args.get("k", ""), CAL_KEY): return "no", 403
    D = S["data"] or {}; names = {p["id"]: p["name"] for p in D.get("people", [])}
    if pid not in names: return "no", 404
    L = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Grupo Rapid//Mercadeo//ES", "CALSCALE:GREGORIAN", "METHOD:PUBLISH",
         "X-WR-CALNAME:Mercadeo · " + names[pid], "X-WR-TIMEZONE:America/Panama", "REFRESH-INTERVAL;VALUE=DURATION:PT1H", "X-PUBLISHED-TTL:PT1H"]
    stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    def ev(uid, title, a, z, desc):
        z2 = (datetime.date.fromisoformat(z) + datetime.timedelta(1)).strftime("%Y%m%d")
        L.extend(["BEGIN:VEVENT", "UID:%s@mercadeo-gruporapid" % uid, "DTSTAMP:" + stamp, "DTSTART;VALUE=DATE:" + a.replace("-", ""),
                  "DTEND;VALUE=DATE:" + z2, "SUMMARY:" + esc(title), "DESCRIPTION:" + esc(desc), "END:VEVENT"])
    for b in D.get("boards", []):
        done = set(D.get("doneStatus", {}).get(b["statusSet"], [])); gdone = set(D.get("doneStatus", {}).get("general", []))
        for it in b["items"]:
            if it.get("owner") == pid and it.get("due") and it.get("status") not in done:
                a = it.get("start") if it.get("start") and it["start"] <= it["due"] else it["due"]
                ev(it["id"], b["icon"] + " " + it["name"], a, it["due"], b["name"])
            for s in it.get("subs") or []:
                if s.get("owner") == pid and s.get("due") and s.get("status") not in gdone:
                    ev(s["id"], "☐ " + s["name"], s["due"], s["due"], it["name"] + " · " + b["name"])
    L.append("END:VCALENDAR")
    return Response("\r\n".join(L), mimetype="text/calendar; charset=utf-8")
