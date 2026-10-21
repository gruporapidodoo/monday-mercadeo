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
app.config["MAX_CONTENT_LENGTH"] = 160 * 1024 * 1024
lock = threading.Lock()
S = {"ver": 0, "data": None, "dirty": False, "saved": 0, "err": "", "beat": 0}


MEMORY = DATABASE_URL.startswith("memory:")     # solo para probar en local: memory:<archivo semilla>


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def load():
    if MEMORY:
        S.update(ver=1, data=json.load(open(DATABASE_URL[7:], encoding="utf-8"))); return
    with db() as c:
        c.execute("create table if not exists tablero (id text primary key, ver int not null, doc jsonb not null, actualizado timestamptz default now())")
        c.execute("create table if not exists tablero_historial (ver int, doc jsonb, guardado timestamptz default now())")
        row = c.execute("select ver, doc from tablero where id='mercadeo'").fetchone()
    if row: S.update(ver=row[0], data=row[1])


def save():
    with lock:
        if not S["dirty"]: return
        ver, doc = S["ver"], json.dumps(S["data"], ensure_ascii=False); S["dirty"] = False
    if MEMORY: return
    try:
        with db() as c:
            n = c.execute("update tablero set ver=%s, doc=%s::jsonb, actualizado=now() where id='mercadeo'", (ver, doc)).rowcount
            if n == 0:
                c.execute("insert into tablero (id, ver, doc) values ('mercadeo', %s, %s::jsonb)", (ver, doc))
            S["saved"] = ver
            if ver % 50 == 0:   # copia de respaldo cada 50 cambios, se guardan las últimas 60
                c.execute("insert into tablero_historial (ver, doc) values (%s, %s::jsonb)", (ver, doc))
                c.execute("delete from tablero_historial where ver not in (select ver from tablero_historial order by guardado desc limit 60)")
    except Exception:
        with lock: S["dirty"] = True
        raise


def writer():
    while True:
        time.sleep(2)
        S["beat"] = int(time.time())
        try: save(); S["err"] = ""
        except Exception as e: S["err"] = repr(e)[:300]; print("guardar:", e, flush=True)


load()
_W = {"pid": None}


def ensure_writer():
    """Gunicorn puede cargar la app antes de crear el proceso que atiende (--preload): el hilo se arranca en ese proceso."""
    if _W["pid"] != os.getpid():
        _W["pid"] = os.getpid()
        threading.Thread(target=writer, daemon=True).start()
        atexit.register(save)


@app.before_request
def _antes(): ensure_writer()


if os.environ.get("TEST_DELAY"):
    @app.before_request
    def _demora(): time.sleep(float(os.environ["TEST_DELAY"]))


def token(): return hmac.new(SECRET, b"mercadeo-ok", hashlib.sha256).hexdigest()
def authed():
    t = request.cookies.get("mk", "") or request.args.get("t", "")
    h = request.headers.get("Authorization", "")
    if h.startswith("Bearer "): t = h[7:]
    return hmac.compare_digest(t, token())


@app.get("/")
def index():
    r = send_from_directory("static", "index.html"); r.headers["Cache-Control"] = "no-cache"; return r


@app.get("/healthz")
def health():
    if not authed(): return "ok"
    return jsonify(ver=S["ver"], saved=S["saved"], dirty=S["dirty"], err=S["err"], beat=int(time.time()) - S["beat"], pid=os.getpid())


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
    if request.content_length and request.content_length > 8 * 1024 * 1024: return jsonify(error="grande"), 413
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


# ================= Revisión de anuncios (calendario de la carpeta de Irian) =================
# Cada pieza: check de listo, estado, comentarios y versiones nuevas subidas. El calendario es un HTML local
# (file://), así que estas rutas aceptan CORS y un token en la cabecera o en ?t=.
REV = {"data": None}
FILES = {}
ESTADO_MONDAY = {"pend": "pend", "rev": "rev", "cambios": "edit", "ok": "ready"}


def rev_load():
    if REV["data"] is not None: return
    if MEMORY: REV["data"] = {}; return
    with db() as c:
        c.execute("create table if not exists pieza_archivo (id serial primary key, pieza text, nombre text, tipo text, tam int, por text, datos bytea, subido timestamptz default now())")
        row = c.execute("select doc from tablero where id='revision'").fetchone()
    REV["data"] = row[0] if row else {}


def rev_save():
    if MEMORY: return
    with db() as c:
        c.execute("insert into tablero (id, ver, doc) values ('revision', 1, %s::jsonb) on conflict (id) do update set doc=excluded.doc, actualizado=now()",
                  (json.dumps(REV["data"], ensure_ascii=False),))


def rev_item(key):
    return REV["data"].setdefault(key, {"listo": False, "estado": "pend", "comentarios": [], "archivos": []})


def sync_monday(key, estado):
    """Pasa el estado de la pieza a su tarea en el tablero de anuncios del Monday."""
    try:
        sube, _, name = key.split("|", 2)
    except ValueError:
        return
    with lock:
        D = S["data"]
        if not D: return
        for b in D["boards"]:
            if b["id"] != "anuncios": continue
            for it in b["items"]:
                if it.get("name") == name and (it.get("sube") or it.get("due")) == sube:
                    new = ESTADO_MONDAY.get(estado, it["status"])
                    if it["status"] != new:
                        it["status"] = new; it["updatedAt"] = int(time.time() * 1000)
                        S["ver"] += 1; S["dirty"] = True
                    return


def cors(r):
    r.headers["Access-Control-Allow-Origin"] = "*"
    r.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    r.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
    return r


@app.after_request
def _cors(r):
    if request.path.startswith(("/api/rev", "/api/token", "/f/")): cors(r)
    return r


@app.route("/api/rev", methods=["OPTIONS"])
@app.route("/api/rev/archivo", methods=["OPTIONS"])
@app.route("/api/rev/archivo/<int:fid>", methods=["OPTIONS"])
@app.route("/api/token", methods=["OPTIONS"])
def _pre(fid=None): return "", 204


@app.post("/api/token")
def api_token():
    pw = (request.get_json(silent=True) or {}).get("pw", "")
    if not hmac.compare_digest(pw.encode(), APP_PASSWORD.encode()):
        time.sleep(1); return jsonify(error="no"), 401
    return jsonify(token=token())


@app.get("/api/rev")
def rev_get():
    if not authed(): return jsonify(error="auth"), 401
    with lock: rev_load(); return jsonify(items=REV["data"])


@app.post("/api/rev")
def rev_post():
    if not authed(): return jsonify(error="auth"), 401
    b = request.get_json(silent=True) or {}
    key = str(b.get("key", ""))[:400]
    if not key: return jsonify(error="key"), 400
    with lock:
        rev_load(); it = rev_item(key)
        if "listo" in b:
            it["listo"] = bool(b["listo"])
            it["estado"] = "ok" if it["listo"] else ("rev" if it["archivos"] else "pend")
        if b.get("estado") in ESTADO_MONDAY: it["estado"] = b["estado"]; it["listo"] = b["estado"] == "ok"
        c = b.get("comentario")
        if isinstance(c, dict) and str(c.get("text", "")).strip():
            it["comentarios"].append({"id": os.urandom(4).hex(), "by": str(c.get("by", ""))[:20], "text": str(c["text"])[:3000], "cambios": bool(b.get("cambios")), "at": int(time.time() * 1000)})
            if b.get("cambios"): it["estado"] = "cambios"; it["listo"] = False
        estado = it["estado"]; out = dict(it)
    rev_save(); sync_monday(key, estado)
    return jsonify(item=out)


@app.post("/api/rev/archivo")
def rev_upload():
    if not authed(): return jsonify(error="auth"), 401
    key = str(request.form.get("key", ""))[:400]; por = str(request.form.get("por", ""))[:20]
    f = request.files.get("file")
    if not key or not f: return jsonify(error="datos"), 400
    data = f.read()
    if len(data) > 100 * 1024 * 1024: return jsonify(error="grande"), 413
    tipo = f.mimetype or "application/octet-stream"
    if MEMORY:
        fid = len(FILES) + 1; FILES[fid] = (f.filename, tipo, data)
    else:
      with db() as c:
        fid = c.execute("insert into pieza_archivo (pieza, nombre, tipo, tam, por, datos) values (%s,%s,%s,%s,%s,%s) returning id",
                        (key, f.filename, tipo, len(data), por, data)).fetchone()[0]
    with lock:
        rev_load(); it = rev_item(key)
        it["archivos"].append({"id": fid, "nombre": f.filename, "tipo": tipo, "tam": len(data), "por": por, "at": int(time.time() * 1000)})
        it["estado"] = "rev"; it["listo"] = False; out = dict(it)
    rev_save(); sync_monday(key, "rev")
    return jsonify(item=out)


@app.delete("/api/rev/archivo/<int:fid>")
def rev_delete(fid):
    if not authed(): return jsonify(error="auth"), 401
    if MEMORY: FILES.pop(fid, None)
    else:
        with db() as c: c.execute("delete from pieza_archivo where id=%s", (fid,))
    with lock:
        rev_load()
        for it in REV["data"].values(): it["archivos"] = [a for a in it["archivos"] if a["id"] != fid]
    rev_save(); return jsonify(ok=True)


@app.get("/f/<int:fid>")
def archivo(fid):
    if not authed(): return "no", 401
    if MEMORY: row = FILES.get(fid)
    else:
        with db() as c:
            row = c.execute("select nombre, tipo, datos from pieza_archivo where id=%s", (fid,)).fetchone()
    if not row: return "no", 404
    data = bytes(row[2]); n = len(data)
    rng = request.headers.get("Range", "")
    if rng.startswith("bytes="):
        a, _, z = rng[6:].partition("-"); a = int(a or 0); z = int(z) if z else n - 1; z = min(z, n - 1)
        r = Response(data[a:z + 1], 206, mimetype=row[1]); r.headers["Content-Range"] = "bytes %d-%d/%d" % (a, z, n)
    else:
        r = Response(data, mimetype=row[1])
    r.headers["Accept-Ranges"] = "bytes"; r.headers["Cache-Control"] = "private, max-age=86400"
    r.headers["Content-Disposition"] = "inline; filename*=UTF-8''" + __import__("urllib.parse").parse.quote(row[0] or "archivo")
    return r
