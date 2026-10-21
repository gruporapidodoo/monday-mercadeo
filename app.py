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
        c.execute("create table if not exists pieza_archivo (id serial primary key, pieza text, nombre text, tipo text, tam int, por text, datos bytea, subido timestamptz default now())")
        c.execute("create table if not exists pieza_parte (fid int, n int, datos bytea, primary key (fid, n))")
        # subidas que quedaron a medias
        c.execute("delete from pieza_archivo a where a.datos is null and a.tam = 0 and not exists (select 1 from pieza_parte p where p.fid = a.id)")
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
ORIG = {}          # pieza -> id del archivo original (la versión vieja)
ORIG_FILES = {}    # solo en modo memoria
ESTADO_MONDAY = {"pend": "pend", "rev": "rev", "cambios": "edit", "ok": "ready"}


def rev_load():
    if REV["data"] is not None: return
    if MEMORY: REV["data"] = {}; return
    with db() as c:
        c.execute("create table if not exists pieza_archivo (id serial primary key, pieza text, nombre text, tipo text, tam int, por text, datos bytea, subido timestamptz default now())")
        c.execute("create table if not exists pieza_original (id serial primary key, pieza text unique, nombre text, tipo text, tam int, datos bytea)")
        c.execute("create table if not exists pieza_parte (fid int, n int, datos bytea, primary key (fid, n))")
        ORIG.clear(); ORIG.update({k: i for i, k in c.execute("select id, pieza from pieza_original").fetchall()})
        row = c.execute("select doc from tablero where id='revision'").fetchone()
    REV["data"] = row[0] if row else {}


def rev_save():
    if MEMORY: return
    with db() as c:
        c.execute("insert into tablero (id, ver, doc) values ('revision', 1, %s::jsonb) on conflict (id) do update set doc=excluded.doc, actualizado=now()",
                  (json.dumps(REV["data"], ensure_ascii=False),))


def rev_item(key):
    return REV["data"].setdefault(key, {"listo": False, "estado": "pend", "comentarios": [], "archivos": []})


def sync_posts(key):
    import re as _re
    grupo = key.split("|")[1] if key.count("|") >= 2 else ""
    m = _re.search(r"Post (\d+)", grupo)
    if not m: return
    n = int(m.group(1)); pref = key.split("|")[0] + "|" + grupo + "|"
    with lock:
        ests = [v["estado"] for k, v in (REV["data"] or {}).items() if k.startswith(pref)]
        if not ests: return
        new = "done" if all(e == "ok" for e in ests) else ("prog" if "cambios" in ests else ("rev" if any(e in ("rev", "ok") for e in ests) else "pend"))
        D = S["data"]
        if not D: return
        for b in D["boards"]:
            for it in b["items"]:
                if it.get("name", "").startswith("Agregar el isotipo de cada empresa"):
                    for su in it.get("subs") or []:
                        if su.get("name", "").split(" ·")[0].strip() == "Post %d" % n and su.get("status") != new:
                            su["status"] = new; S["ver"] += 1; S["dirty"] = True
                    allsubs = [su.get("status") for su in it.get("subs") or []]
                    if allsubs and all(x == "done" for x in allsubs) and it["status"] != "done":
                        it["status"] = "done"; S["ver"] += 1; S["dirty"] = True


def sync_monday(key, estado):
    """Pasa el estado de la pieza a su tarea en el tablero de anuncios del Monday."""
    if "|Posts @gruporapid" in key: return sync_posts(key)
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
    with lock: rev_load(); return jsonify(items={k: v for k, v in REV["data"].items() if not k.startswith("_")}, originales=ORIG)


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
    if estado == "ok": drive_notify(key, request.url_root)
    return jsonify(item=out)


@app.post("/api/rev/archivo")
def rev_upload():
    if not authed(): return jsonify(error="auth"), 401
    key = str(request.form.get("key", ""))[:400]; por = str(request.form.get("por", ""))[:20]
    f = request.files.get("file")
    if not key or not f: return jsonify(error="datos"), 400
    tipo = f.mimetype or "application/octet-stream"
    CH = 4 * 1024 * 1024
    if MEMORY:
        data = f.read(); fid = len(FILES) + 1; FILES[fid] = (f.filename, tipo, data); tam = len(data)
    else:
        # por partes de 4 MB: un solo insert grande corta la conexión de la base gratis
        with db() as c:
            fid = c.execute("insert into pieza_archivo (pieza, nombre, tipo, tam, por, datos) values (%s,%s,%s,0,%s,NULL) returning id",
                            (key, f.filename, tipo, por)).fetchone()[0]
        tam = 0; n = 0
        try:
            while True:
                chunk = f.stream.read(CH)
                if not chunk: break
                tam += len(chunk)
                if tam > 100 * 1024 * 1024: raise ValueError("grande")
                for intento in range(3):
                    try:
                        with db() as c: c.execute("insert into pieza_parte (fid, n, datos) values (%s,%s,%s) on conflict do nothing", (fid, n, chunk))
                        break
                    except psycopg.OperationalError:
                        if intento == 2: raise
                        time.sleep(1.5)
                n += 1
        except Exception as e:
            with db() as c:
                c.execute("delete from pieza_parte where fid=%s", (fid,)); c.execute("delete from pieza_archivo where id=%s", (fid,))
            if str(e) == "grande": return jsonify(error="grande"), 413
            raise
        with db() as c: c.execute("update pieza_archivo set tam=%s where id=%s", (tam, fid))
    data = None
    with lock:
        rev_load(); it = rev_item(key)
        it["archivos"].append({"id": fid, "nombre": f.filename, "tipo": tipo, "tam": tam, "por": por, "at": int(time.time() * 1000)})
        it["estado"] = "rev"; it["listo"] = False; out = dict(it)
    rev_save(); sync_monday(key, "rev")
    return jsonify(item=out)


@app.delete("/api/rev/archivo/<int:fid>")
def rev_delete(fid):
    if not authed(): return jsonify(error="auth"), 401
    if MEMORY: FILES.pop(fid, None)
    else:
        with db() as c: c.execute("delete from pieza_parte where fid=%s", (fid,)); c.execute("delete from pieza_archivo where id=%s", (fid,))
    with lock:
        rev_load()
        for it in REV["data"].values(): it["archivos"] = [a for a in it["archivos"] if a["id"] != fid]
    rev_save(); return jsonify(ok=True)


@app.get("/f/<int:fid>")
def archivo(fid):
    if not authed(): return "no", 401
    return archivo_resp(fid)


@app.post("/api/rev/original")
def rev_original():
    """Carga (o reemplaza) el archivo original de una pieza: la versión vieja que hay que actualizar."""
    if not authed(): return jsonify(error="auth"), 401
    key = str(request.form.get("key", ""))[:400]; f = request.files.get("file")
    if not key or not f: return jsonify(error="datos"), 400
    data = f.read(); tipo = f.mimetype or "application/octet-stream"
    with lock: rev_load()
    if MEMORY:
        fid = ORIG.get(key) or len(ORIG_FILES) + 1; ORIG_FILES[fid] = (f.filename, tipo, data)
    else:
        with db() as c:
            fid = c.execute("insert into pieza_original (pieza, nombre, tipo, tam, datos) values (%s,%s,%s,%s,%s) on conflict (pieza) do update set nombre=excluded.nombre, tipo=excluded.tipo, tam=excluded.tam, datos=excluded.datos returning id",
                            (key, f.filename, tipo, len(data), data)).fetchone()[0]
    ORIG[key] = fid
    return jsonify(id=fid)


def stream_partes(fid, nombre, tipo, tam):
    CH = 4 * 1024 * 1024; rng = request.headers.get("Range", "")
    a, z, code = 0, tam - 1, 200
    if rng.startswith("bytes="):
        x, _, y = rng[6:].partition("-"); a = int(x or 0); z = min(int(y) if y else tam - 1, tam - 1); code = 206
        z = min(z, a + 8 * 1024 * 1024 - 1)       # máx. 8 MB por pedido
    n0, n1 = a // CH, z // CH
    with db() as c:
        parts = c.execute("select n, datos from pieza_parte where fid=%s and n between %s and %s order by n", (fid, n0, n1)).fetchall()
    buf = b"".join(bytes(p[1]) for p in parts); off = n0 * CH
    body = buf[a - off: z - off + 1]
    r = Response(body, code, mimetype=tipo)
    if code == 206: r.headers["Content-Range"] = "bytes %d-%d/%d" % (a, a + len(body) - 1, tam)
    r.headers["Accept-Ranges"] = "bytes"; r.headers["Cache-Control"] = "private, max-age=86400"
    if request.args.get("dl"): r.headers["Content-Disposition"] = "attachment; filename*=UTF-8''" + __import__("urllib.parse").parse.quote(nombre or "archivo")
    return r


def archivo_resp(fid):
    if MEMORY:
        row = FILES.get(fid); return stream(row) if row else ("no", 404)
    with db() as c:
        row = c.execute("select nombre, tipo, tam, datos is not null from pieza_archivo where id=%s", (fid,)).fetchone()
        if not row: return "no", 404
        if row[3]:
            full = c.execute("select nombre, tipo, datos from pieza_archivo where id=%s", (fid,)).fetchone(); return stream(full)
    return stream_partes(fid, row[0], row[1], row[2])


def archivo_resp_completo(fid):
    """Para Drive: el archivo entero (el Apps Script no pide por rangos)."""
    if MEMORY:
        row = FILES.get(fid); return stream(row) if row else ("no", 404)
    with db() as c:
        row = c.execute("select nombre, tipo, tam, datos from pieza_archivo where id=%s", (fid,)).fetchone()
        if not row: return "no", 404
        if row[3] is not None: return Response(bytes(row[3]), mimetype=row[1])
        parts = c.execute("select datos from pieza_parte where fid=%s order by n", (fid,)).fetchall()
    return Response(b"".join(bytes(p[0]) for p in parts), mimetype=row[1])


def stream(row):
    data = bytes(row[2]); n = len(data); rng = request.headers.get("Range", "")
    if rng.startswith("bytes="):
        a, _, z = rng[6:].partition("-"); a = int(a or 0); z = int(z) if z else n - 1; z = min(z, n - 1)
        r = Response(data[a:z + 1], 206, mimetype=row[1]); r.headers["Content-Range"] = "bytes %d-%d/%d" % (a, z, n)
    else:
        r = Response(data, mimetype=row[1])
    r.headers["Accept-Ranges"] = "bytes"; r.headers["Cache-Control"] = "private, max-age=86400"
    if request.args.get("dl"): r.headers["Content-Disposition"] = "attachment; filename*=UTF-8''" + __import__("urllib.parse").parse.quote(row[0] or "archivo")
    return r


@app.get("/f/o/<int:fid>")
def original(fid):
    if not authed(): return "no", 401
    if MEMORY: row = ORIG_FILES.get(fid)
    else:
        with db() as c: row = c.execute("select nombre, tipo, datos from pieza_original where id=%s", (fid,)).fetchone()
    if not row: return "no", 404
    return stream(row)


LOGIN_PAGE = """<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Anuncios a actualizar</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f6f5f3;font:15px/1.5 "Segoe UI",system-ui,sans-serif;color:#1b1d22;padding:16px}
form{background:#fff;border:1px solid #e2dfda;border-radius:14px;padding:28px;display:grid;gap:12px;width:min(360px,100%)}h1{margin:0;font-size:20px}p{margin:0;color:#646a75;font-size:13px}
input{border:1px solid #e2dfda;border-radius:8px;padding:10px;font:inherit}button{background:#e01f26;color:#fff;border:0;border-radius:8px;padding:10px;font:inherit;font-weight:700;cursor:pointer}.e{color:#e01f26;font-size:13px;min-height:1em}</style></head>
<body><form id="f"><h1>Anuncios a actualizar · Grupo Rapid</h1><p>Revisión de las piezas de pauta. Escriba la contraseña del equipo de Mercadeo.</p><input type="password" id="p" placeholder="Contraseña" autofocus><button>Entrar</button><div class="e" id="e"></div></form>
<script>document.getElementById("f").onsubmit=function(ev){ev.preventDefault();fetch("/api/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({pw:document.getElementById("p").value}),credentials:"same-origin"}).then(function(r){if(r.ok)location.reload();else document.getElementById("e").textContent="Contraseña incorrecta"})}</script></body></html>"""


PAGINA = {"html": None}


@app.post("/api/pagina")
def subir_pagina():
    """La página de revisión trae nombres de anuncios: no va en el repositorio público, se guarda en la base."""
    if not authed(): return jsonify(error="auth"), 401
    html = request.get_data(as_text=True)
    if "<html" not in html[:2000]: return jsonify(error="html"), 400
    PAGINA["html"] = html
    if not MEMORY:
        with db() as c:
            c.execute("insert into tablero (id, ver, doc) values ('pagina_anuncios', 1, %s::jsonb) on conflict (id) do update set doc=excluded.doc, actualizado=now()",
                      (json.dumps({"html": html}),))
    return jsonify(ok=True, kb=len(html) // 1024)


@app.get("/anuncios")
def anuncios():
    if not authed(): return Response(LOGIN_PAGE, mimetype="text/html")
    if PAGINA["html"] is None and not MEMORY:
        with db() as c:
            row = c.execute("select doc from tablero where id='pagina_anuncios'").fetchone()
        PAGINA["html"] = row[0]["html"] if row else ""
    r = Response(PAGINA["html"] or "<p>La página todavía no está cargada.</p>", mimetype="text/html"); r.headers["Cache-Control"] = "no-cache"; return r


# ================= Drive: al aprobar una pieza, su versión nueva se guarda en Google Drive =================
# Un Apps Script en la cuenta de Google de Mercadeo recibe el aviso (web app) y baja el archivo a
# "Anuncios aprobados - Grupo Rapid / <día de entrega> / <anuncio>". El servidor no guarda credenciales de Google.
import datetime as _dt
DOWS = ["lun", "mar", "mié", "jue", "vie", "sáb", "dom"]


def drive_key(): return hmac.new(SECRET, b"drive", hashlib.sha256).hexdigest()[:32]
def drive_ok(): return hmac.compare_digest(request.args.get("k", ""), drive_key())


def pieza_info(key):
    sube, grupo, name = (key.split("|", 2) + ["", "", ""])[:3]
    if grupo.startswith("Posts @gruporapid"):
        post = grupo.split(" · ", 1)[1] if " · " in grupo else grupo
        return {"dia": "2026-10-21 · Posts @gruporapid con isotipo", "anuncio": post, "grupo": grupo, "nombre_pieza": name}
    try:
        d = _dt.date.fromisoformat(sube); e = d - _dt.timedelta(2)
        dia = "%s · Entrega %s %d oct · sube %s %d oct" % (e.isoformat(), DOWS[e.weekday()], e.day, DOWS[d.weekday()], d.day)
    except ValueError:
        dia = sube or "Sin fecha"
    return {"dia": dia, "anuncio": (name + " — " + grupo)[:180], "grupo": grupo, "nombre_pieza": name}


def drive_payload(key, a, base):
    if "localhost" not in base and "127.0.0.1" not in base: base = base.replace("http://", "https://")
    info = pieza_info(key)
    return {"k": drive_key(), "id": a["id"], "key": key, "nombre": a["nombre"], "tipo": a.get("tipo", ""),
            "url": base.rstrip("/") + "/api/drive/archivo/%d?k=%s" % (a["id"], drive_key()), **info}


def drive_notify(key, base):
    """Se llama al aprobar: manda al Apps Script la última versión de la pieza si todavía no está en Drive."""
    hook = (REV["data"] or {}).get("_drive", {}).get("hook")
    it = (REV["data"] or {}).get(key)
    if not hook or not it or not it.get("listo") or not it.get("archivos"): return
    a = it["archivos"][-1]
    if a.get("drive"): return
    payload = drive_payload(key, a, base)
    def run():
        import urllib.request
        try:
            req = urllib.request.Request(hook, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
            res = json.loads(urllib.request.urlopen(req, timeout=300).read().decode() or "{}")
            if res.get("url"):
                with lock:
                    for x in REV["data"].get(key, {}).get("archivos", []):
                        if x["id"] == a["id"]: x["drive"] = res["url"]
                rev_save()
            else:
                print("drive:", res, flush=True)
        except Exception as e:
            print("drive:", e, flush=True)
    threading.Thread(target=run, daemon=True).start()


@app.get("/api/drive/clave")
def drive_clave():
    if not authed(): return jsonify(error="auth"), 401
    with lock: rev_load(); hook = REV["data"].get("_drive", {}).get("hook", "")
    return jsonify(clave=drive_key(), servidor=request.url_root.rstrip("/"), hook=hook)


@app.post("/api/drive/config")
def drive_config():
    if not authed(): return jsonify(error="auth"), 401
    hook = str((request.get_json(silent=True) or {}).get("hook", "")).strip()
    if hook and not hook.startswith("https://script.google.com/") and not os.environ.get("TEST_ALLOW_HOOK"): return jsonify(error="url"), 400
    with lock: rev_load(); REV["data"]["_drive"] = {"hook": hook}
    rev_save(); return jsonify(ok=True)


@app.get("/api/drive/archivo/<int:fid>")
def drive_archivo(fid):
    if not drive_ok(): return "no", 403
    if request.args.get("todo"): request.environ.pop("HTTP_RANGE", None)
    return archivo_resp_completo(fid)


@app.get("/api/drive/pendientes")
def drive_pendientes():
    """Aprobadas cuya última versión todavía no está en Drive (el Apps Script las recoge si un aviso falló)."""
    if not drive_ok(): return jsonify(error="no"), 403
    base = request.url_root
    with lock:
        rev_load()
        out = [drive_payload(k, it["archivos"][-1], base) for k, it in REV["data"].items()
               if not k.startswith("_") and it.get("listo") and it.get("archivos") and not it["archivos"][-1].get("drive")]
    return jsonify(pendientes=out)


@app.post("/api/drive/listo")
def drive_listo():
    if not drive_ok(): return jsonify(error="no"), 403
    b = request.get_json(silent=True) or {}
    with lock:
        rev_load()
        for k, it in REV["data"].items():
            if k.startswith("_"): continue
            for a in it.get("archivos", []):
                if a["id"] == b.get("id"): a["drive"] = str(b.get("url", ""))[:500]
    rev_save(); return jsonify(ok=True)


# ================= Drive interno: carpetas por día › anuncio › Vieja / Nueva =================
@app.get("/drive")
def drive_page():
    if not authed(): return Response(LOGIN_PAGE.replace("Anuncios a actualizar · Grupo Rapid", "Drive de Mercadeo · Grupo Rapid"), mimetype="text/html")
    r = send_from_directory("static", "drive.html"); r.headers["Cache-Control"] = "no-cache"; return r


@app.get("/api/drive/arbol")
def drive_arbol():
    if not authed(): return jsonify(error="auth"), 401
    with lock: rev_load()
    if MEMORY:
        orig = {k: {"id": i, "nombre": ORIG_FILES[i][0], "tipo": ORIG_FILES[i][1], "tam": len(ORIG_FILES[i][2])} for k, i in ORIG.items() if i in ORIG_FILES}
    else:
        with db() as c:
            orig = {r[1]: {"id": r[0], "nombre": r[2], "tipo": r[3], "tam": r[4]} for r in c.execute("select id, pieza, nombre, tipo, tam from pieza_original").fetchall()}
    with lock:
        keys = set(orig) | {k for k in REV["data"] if not k.startswith("_")}
        out = []
        for k in sorted(keys):
            it = REV["data"].get(k, {}); info = pieza_info(k)
            out.append({"key": k, "dia": info["dia"], "grupo": info["grupo"], "nombre": info["nombre_pieza"], "estado": it.get("estado", "pend"),
                        "vieja": orig.get(k), "nuevas": it.get("archivos", [])})
    return jsonify(items=out)


# ================= Documentos (planes, presentaciones) dentro del Drive interno =================
def guardar_por_partes(pieza, nombre, tipo, por, stream_):
    CH = 4 * 1024 * 1024
    if MEMORY:
        data = stream_.read(); fid = len(FILES) + 1; FILES[fid] = (nombre, tipo, data); return fid, len(data)
    with db() as c:
        fid = c.execute("insert into pieza_archivo (pieza, nombre, tipo, tam, por, datos) values (%s,%s,%s,0,%s,NULL) returning id", (pieza, nombre, tipo, por)).fetchone()[0]
    tam = 0; n = 0
    try:
        while True:
            chunk = stream_.read(CH)
            if not chunk: break
            tam += len(chunk)
            for intento in range(3):
                try:
                    with db() as c: c.execute("insert into pieza_parte (fid, n, datos) values (%s,%s,%s) on conflict do nothing", (fid, n, chunk))
                    break
                except psycopg.OperationalError:
                    if intento == 2: raise
                    time.sleep(1.5)
            n += 1
    except Exception:
        with db() as c:
            c.execute("delete from pieza_parte where fid=%s", (fid,)); c.execute("delete from pieza_archivo where id=%s", (fid,))
        raise
    with db() as c: c.execute("update pieza_archivo set tam=%s where id=%s", (tam, fid))
    return fid, tam


@app.post("/api/doc")
def subir_doc():
    """Sube un documento a una carpeta del Drive interno; reemplaza el que tenga el mismo nombre en esa carpeta."""
    if not authed(): return jsonify(error="auth"), 401
    carpeta = str(request.form.get("carpeta", "Planes y documentos"))[:120]; f = request.files.get("file")
    if not f: return jsonify(error="datos"), 400
    pieza = "_doc|" + carpeta
    if not MEMORY:
        with db() as c:
            for (old,) in c.execute("select id from pieza_archivo where pieza=%s and nombre=%s", (pieza, f.filename)).fetchall():
                c.execute("delete from pieza_parte where fid=%s", (old,)); c.execute("delete from pieza_archivo where id=%s", (old,))
    fid, tam = guardar_por_partes(pieza, f.filename, f.mimetype or "application/octet-stream", "mercadeo", f.stream)
    return jsonify(id=fid, tam=tam)


@app.get("/api/docs")
def listar_docs():
    if not authed(): return jsonify(error="auth"), 401
    if MEMORY: return jsonify(docs=[])
    with db() as c:
        rows = c.execute("select id, pieza, nombre, tipo, tam, subido from pieza_archivo where left(pieza, 5) = '_doc|' order by pieza, nombre").fetchall()
    return jsonify(docs=[{"id": r[0], "carpeta": r[1][5:], "nombre": r[2], "tipo": r[3], "tam": r[4], "at": int(r[5].timestamp() * 1000) if r[5] else 0} for r in rows])
