"""Tablero de Mercadeo Grupo Rapid: servidor pequeño.
Guarda el tablero en un repositorio privado de GitHub (data/state.json) para que no se pierda cuando Render reinicia,
lo comparte entre Hilda e Irian con contraseña y publica un calendario .ics por persona para Google Calendar."""
import atexit, base64, datetime, hashlib, hmac, json, os, threading, time
import requests
from flask import Flask, Response, jsonify, request, send_from_directory

GH_TOKEN = os.environ["GH_TOKEN"]
DATA_REPO = os.environ["DATA_REPO"]            # p. ej. org/monday-mercadeo-datos
APP_PASSWORD = os.environ["APP_PASSWORD"]
SECRET = os.environ["SECRET"].encode()
CAL_KEY = os.environ["CAL_KEY"]
PATH = os.environ.get("DATA_PATH", "data/state.json")
GH = "https://api.github.com/repos/%s/contents/%s" % (DATA_REPO, PATH)
HDR = {"Authorization": "token " + GH_TOKEN, "Accept": "application/vnd.github+json"}

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024
lock = threading.Lock()
S = {"ver": 0, "data": None, "sha": None, "dirty": False}


def gh_load():
    r = requests.get(GH, headers=HDR, timeout=30); r.raise_for_status()
    sha = r.json()["sha"]
    raw = requests.get(GH, headers={**HDR, "Accept": "application/vnd.github.raw"}, timeout=60); raw.raise_for_status()
    doc = json.loads(raw.content.decode("utf-8"))
    S.update(ver=doc["ver"], data=doc["data"], sha=sha)


def gh_save():
    with lock:
        if not S["dirty"]: return
        body = json.dumps({"ver": S["ver"], "data": S["data"]}, ensure_ascii=False).encode("utf-8")
        S["dirty"] = False
    for _ in range(3):
        r = requests.put(GH, headers=HDR, timeout=60, json={"message": "Tablero v%d" % S["ver"], "sha": S["sha"],
                                                           "content": base64.b64encode(body).decode()})
        if r.ok:
            S["sha"] = r.json()["content"]["sha"]; return
        if r.status_code in (409, 422):         # sha viejo: se vuelve a leer y se reintenta
            S["sha"] = requests.get(GH, headers=HDR, timeout=30).json().get("sha"); continue
        break
    with lock: S["dirty"] = True


def writer():
    while True:
        time.sleep(3)
        try: gh_save()
        except Exception as e: print("guardar:", e, flush=True)


gh_load()
threading.Thread(target=writer, daemon=True).start()
atexit.register(gh_save)


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
    v = request.args.get("v")
    if v is not None and v == str(S["ver"]): return "", 204
    return jsonify(ver=S["ver"], data=S["data"], calKey=CAL_KEY)


@app.post("/api/state")
def post_state():
    if not authed(): return jsonify(error="auth"), 401
    b = request.get_json(silent=True) or {}
    if not isinstance(b.get("data"), dict) or "boards" not in b["data"]: return jsonify(error="datos"), 400
    with lock:
        if b.get("base") != S["ver"]: return jsonify(ver=S["ver"], data=S["data"]), 409
        S["ver"] += 1; S["data"] = b["data"]; S["dirty"] = True
        return jsonify(ver=S["ver"])


def esc(t): return str(t).replace("\\", "\\\\").replace(",", "\\,").replace(";", "\\;").replace("\n", "\\n")


@app.get("/cal/<pid>.ics")
def cal(pid):
    if not hmac.compare_digest(request.args.get("k", ""), CAL_KEY): return "no", 403
    D = S["data"]; names = {p["id"]: p["name"] for p in D.get("people", [])}
    if pid not in names: return "no", 404
    L = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Grupo Rapid//Mercadeo//ES", "CALSCALE:GREGORIAN", "METHOD:PUBLISH",
         "X-WR-CALNAME:Mercadeo · " + names[pid], "X-WR-TIMEZONE:America/Panama", "REFRESH-INTERVAL;VALUE=DURATION:PT1H", "X-PUBLISHED-TTL:PT1H"]
    stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    def ev(uid, title, a, z, desc):
        z2 = (datetime.date.fromisoformat(z) + datetime.timedelta(1)).strftime("%Y%m%d")
        L.extend(["BEGIN:VEVENT", "UID:%s@mercadeo-gruporapid" % uid, "DTSTAMP:" + stamp, "DTSTART;VALUE=DATE:" + a.replace("-", ""),
                  "DTEND;VALUE=DATE:" + z2, "SUMMARY:" + esc(title), "DESCRIPTION:" + esc(desc), "END:VEVENT"])
    for b in D["boards"]:
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
