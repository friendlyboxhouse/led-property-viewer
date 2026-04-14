import sys
import io
import os
import requests
from bs4 import BeautifulSoup
import urllib.parse
import csv
from datetime import datetime
import re
import time
import random
import signal

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

BASE_URL  = "https://asset.led.go.th"
LIST_URL  = f"{BASE_URL}/newbidreg/default.asp"
IMAGE_URL = f"{BASE_URL}/PPKPicture"

# ==============================================================
# ⚠️  RATE LIMITING CONFIG — แก้ค่าเหล่านี้ถ้าจำเป็น
#      ค่า default ออกแบบมาเพื่อให้ server ไม่ถูกกด
# ==============================================================
RATE = {
    # ดีเลย์ระหว่างแต่ละ request (วินาที) — สุ่มในช่วงนี้
    'page_delay_min':    2.0,
    'page_delay_max':    4.0,

    # ดีเลย์ระหว่างจังหวัด (วินาที) เมื่อ scrape หลายจังหวัด
    # ถ้าใช้ VPN ลด delay ลงได้เล็กน้อย (ยังคง polite ต่อ server)
    'province_delay_min': 15.0,
    'province_delay_max': 25.0,

    # ถ้า server ตอบช้าเกินนี้ (วินาที) → พักเพิ่ม
    'slow_response_threshold': 8.0,
    'slow_response_extra_wait': 10.0,

    # Retry + Exponential backoff
    'max_retries':     3,
    'backoff_base':    5.0,   # วินาที  retry1=5s, retry2=10s, retry3=20s

    # Circuit breaker — หยุดทันทีถ้า error ติดต่อกันเกินนี้
    'circuit_breaker_threshold': 4,

    # จำกัดจำนวน request สูงสุดต่อ session (ป้องกันกด loop ผิด)
    'max_requests_per_session': 6000,
}

# ==============================================================
# Global state สำหรับ rate limiter
# ==============================================================
_rate_state = {
    'total_requests':    0,        # request ทั้งหมดใน session นี้
    'consecutive_errors': 0,       # error ติดต่อกัน
    'last_request_time': 0.0,      # timestamp ของ request ล่าสุด
    'aborted':           False,    # ถูก Ctrl+C หรือ circuit breaker
}


def _handle_sigint(sig, frame):
    """จับ Ctrl+C → บันทึก partial data ก่อน exit"""
    print("\n\n[!] ได้รับสัญญาณหยุด (Ctrl+C) — กำลังหยุดอย่างปลอดภัย...")
    _rate_state['aborted'] = True


signal.signal(signal.SIGINT, _handle_sigint)

# ——— โฟลเดอร์เก็บข้อมูล ———
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ——— รายชื่อ 77 จังหวัด จัดตามภาค ———
PROVINCES_BY_REGION = {
    "ภาคเหนือ": [
        "เชียงใหม่","เชียงราย","น่าน","พะเยา","แพร่","แม่ฮ่องสอน",
        "ลำปาง","ลำพูน","อุตรดิตถ์",
    ],
    "ภาคเหนือตอนล่าง": [
        "กำแพงเพชร","ตาก","นครสวรรค์","พิจิตร","พิษณุโลก",
        "เพชรบูรณ์","สุโขทัย","อุทัยธานี",
    ],
    "ภาคกลาง": [
        "กรุงเทพมหานคร","กาญจนบุรี","ชัยนาท","นครนายก","นครปฐม",
        "นนทบุรี","ปทุมธานี","พระนครศรีอยุธยา","ลพบุรี",
        "สมุทรปราการ","สมุทรสงคราม","สมุทรสาคร","สระบุรี",
        "สิงห์บุรี","สุพรรณบุรี","อ่างทอง",
    ],
    "ภาคตะวันตก": [
        "ประจวบคีรีขันธ์","เพชรบุรี","ราชบุรี",
    ],
    "ภาคตะวันออก": [
        "จันทบุรี","ฉะเชิงเทรา","ชลบุรี","ตราด",
        "ปราจีนบุรี","ระยอง","สระแก้ว",
    ],
    "ภาคอีสาน": [
        "กาฬสินธุ์","ขอนแก่น","ชัยภูมิ","นครพนม","นครราชสีมา",
        "บึงกาฬ","บุรีรัมย์","มหาสารคาม","มุกดาหาร","ยโสธร",
        "ร้อยเอ็ด","เลย","ศรีสะเกษ","สกลนคร","สุรินทร์",
        "หนองคาย","หนองบัวลำภู","อำนาจเจริญ","อุดรธานี","อุบลราชธานี",
    ],
    "ภาคใต้": [
        "กระบี่","ชุมพร","ตรัง","นครศรีธรรมราช","นราธิวาส",
        "ปัตตานี","พัทลุง","พังงา","ภูเก็ต","ยะลา",
        "ระนอง","สงขลา","สตูล","สุราษฎร์ธานี",
    ],
}
ALL_PROVINCES = sorted({p for ps in PROVINCES_BY_REGION.values() for p in ps})

# ==============================================================
# 🔒  PRIVACY CONFIG
#
#  ระดับการซ่อนตัวตน (เลือกได้ 1 ระดับ):
#
#  ระดับ 1 — Header Rotation (built-in, เปิดอยู่เสมอ)
#    • สุ่ม User-Agent, Accept-Language, viewport
#    • ทำให้ไม่ดูเหมือน bot แต่ IP ยังเป็น IP ของคุณ
#
#  ระดับ 2 — HTTP/HTTPS Proxy  (ซ่อน IP ได้)
#    • ตั้งค่า PROXY_URL = "http://user:pass@host:port"
#    • IP ที่ server เห็นจะเป็น IP ของ proxy
#    • ใช้ได้กับ: proxy-cheap.com, webshare.io, brightdata.com
#
#  ระดับ 3 — SOCKS5 Proxy  (ซ่อน IP ได้ + เข้ารหัส)
#    • ตั้งค่า PROXY_URL = "socks5://user:pass@host:port"
#    • ต้องติดตั้งเพิ่ม: pip install requests[socks]
#    • ใช้ได้กับ: Mullvad, Proton VPN SOCKS5 port
#
#  ระดับ 4 — VPN (แนะนำสุด, ซ่อนได้สมบูรณ์)
#    • เปิด VPN ก่อนรัน scraper แล้วตั้ง PROXY_URL = None
#    • VPN แนะนำ: Mullvad (ไม่เก็บ log), ProtonVPN, NordVPN
#    • traffic ทั้งหมดออกจาก VPN server โดยอัตโนมัติ
#
#  ⚠️  สิ่งที่ทำไม่ได้ด้วยโค้ดเพียงอย่างเดียว:
#    • ซ่อน DNS leak → ต้องใช้ VPN ที่มี DNS leak protection
#    • ซ่อน WebRTC leak → ไม่เกี่ยวกับ Python (เฉพาะ browser)
#    • ซ่อนตัวตนจาก ISP → ต้องใช้ VPN/Tor
# ==============================================================
PRIVACY = {
    # --- Proxy (None = ไม่ใช้ proxy) ---
    # ถ้าใช้ VPN บนเครื่อง ไม่ต้องตั้งค่านี้
    # ตัวอย่าง: "http://user:pass@proxy.example.com:8080"
    # ตัวอย่าง: "socks5://user:pass@proxy.example.com:1080"
    'proxy_url': None,

    # --- Session rotation ---
    'rotate_session_per_province': True,

    # --- User-Agent rotation ---
    'rotate_user_agent': True,

    # --- ตรวจสอบ IP ก่อนเริ่ม scrape ---
    'show_ip_on_start': True,

    # --- VPN detection ---
    # บันทึก "home IP" ครั้งแรก แล้วเปรียบเทียบทุกครั้งที่รัน
    # ถ้า IP เปลี่ยน = VPN เปิดอยู่  |  ถ้าเหมือนเดิม = เตือน
    'detect_vpn': True,
}

# ==============================================================
# 🎭  USER-AGENT POOL
#  Browser UA จริงๆ หลาย OS / version เพื่อหมุนเวียน
# ==============================================================
UA_POOL = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Chrome macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    # Firefox macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.1; rv:120.0) Gecko/20100101 Firefox/120.0",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    # Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

# Accept-Language pool เพื่อให้ดูหลากหลาย
LANG_POOL = [
    "th,en-US;q=0.9,en;q=0.8",
    "th-TH,th;q=0.9,en;q=0.8",
    "th,en;q=0.9",
    "th-TH,th;q=0.8,en-US;q=0.7,en;q=0.6",
]


def _random_headers() -> dict:
    """สร้าง HTTP headers ที่สุ่ม เพื่อลด fingerprint"""
    ua   = random.choice(UA_POOL)   if PRIVACY['rotate_user_agent'] else UA_POOL[0]
    lang = random.choice(LANG_POOL)

    # กำหนด sec-ch-ua ให้สอดคล้องกับ UA (Chrome เท่านั้น)
    is_chrome = 'Chrome/' in ua and 'Firefox' not in ua
    version   = ''
    if is_chrome:
        m = re.search(r'Chrome/(\d+)', ua)
        if m:
            version = m.group(1)

    h = {
        "User-Agent":              ua,
        "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language":         lang,
        "Accept-Encoding":         "gzip, deflate, br",
        "Connection":              "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control":           random.choice(["max-age=0", "no-cache"]),
    }
    if is_chrome and version:
        h["sec-ch-ua"]          = f'"Not_A Brand";v="8", "Chromium";v="{version}", "Google Chrome";v="{version}"'
        h["sec-ch-ua-mobile"]   = "?0"
        h["sec-ch-ua-platform"] = random.choice(['"Windows"', '"macOS"'])
        h["Sec-Fetch-Dest"]     = "document"
        h["Sec-Fetch-Mode"]     = "navigate"
        h["Sec-Fetch-Site"]     = random.choice(["none", "same-origin"])
        h["Sec-Fetch-User"]     = "?1"
    return h


def _build_proxies() -> dict | None:
    """สร้าง proxies dict สำหรับ requests"""
    url = PRIVACY.get('proxy_url')
    if not url:
        return None
    return {"http": url, "https": url}


_IP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.home_ip')


def _check_and_show_ip():
    """
    แสดง IP ปัจจุบันก่อนเริ่ม scrape และตรวจสอบว่า VPN เปิดอยู่หรือไม่

    ระบบ VPN Detection:
      - รันครั้งแรก → บันทึก IP เป็น "home IP" (.home_ip)
      - รันครั้งถัดไป → เปรียบเทียบ:
          IP ต่างกัน = ✅ VPN เปิดอยู่ (IP ถูก mask)
          IP เหมือนกัน = ⚠️ VPN อาจยังไม่เปิด
      - ถ้าต้องการ reset home IP: ลบไฟล์ .home_ip
    """
    if not PRIVACY.get('show_ip_on_start'):
        return
    try:
        proxies  = _build_proxies()
        r        = requests.get("https://api4.my-ip.io/ip.json",
                                timeout=10, proxies=proxies)
        data     = r.json()
        ip       = data.get('ip', 'ไม่ทราบ')
        isp      = data.get('isp', '')

        print(f"\n{'='*54}")
        print(f"  🌐  IP ที่ server จะเห็น : {ip}")
        if isp:
            print(f"  🏢  ISP / VPN Provider  : {isp}")

        # ── ตรวจ Proxy config ──
        if proxies:
            print(f"  ✅  ใช้ Proxy config — IP ถูก mask")

        # ── ตรวจ VPN ด้วย home IP comparison ──
        elif PRIVACY.get('detect_vpn'):
            if os.path.exists(_IP_FILE):
                with open(_IP_FILE, 'r', encoding='utf-8') as f:
                    home_ip = f.read().strip()
                if ip != home_ip:
                    print(f"  ✅  VPN ตรวจพบ! IP เปลี่ยน: {home_ip} → {ip}")
                    print(f"  🔒  IP จริงของคุณถูกซ่อนอยู่")
                else:
                    print(f"  ⚠️   IP เหมือน home IP ({home_ip})")
                    print(f"  💡   VPN อาจยังไม่เปิด หรือ IP ยังไม่เปลี่ยน")
            else:
                # บันทึก home IP ครั้งแรก
                with open(_IP_FILE, 'w', encoding='utf-8') as f:
                    f.write(ip)
                print(f"  📝  บันทึก home IP ครั้งแรก: {ip}")
                print(f"  💡  รันครั้งหน้าพร้อม VPN เพื่อตรวจสอบการเปลี่ยน IP")
        else:
            print(f"  ℹ️   (ปิด VPN detection — PRIVACY['detect_vpn'] = False)")

        print(f"{'='*54}\n")

    except Exception as e:
        # fallback ถ้า my-ip.io ล้มเหลว → ลอง ipify
        try:
            r2 = requests.get("https://api.ipify.org?format=json", timeout=8)
            ip = r2.json().get('ip', '?')
            print(f"\n  🌐  IP: {ip}  (ตรวจ ISP ไม่ได้: {e})\n")
        except Exception:
            print(f"  [ตรวจ IP ไม่ได้: {e}]")

# สถานะการขายแต่ละนัด
ISSALE_MAP = {
    '0': '-',
    '1': 'ขายได้',
    '2': 'งดขาย',
    '3': 'งดขายไม่มีผู้สู้ราคา',
    '4': 'เลื่อนนัด',
    '5': 'ถอนการขาย',
}


def ts():
    return datetime.now().strftime('%H:%M:%S')


# ==============================================================
# Rate-limited request wrappers
# ==============================================================

def _polite_wait(label: str = ""):
    """พักระหว่าง request แบบสุ่มเพื่อไม่กด server"""
    wait = random.uniform(RATE['page_delay_min'], RATE['page_delay_max'])
    print(f"  [⏱] รอ {wait:.1f}s ก่อน request {label}".rstrip())
    time.sleep(wait)


def _do_request(session, method: str, url: str, **kwargs):
    """
    ส่ง request พร้อม:
    - ตรวจ circuit breaker / session limit
    - วัด response time
    - Retry + Exponential backoff เมื่อ error
    - ตรวจสอบ HTTP status code ที่ควรหยุด
    - Proxy + Header rotation เพื่อความเป็นส่วนตัว
    """
    if _rate_state['aborted']:
        raise RuntimeError("ABORTED")

    # ตรวจ session limit
    if _rate_state['total_requests'] >= RATE['max_requests_per_session']:
        raise RuntimeError(
            f"[SAFETY] ถึง limit {RATE['max_requests_per_session']} requests/session แล้ว — หยุด"
        )

    # ตรวจ circuit breaker
    if _rate_state['consecutive_errors'] >= RATE['circuit_breaker_threshold']:
        raise RuntimeError(
            f"[CIRCUIT BREAKER] error ติดต่อกัน {_rate_state['consecutive_errors']} ครั้ง — หยุดป้องกัน server"
        )

    # ใส่ random headers ทุก request (ถ้า caller ไม่ได้กำหนด headers เอง)
    if 'headers' not in kwargs:
        kwargs['headers'] = _random_headers()

    # ใส่ proxy ถ้ากำหนดไว้
    proxies = _build_proxies()
    if proxies and 'proxies' not in kwargs:
        kwargs['proxies'] = proxies

    last_err = None
    for attempt in range(1, RATE['max_retries'] + 2):  # +2 = attempt แรก + retries
        try:
            t0 = time.time()
            _rate_state['total_requests'] += 1
            _rate_state['last_request_time'] = t0

            fn = session.get if method == 'GET' else session.post
            resp = fn(url, **kwargs)
            elapsed = time.time() - t0

            # ตรวจ status code อันตราย
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
                print(f"  [⚠️ 429] Server บอกให้รอ {retry_after}s — กำลังรอ...")
                time.sleep(retry_after + random.uniform(5, 15))
                _rate_state['consecutive_errors'] += 1
                continue

            if resp.status_code in (503, 502, 504):
                wait = RATE['backoff_base'] * (2 ** (attempt - 1))
                print(f"  [⚠️ {resp.status_code}] Server ไม่ตอบสนอง — รอ {wait:.0f}s (attempt {attempt})")
                time.sleep(wait + random.uniform(0, 5))
                _rate_state['consecutive_errors'] += 1
                continue

            if resp.status_code >= 400:
                print(f"  [⚠️ HTTP {resp.status_code}] {url[:60]}")
                _rate_state['consecutive_errors'] += 1
                return resp  # คืนค่าให้ caller จัดการ

            # สำเร็จ — reset error counter
            _rate_state['consecutive_errors'] = 0

            # ถ้า server ตอบช้า → pause เพิ่ม
            if elapsed > RATE['slow_response_threshold']:
                extra = RATE['slow_response_extra_wait']
                print(f"  [🐌] Server ตอบช้า ({elapsed:.1f}s) — พักเพิ่ม {extra}s")
                time.sleep(extra)

            print(f"  [✓] {method} {url[:60]}... ({elapsed:.1f}s | req#{_rate_state['total_requests']})")
            return resp

        except requests.exceptions.Timeout:
            wait = RATE['backoff_base'] * (2 ** (attempt - 1))
            print(f"  [⚠️ Timeout] attempt {attempt}/{RATE['max_retries']+1} — รอ {wait:.0f}s")
            _rate_state['consecutive_errors'] += 1
            last_err = "Timeout"
            time.sleep(wait)

        except requests.exceptions.ConnectionError as e:
            wait = RATE['backoff_base'] * (2 ** (attempt - 1))
            print(f"  [⚠️ ConnectionError] {e} — รอ {wait:.0f}s")
            _rate_state['consecutive_errors'] += 1
            last_err = str(e)
            time.sleep(wait)

        if _rate_state['aborted']:
            raise RuntimeError("ABORTED")

    raise RuntimeError(f"Request ล้มเหลวหลัง {RATE['max_retries']+1} attempts: {last_err}")


def safe_get(session, url, **kwargs):
    _polite_wait(f"GET {url[:50]}")
    return _do_request(session, 'GET', url, **kwargs)


def safe_post(session, url, **kwargs):
    _polite_wait(f"POST {url[:50]}")
    return _do_request(session, 'POST', url, **kwargs)


def format_thaidate(d):
    """แปลง '25690127' → '27/01/2569'"""
    d = str(d).strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[6:8]}/{d[4:6]}/{d[:4]}"
    return d


def picture_path_to_url(path):
    """แปลง 'Z:\\2568\\10-2568\\24\\39481p.jpg' → URL รูปภาพ"""
    if not path or not path.strip():
        return ""
    path = path.strip()
    # ตัด drive letter (Z:\) ออก
    path = re.sub(r'^[A-Za-z]:\\?', '', path)
    # แทน \ ด้วย /
    path = path.replace('\\', '/')
    # ตัด / นำหน้าถ้ามี
    path = path.lstrip('/')
    return f"{IMAGE_URL}/{path}"


def extract_form_data(form):
    """ดึงข้อมูลทั้งหมดจาก hidden inputs ของ form ในแถว"""
    inputs = {}
    for inp in form.find_all('input'):
        name  = inp.get('name', '').strip()
        value = inp.get('value', '').strip()
        if name:
            inputs[name] = value
    return inputs


def parse_property(inputs):
    """แปลง dict ของ inputs ให้เป็น dict ข้อมูลที่เข้าใจง่าย"""
    p = {}

    # ข้อมูลคดี
    p['คดีแดง']         = f"{inputs.get('law_suit_no','')} / {inputs.get('law_suit_year','')}".strip(' /')
    p['โจทก์']          = inputs.get('person1', '').strip()
    p['จำเลย']          = inputs.get('person2', '').strip()
    p['ศาล']            = inputs.get('law_court_name', '').strip()

    # ข้อมูลทรัพย์
    p['ประเภททรัพย์']   = inputs.get('assettypedesc', '').strip()
    p['โฉนดเลขที่']     = inputs.get('deedno', '').strip()
    p['ประเภทเอกสารสิทธิ์'] = inputs.get('landtype', '').strip()
    p['บ้านเลขที่']     = inputs.get('addrno', '').strip()
    p['ตำบล']           = inputs.get('tumbol', inputs.get('deedtumbol', '')).strip()
    p['อำเภอ']          = inputs.get('ampur', inputs.get('deedampur', '')).strip()
    p['จังหวัด']        = inputs.get('city', inputs.get('province_name', '')).strip()

    # เนื้อที่
    rai        = inputs.get('rai', '0').strip()
    quaterrai  = inputs.get('quaterrai', '0').strip()
    wa         = inputs.get('wa', '0').strip()
    p['เนื้อที่'] = f"{rai} ไร่ {quaterrai} งาน {wa} ตร.วา"

    # เจ้าของ
    p['เจ้าของ'] = inputs.get('ownername', inputs.get('owner_suit_name', '')).strip()

    # ราคา
    p['เงินหลักประกัน (บาท)'] = inputs.get('ReserveFund', '').strip()

    # ราคาประเมิน (เก็บ assetprice3 เป็นราคาหลัก หรือค่าที่ไม่เป็น 0)
    for i in range(1, 10):
        v = inputs.get(f'assetprice{i}', '0').strip()
        if v and v != '0':
            p['ราคาประเมิน (บาท)'] = v
            break
    else:
        p['ราคาประเมิน (บาท)'] = '0'

    # วันนัดประมูล 1-8
    for i in range(1, 9):
        date_val   = inputs.get(f'biddate{i}', '').strip()
        issale_val = inputs.get(f'issale{i}', '').strip()
        if date_val:
            p[f'นัดที่ {i} วันที่']  = format_thaidate(date_val)
            p[f'นัดที่ {i} สถานะ']   = ISSALE_MAP.get(issale_val, issale_val)

    # วันที่ประกาศ
    p['วันที่ตรวจสอบ'] = format_thaidate(inputs.get('ischeck_date', ''))

    # รูปภาพ
    pic = inputs.get('landpicture', '').strip()
    p['ลิงก์รูปภาพ'] = picture_path_to_url(pic)

    # แผนที่
    p['ลิงก์แผนที่'] = inputs.get('map', '').strip()

    # รหัสอ้างอิง
    p['รหัสทรัพย์ (auc_asset_gen)'] = inputs.get('auc_asset_gen', '').strip()
    p['โทรศัพท์สำนักงาน']          = inputs.get('tel', '').strip()

    return p


# =========================================================
# scraper หลัก
# =========================================================
def scrape(province_name):
    print(f"\n[{ts()}] {'='*50}")
    print(f"[{ts()}] เริ่มดึงข้อมูล จังหวัด: {province_name}")
    print(f"[{ts()}] Rate config: delay {RATE['page_delay_min']}-{RATE['page_delay_max']}s/page")
    print(f"[{ts()}] {'='*50}")

    if _rate_state['aborted']:
        print("[!] Session ถูก abort แล้ว — ข้ามจังหวัดนี้")
        return 0

    # สร้าง session ใหม่ทุกจังหวัด (reset cookies + fingerprint)
    session = requests.Session()
    # ไม่ set headers ถาวรใน session — ใช้ random headers ต่อ request แทน
    all_properties = []

    try:
        # --- Step 1: ดึง CAPTCHA (ไม่ delay request แรก) ---
        print(f"[{ts()}] กำลังดึง CAPTCHA...")
        res_get = _do_request(session, 'GET', LIST_URL, timeout=20)
        res_get.encoding = 'tis-620'
        soup_get = BeautifulSoup(res_get.text, 'html.parser')

        captcha_input = soup_get.find('input', {'name': 'oseckey'})
        if not captcha_input:
            print("ERROR: ไม่พบรหัส CAPTCHA")
            return 0
        captcha_code = captcha_input.get('value', '')
        print(f"[{ts()}] CAPTCHA: {captcha_code}")

        # --- Step 2: POST หน้าแรก ---
        encoded_province = urllib.parse.quote(province_name.encode('tis-620'))

        def make_payload(page):
            return (
                f"region_name={encoded_province}&province=&ampur=&tumbol=&asset_type=&"
                f"person1=&bid_date=&price_begin=&price_end=&rai_if=1&rai=&"
                f"quaterrai_if=1&quaterrai=&wa_if=1&wa=&"
                f"oseckey={captcha_code}&seckey={captcha_code}&search=ok&page={page}"
            )

        def _post_headers():
            """Merge random browser headers กับ content-type สำหรับ POST"""
            h = _random_headers()
            h["Content-Type"] = "application/x-www-form-urlencoded"
            h["Referer"]      = LIST_URL
            return h

        res_p1 = safe_post(session, LIST_URL,
                           data=make_payload(1), headers=_post_headers(), timeout=20)
        res_p1.encoding = 'tis-620'
        soup_p1 = BeautifulSoup(res_p1.text, 'html.parser')

        # นับจำนวนหน้า
        total_pages = 1
        m = re.search(r'หน้าที่\s*\d+\s*/\s*(\d+)', soup_p1.get_text())
        if m:
            total_pages = int(m.group(1))
        else:
            m2 = re.search(r'จาก\s*(\d+)', soup_p1.get_text())
            if m2:
                total_pages = int(m2.group(1))

        est_reqs = total_pages
        print(f"[{ts()}] พบทั้งหมด {total_pages} หน้า (ประมาณ {est_reqs} requests)")
        print(f"[{ts()}] เวลาโดยประมาณ: {est_reqs * (RATE['page_delay_min']+RATE['page_delay_max'])/2:.0f}s")

        # --- Step 3: วนลูปทุกหน้า ---
        for page_num in range(1, total_pages + 1):

            # ตรวจสอบการ abort (Ctrl+C หรือ circuit breaker)
            if _rate_state['aborted']:
                print(f"\n[!] หยุดที่หน้า {page_num}/{total_pages} — บันทึก partial data...")
                break

            print(f"\n[{ts()}] === หน้า {page_num}/{total_pages} | req#{_rate_state['total_requests']} ===")

            if page_num == 1:
                soup_cur = soup_p1
            else:
                res_cur = safe_post(session, LIST_URL,
                                    data=make_payload(page_num),
                                    headers=_post_headers(), timeout=20)
                res_cur.encoding = 'tis-620'
                soup_cur = BeautifulSoup(res_cur.text, 'html.parser')

            # หา form ทั้งหมดที่มี action="asset_open.asp"
            page_count = 0
            for form in soup_cur.find_all('form', action=re.compile(r'asset_open\.asp', re.I)):
                inputs = extract_form_data(form)
                if not inputs.get('law_suit_no'):
                    continue
                prop = parse_property(inputs)
                all_properties.append(prop)
                page_count += 1

                case_no = prop.get('คดีแดง', '')
                prov    = prop.get('จังหวัด', '')
                deed    = prop.get('โฉนดเลขที่', '')
                price   = prop.get('ราคาประเมิน (บาท)', '')
                print(f"  [{page_count:>3}] {case_no} | {prov} | โฉนด {deed} | {price} บาท")

            print(f"[{ts()}] หน้า {page_num}: ได้ {page_count} รายการ | รวมสะสม {len(all_properties)}")

        # --- Step 4: บันทึก CSV ---
        if not all_properties:
            print("ไม่พบข้อมูล")
            return

        # กำหนดลำดับคอลัมน์
        priority_cols = [
            'คดีแดง', 'โจทก์', 'จำเลย', 'ศาล',
            'ประเภททรัพย์', 'โฉนดเลขที่', 'ประเภทเอกสารสิทธิ์',
            'บ้านเลขที่', 'ตำบล', 'อำเภอ', 'จังหวัด', 'เนื้อที่',
            'เจ้าของ', 'เงินหลักประกัน (บาท)', 'ราคาประเมิน (บาท)',
            'นัดที่ 1 วันที่', 'นัดที่ 1 สถานะ',
            'นัดที่ 2 วันที่', 'นัดที่ 2 สถานะ',
            'นัดที่ 3 วันที่', 'นัดที่ 3 สถานะ',
            'นัดที่ 4 วันที่', 'นัดที่ 4 สถานะ',
            'นัดที่ 5 วันที่', 'นัดที่ 5 สถานะ',
            'นัดที่ 6 วันที่', 'นัดที่ 6 สถานะ',
            'วันที่ตรวจสอบ', 'ลิงก์รูปภาพ', 'ลิงก์แผนที่',
            'โทรศัพท์สำนักงาน', 'รหัสทรัพย์ (auc_asset_gen)',
        ]

        all_keys = set()
        for p in all_properties:
            all_keys.update(p.keys())

        ordered_cols = [c for c in priority_cols if c in all_keys]
        ordered_cols += sorted(c for c in all_keys if c not in priority_cols)

        date_str = datetime.now().strftime("%Y-%m-%d_%H%M")
        filename = f"led_data_{province_name}_{date_str}.csv"
        filepath = os.path.join(DATA_DIR, filename)

        with open(filepath, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_cols, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_properties)

        print(f"\n[{ts()}] บันทึกแล้ว: data/{filename}")
        print(f"[{ts()}] รวมทั้งหมด {len(all_properties)} รายการ | {len(ordered_cols)} คอลัมน์")
        return len(all_properties)

    except RuntimeError as e:
        if 'ABORTED' in str(e) or 'CIRCUIT BREAKER' in str(e) or 'SAFETY' in str(e):
            print(f"\n[!] หยุดโดยระบบป้องกัน: {e}")
            _rate_state['aborted'] = True
        else:
            print(f"ERROR: {e}")
            import traceback; traceback.print_exc()

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    # บันทึก partial data เสมอ ถ้ามีข้อมูล (ทำหลัง except เพื่อหลีกเลี่ยง return ใน finally)
    if all_properties:
        _save_csv(province_name, all_properties, partial=_rate_state['aborted'])
        return len(all_properties)

    print(f"[{ts()}] ไม่มีข้อมูลที่จะบันทึก")
    return 0


def _remove_old_province_files(province_name: str):
    """
    ลบ CSV เก่าของจังหวัดนี้ออกก่อนบันทึกไฟล์ใหม่
    รูปแบบที่ลบ: led_data_{province_name}_*.csv  (ทั้ง partial และ full)
    """
    import glob as _glob
    pattern = os.path.join(DATA_DIR, f"led_data_{province_name}_*.csv")
    old_files = _glob.glob(pattern)
    for f in old_files:
        try:
            os.remove(f)
            print(f"  [🗑] ลบไฟล์เก่า: {os.path.basename(f)}")
        except OSError as e:
            print(f"  [!] ลบไม่ได้: {f} — {e}")
    if old_files:
        print(f"  [🗑] ลบไฟล์เก่าของ '{province_name}' ทั้งหมด {len(old_files)} ไฟล์")


def _save_csv(province_name, all_properties, partial=False):
    priority_cols = [
        'คดีแดง', 'โจทก์', 'จำเลย', 'ศาล',
        'ประเภททรัพย์', 'โฉนดเลขที่', 'ประเภทเอกสารสิทธิ์',
        'บ้านเลขที่', 'ตำบล', 'อำเภอ', 'จังหวัด', 'เนื้อที่',
        'เจ้าของ', 'เงินหลักประกัน (บาท)', 'ราคาประเมิน (บาท)',
        'นัดที่ 1 วันที่', 'นัดที่ 1 สถานะ',
        'นัดที่ 2 วันที่', 'นัดที่ 2 สถานะ',
        'นัดที่ 3 วันที่', 'นัดที่ 3 สถานะ',
        'นัดที่ 4 วันที่', 'นัดที่ 4 สถานะ',
        'นัดที่ 5 วันที่', 'นัดที่ 5 สถานะ',
        'นัดที่ 6 วันที่', 'นัดที่ 6 สถานะ',
        'วันที่ตรวจสอบ', 'ลิงก์รูปภาพ', 'ลิงก์แผนที่',
        'โทรศัพท์สำนักงาน', 'รหัสทรัพย์ (auc_asset_gen)',
    ]
    all_keys = set()
    for p in all_properties:
        all_keys.update(p.keys())
    ordered_cols = [c for c in priority_cols if c in all_keys]
    ordered_cols += sorted(c for c in all_keys if c not in priority_cols)

    # ── ลบ CSV เก่าของจังหวัดนี้ก่อนบันทึก (deduplication) ──
    _remove_old_province_files(province_name)

    date_str = datetime.now().strftime("%Y-%m-%d_%H%M")
    suffix   = "_partial" if partial else ""
    filename = f"led_data_{province_name}_{date_str}{suffix}.csv"
    filepath = os.path.join(DATA_DIR, filename)

    with open(filepath, mode='w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=ordered_cols, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_properties)

    label = "[PARTIAL]" if partial else ""
    print(f"\n[{ts()}] {label} บันทึกแล้ว: data/{filename}")
    print(f"[{ts()}] รวมทั้งหมด {len(all_properties)} รายการ | {len(ordered_cols)} คอลัมน์")


if __name__ == "__main__":
    import sys as _sys

    # แสดง IP ก่อนเริ่มทำงานทุกครั้ง
    _check_and_show_ip()

    if len(_sys.argv) > 1:
        targets = _sys.argv[1:]
        if targets == ['all']:
            targets = ALL_PROVINCES
            # ⚠️ คำเตือนก่อน scrape ทั้ง 77 จังหวัด
            total_est = len(targets) * 40  # ประมาณการ
            print(f"\n{'='*55}")
            print(f"⚠️  คำเตือน: กำลังจะ scrape ทั้งหมด {len(targets)} จังหวัด")
            print(f"   ประมาณ {total_est}+ requests")
            delay_est = len(targets) * (
                RATE['province_delay_min'] + RATE['page_delay_min'] * 30
            ) / 60
            print(f"   ใช้เวลาโดยประมาณ: {delay_est:.0f}+ นาที")
            print(f"{'='*55}")
            confirm = input("พิมพ์ YES เพื่อยืนยัน: ").strip()
            if confirm != 'YES':
                print("ยกเลิก")
                exit(0)

        for i, prov in enumerate(targets):
            if _rate_state['aborted']:
                print("\n[!] Session ถูก abort — หยุดทั้งหมด")
                break

            scrape(prov)

            # พักระหว่างจังหวัด (ยกเว้นจังหวัดสุดท้าย)
            if i < len(targets) - 1 and not _rate_state['aborted']:
                wait = random.uniform(
                    RATE['province_delay_min'],
                    RATE['province_delay_max']
                )
                print(f"\n[⏸] พักระหว่างจังหวัด {wait:.0f}s ก่อนเริ่ม '{targets[i+1]}'...")
                time.sleep(wait)

        print(f"\n{'='*55}")
        print(f"[{ts()}] สรุป: total requests = {_rate_state['total_requests']}")
        print(f"{'='*55}")

    else:
        # แสดงเมนูเลือกจังหวัด
        print("\n=== ระบบดึงข้อมูลทรัพย์บังคับคดี ===")
        for region, provs in PROVINCES_BY_REGION.items():
            print(f"\n{region}:")
            print("  " + "  ".join(provs))
        print(f"\n{'─'*45}")
        print("วิธีใช้:")
        print("  python scraper.py กระบี่")
        print("  python scraper.py กระบี่ ภูเก็ต สงขลา")
        print("  python scraper.py all  (ทั้ง 77 จังหวัด — ต้องยืนยัน)")
        print(f"\nRate config ปัจจุบัน:")
        print(f"  delay ระหว่างหน้า: {RATE['page_delay_min']}-{RATE['page_delay_max']}s")
        print(f"  delay ระหว่างจังหวัด: {RATE['province_delay_min']}-{RATE['province_delay_max']}s")
        print(f"  max requests/session: {RATE['max_requests_per_session']}")
        print()
        prov = input("พิมพ์ชื่อจังหวัดที่ต้องการดึง: ").strip()
        if prov:
            scrape(prov)
