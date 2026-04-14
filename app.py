import sys
import io
import os
import csv
import glob
import json
import time
from flask import Flask, render_template, jsonify, request, redirect, url_for
from werkzeug.utils import secure_filename

# ป้องกัน crash บน Vercel ที่ stdout อาจไม่มี .buffer
try:
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
except Exception:
    pass

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB upload limit

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ——— 77 จังหวัด (ใช้ร่วมกับ scraper.py) ———
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

ISSALE_LABELS = {
    '': 'ยังไม่ถึงนัด', '-': 'ยังไม่ถึงนัด', '0': 'ยังไม่ถึงนัด',
    '1': 'ขายได้', '2': 'งดขาย', '3': 'งดขายไม่มีผู้สู้ราคา',
    '4': 'เลื่อนนัด', '5': 'ถอนการขาย', '6': 'งดขาย',
    '7': 'รอดำเนินการ', '8': 'รอดำเนินการ', '10': 'รอดำเนินการ', '26': 'รอดำเนินการ',
    'ขายได้': 'ขายได้', 'งดขาย': 'งดขาย',
    'งดขายไม่มีผู้สู้ราคา': 'งดขายไม่มีผู้สู้ราคา',
    'เลื่อนนัด': 'เลื่อนนัด', 'ถอนการขาย': 'ถอนการขาย', 'รอดำเนินการ': 'รอดำเนินการ',
}
ISSALE_CAT = {
    'ขายได้': 'sold', 'งดขาย': 'cancelled',
    'งดขายไม่มีผู้สู้ราคา': 'no_bidder', 'เลื่อนนัด': 'postponed',
    'ถอนการขาย': 'withdrawn', 'ยังไม่ถึงนัด': 'upcoming', 'รอดำเนินการ': 'upcoming',
}

# ——— In-memory cache ———
_cache = {'props': [], 'mtimes': {}, 'province_counts': {}}


def get_data_files():
    return sorted(glob.glob(os.path.join(DATA_DIR, 'led_data_*.csv')))


def needs_reload():
    files = get_data_files()
    if len(files) != len(_cache['mtimes']):
        return True
    return any(_cache['mtimes'].get(f) != os.path.getmtime(f) for f in files)


def parse_row(row, idx):
    auctions = []
    for n in range(1, 9):
        dv = row.get(f'นัดที่ {n} วันที่', '').strip()
        sv = row.get(f'นัดที่ {n} สถานะ', '').strip()
        if dv and dv not in ('', '-'):
            lbl = ISSALE_LABELS.get(sv, sv or 'ยังไม่ถึงนัด')
            auctions.append({'round': n, 'date': dv, 'status': lbl, 'category': ISSALE_CAT.get(lbl, 'upcoming')})

    def to_int(s):
        try: return int(float(str(s).replace(',', '')))
        except: return 0

    price   = to_int(row.get('ราคาประเมิน (บาท)', 0))
    deposit = to_int(row.get('เงินหลักประกัน (บาท)', 0))

    cur = 'ยังไม่ถึงนัด'
    for a in reversed(auctions):
        if a['category'] != 'upcoming':
            cur = a['status']; break
    if cur == 'ยังไม่ถึงนัด' and auctions:
        cur = 'รอประมูล'

    no_bid = sum(1 for a in auctions if a['category'] == 'no_bidder')

    return {
        'id': idx,
        'คดีแดง':     row.get('คดีแดง', '').strip(),
        'โจทก์':      row.get('โจทก์', '').strip(),
        'จำเลย':      row.get('จำเลย', '').strip(),
        'ศาล':        row.get('ศาล', '').strip(),
        'ประเภท':     row.get('ประเภททรัพย์', '').strip(),
        'โฉนดเลขที่': row.get('โฉนดเลขที่', '').strip(),
        'เอกสารสิทธิ์': row.get('ประเภทเอกสารสิทธิ์', '').strip(),
        'บ้านเลขที่':  row.get('บ้านเลขที่', '').strip(),
        'ตำบล':       row.get('ตำบล', '').strip(),
        'อำเภอ':      row.get('อำเภอ', '').strip(),
        'จังหวัด':    row.get('จังหวัด', '').strip(),
        'เนื้อที่':   row.get('เนื้อที่', '').strip(),
        'เจ้าของ':    row.get('เจ้าของ', '').strip(),
        'ราคา':       price,
        'ราคา_text':  f"{price:,}",
        'เงินประกัน': deposit,
        'เงินประกัน_text': f"{deposit:,}",
        'รูปภาพ':     row.get('ลิงก์รูปภาพ', '').strip(),
        'แผนที่':     row.get('ลิงก์แผนที่', '').strip(),
        'โทรศัพท์':  row.get('โทรศัพท์สำนักงาน', '').strip(),
        'รหัสทรัพย์': row.get('รหัสทรัพย์ (auc_asset_gen)', '').strip(),
        'วันที่ตรวจสอบ': row.get('วันที่ตรวจสอบ', '').strip(),
        'auctions':      auctions,
        'current_status': cur,
        'total_rounds':   len(auctions),
        'no_bidder_rounds': no_bid,
    }


def load_all():
    if not needs_reload():
        return _cache['props']

    files = get_data_files()
    props, mtimes, pcounts = [], {}, {}

    for fpath in files:
        try:
            with open(fpath, encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    p = parse_row(row, len(props))
                    props.append(p)
                    pv = p['จังหวัด'] or _province_from_filename(fpath)
                    pcounts[pv] = pcounts.get(pv, 0) + 1
            mtimes[fpath] = os.path.getmtime(fpath)
        except Exception as e:
            print(f"[WARN] {fpath}: {e}")

    _cache['props']           = props
    _cache['mtimes']          = mtimes
    _cache['province_counts'] = pcounts
    print(f"[Cache] โหลด {len(props)} รายการ จาก {len(files)} ไฟล์")
    return props


def _province_from_filename(fpath):
    name = os.path.basename(fpath)
    m = __import__('re').search(r'led_data_(.+?)_\d{4}-\d{2}-\d{2}', name)
    return m.group(1) if m else ''


def filter_sort_props(props, province='', asset_type='', amphur='', status='', search='', sort='default'):
    q = search.lower().strip()
    out = []
    for p in props:
        if province and p['จังหวัด'] != province:
            continue
        if asset_type and p['ประเภท'] != asset_type:
            continue
        if amphur and p['อำเภอ'] != amphur:
            continue
        if status and p['current_status'] != status:
            continue
        if q:
            hay = ' '.join([p['คดีแดง'], p['โจทก์'], p['จำเลย'], p['โฉนดเลขที่'], p['เจ้าของ']]).lower()
            if q not in hay:
                continue
        out.append(p)

    if sort == 'price_asc':    out.sort(key=lambda x: x['ราคา'])
    elif sort == 'price_desc': out.sort(key=lambda x: -x['ราคา'])
    elif sort == 'no_bidder':  out.sort(key=lambda x: -x['no_bidder_rounds'])
    elif sort == 'deposit_asc': out.sort(key=lambda x: x['เงินประกัน'])
    return out


# ========================== ROUTES ==========================

@app.route('/')
def index():
    return render_template('index.html',
                           provinces_by_region=PROVINCES_BY_REGION,
                           all_provinces=ALL_PROVINCES)


@app.route('/api/provinces')
def api_provinces():
    load_all()
    pc = _cache['province_counts']
    result = {}
    for region, provs in PROVINCES_BY_REGION.items():
        result[region] = [
            {'name': p, 'count': pc.get(p, 0), 'has_data': p in pc}
            for p in provs
        ]
    return jsonify({
        'by_region': result,
        'total_provinces_with_data': len(pc),
        'total_provinces': len(ALL_PROVINCES),
        'province_counts': pc,
    })


@app.route('/api/data')
def api_data():
    province   = request.args.get('province', '')
    asset_type = request.args.get('type', '')
    amphur     = request.args.get('amphur', '')
    status     = request.args.get('status', '')
    search     = request.args.get('search', '')
    sort       = request.args.get('sort', 'default')
    page       = max(1, int(request.args.get('page', 1)))
    per_page   = min(100, int(request.args.get('per_page', 48)))

    props    = load_all()
    filtered = filter_sort_props(props, province, asset_type, amphur, status, search, sort)

    total       = len(filtered)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = min(page, total_pages)
    start       = (page - 1) * per_page
    page_props  = filtered[start: start + per_page]

    # สถิติเฉพาะ filtered set
    total_price  = sum(p['ราคา'] for p in filtered)
    type_counts  = {}
    status_counts = {}
    for p in filtered:
        t = p['ประเภท'] or 'ไม่ระบุ'
        type_counts[t] = type_counts.get(t, 0) + 1
        s = p['current_status']
        status_counts[s] = status_counts.get(s, 0) + 1

    # อำเภอที่มีใน province ที่เลือก (สำหรับ filter dropdown)
    amphur_counts = {}
    for p in (props if not province else [x for x in props if x['จังหวัด'] == province]):
        a = p['อำเภอ']
        if a and a != '-':
            amphur_counts[a] = amphur_counts.get(a, 0) + 1

    return jsonify({
        'properties':   page_props,
        'total':        total,
        'page':         page,
        'per_page':     per_page,
        'total_pages':  total_pages,
        'stats': {
            'total_price':      total_price,
            'total_price_text': f"{total_price:,}",
            'type_counts':      type_counts,
            'status_counts':    status_counts,
            'amphur_counts':    dict(sorted(amphur_counts.items(), key=lambda x: -x[1])),
        },
    })


@app.route('/api/upload', methods=['POST'])
def api_upload():
    """รับไฟล์ CSV และบันทึกลง data/ folder"""
    files = request.files.getlist('files')
    saved, errors = [], []
    for f in files:
        if not f.filename.endswith('.csv'):
            errors.append(f'{f.filename}: ต้องเป็นไฟล์ .csv')
            continue
        name = secure_filename(f.filename)
        dest = os.path.join(DATA_DIR, name)
        try:
            f.save(dest)
            saved.append(name)
            _cache['mtimes'] = {}  # Reset cache
        except (PermissionError, OSError):
            errors.append(f'{f.filename}: ไม่สามารถบันทึกไฟล์ได้ (สภาพแวดล้อมนี้เป็น read-only — ใช้ scraper.py บนเครื่องตัวเองแล้ว commit ข้อมูลขึ้น Git แทน)')

    return jsonify({'saved': saved, 'errors': errors})


@app.route('/api/export-json')
def api_export_json():
    """Export ข้อมูลทั้งหมดเป็น JSON สำหรับ Vercel deployment"""
    province = request.args.get('province', '')
    props = load_all()
    if province:
        props = [p for p in props if p['จังหวัด'] == province]
    return jsonify({
        'generated_at': __import__('datetime').datetime.now().isoformat(),
        'total': len(props),
        'provinces_by_region': PROVINCES_BY_REGION,
        'properties': props,
    })


if __name__ == '__main__':
    # โหลด data ล่วงหน้า
    load_all()
    print("เปิดเว็บที่ http://127.0.0.1:5000")
    app.run(debug=True, port=5000)
