"""
market_scraper.py — สร้างฐานข้อมูลราคากลางตลาดจากข้อมูล LED auction
======================================================================
ใช้ข้อมูลการประมูลคดีจาก LED (กรมบังคับคดี) ที่ดึงมาแล้วใน data/
คำนวณ median ราคา/ตร.วา ต่อจังหวัด เพื่อใช้เป็น benchmark เปรียบเทียบ

วิธีใช้:
    python market_scraper.py            # คำนวณทุกจังหวัด
    python market_scraper.py เชียงใหม่   # คำนวณเฉพาะจังหวัด
    python market_scraper.py --reset     # ล้างแล้วคำนวณใหม่ทั้งหมด

Output: data/market_benchmark.json
"""

import sys
import os
import re
import csv
import glob
import json
import io
import statistics
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, 'data')
OUTPUT_FILE = os.path.join(DATA_DIR, 'market_benchmark.json')


def parse_area(text: str) -> float:
    """'X ไร่ Y งาน Z ตร.วา' → ตารางวา"""
    if not text or text in ('-', ''):
        return 0.0
    m = re.search(r'(\d+\.?\d*)\s*ไร่\s*(\d+\.?\d*)\s*งาน\s*(\d+\.?\d*)', text)
    if m:
        return float(m.group(1)) * 400 + float(m.group(2)) * 100 + float(m.group(3))
    m2 = re.search(r'(\d+\.?\d*)', text)
    return float(m2.group(1)) if m2 else 0.0


def compute_benchmark(province_filter: list | None = None) -> dict:
    """
    อ่าน CSV ทุกไฟล์ใน data/ → คำนวณ price/ตร.วา ต่อจังหวัด
    ตัด outlier ด้วย IQR ก่อนคำนวณ median/mean
    """
    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, 'led_data_*.csv')))
    if not csv_files:
        print("[ERR] ไม่พบ CSV ใน data/ — รัน scraper.py ก่อน")
        sys.exit(1)

    print(f"  อ่าน {len(csv_files)} ไฟล์ CSV...")
    acc: dict[str, list[float]] = {}

    for fpath in csv_files:
        try:
            with open(fpath, encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    pv = row.get('จังหวัด', '').strip()
                    if not pv or pv == '-':
                        continue
                    if province_filter and pv not in province_filter:
                        continue
                    try:
                        price = int(float(str(row.get('ราคาประเมิน (บาท)', '0')).replace(',', '')))
                    except Exception:
                        continue
                    area = parse_area(row.get('เนื้อที่', ''))
                    if price > 0 and area > 0:
                        ppsw = price / area
                        if ppsw < 5_000_000:   # กรองข้อมูลผิดปกติ
                            acc.setdefault(pv, []).append(ppsw)
        except Exception as e:
            print(f"  [WARN] {os.path.basename(fpath)}: {e}")

    result: dict[str, dict] = {}
    for pv, vals in sorted(acc.items()):
        vals.sort()
        n = len(vals)
        if n < 3:
            continue
        # IQR outlier removal
        q1, q3 = vals[n // 4], vals[n * 3 // 4]
        iqr    = q3 - q1
        clean  = [v for v in vals if q1 - 1.5 * iqr <= v <= q3 + 1.5 * iqr] or vals
        median = round(statistics.median(clean))
        mean   = round(sum(clean) / len(clean))
        result[pv] = {
            'median_price_sqwah': median,
            'avg_price_sqwah':    mean,
            'p25':                round(clean[len(clean) // 4]),
            'p75':                round(clean[len(clean) * 3 // 4]),
            'listings':           n,
            'sample':             len(clean),
        }

    return result


def load_existing() -> dict:
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, encoding='utf-8') as f:
            return json.load(f).get('provinces', {})
    return {}


def save(provinces: dict):
    out = {
        'generated_at': datetime.now().isoformat(),
        'source':       'LED auction data (กรมบังคับคดี)',
        'note':         'median_price_sqwah = บาท/ตร.วา คำนวณจากราคาประเมินจริงในคดีประมูล',
        'provinces':    provinces,
    }
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


def main():
    args    = [a for a in sys.argv[1:] if not a.startswith('-')]
    flags   = [a for a in sys.argv[1:] if a.startswith('-')]
    reset   = '--reset' in flags

    if reset and os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print("[รีเซ็ต] ลบไฟล์เก่าแล้ว\n")

    targets = args if args else None   # None = ทุกจังหวัด

    print("=" * 48)
    print("  LED Market Benchmark Builder")
    print("=" * 48)
    if targets:
        print(f"  จังหวัดเป้าหมาย: {', '.join(targets)}")
    else:
        print("  คำนวณทุกจังหวัดที่มีข้อมูล")
    print()

    existing   = load_existing()
    new_data   = compute_benchmark(targets)

    # merge: ถ้า reset ใช้ new_data อย่างเดียว ถ้าไม่ reset merge เข้ากับเดิม
    merged = {} if reset else dict(existing)
    merged.update(new_data)

    save(merged)

    # ── สรุปผล ──
    print()
    print(f"{'จังหวัด':<22} {'median บาท/ตร.วา':>18} {'listings':>10}")
    print("-" * 52)
    top    = sorted(merged.items(), key=lambda x: -x[1].get('median_price_sqwah', 0))
    for pv, b in top:
        m = b.get('median_price_sqwah', 0)
        n = b.get('listings', 0)
        bar = '█' * min(30, max(1, m // 5000))
        print(f"  {pv:<20} {m:>15,}  {n:>8,}  {bar}")

    print()
    print(f"✅ บันทึกแล้ว: {OUTPUT_FILE}")
    print(f"   {len(merged)} จังหวัด · สูงสุด {top[0][0]} {top[0][1]['median_price_sqwah']:,} บาท/ตร.วา")


if __name__ == '__main__':
    main()
