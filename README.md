# 🥚 ROON KHANOMKHAI — ระบบบริหารจัดการร้าน

ระบบบริหารจัดการร้านรุนขนมไข่ สร้างด้วย Python + Streamlit + Excel Database

---

## 📋 ความต้องการของระบบ

- Python 3.10 ขึ้นไป
- ไม่ต้องใช้ Database ภายนอก (ใช้ Excel เป็น Database)

---

## 🚀 วิธีติดตั้ง

```bash
# 1. Clone หรือวางโฟลเดอร์โปรเจกต์
cd roon_management_system

# 2. ติดตั้ง dependencies
pip install -r requirements.txt

# 3. รันโปรแกรม
streamlit run app.py
```

เปิดเบราเซอร์ที่ **http://localhost:8501**

---

## 📁 โครงสร้างไฟล์

```
roon_management_system/
├── app.py                    # Main app + Router
├── config.py                 # ค่าคงที่, ชื่อ Sheet ทั้งหมด
├── requirements.txt
├── README.md
├── data/
│   └── roon_database.xlsx    # Excel Database (41 Sheets)
├── modules/
│   ├── excel_db.py           # CRUD กลาง (read/write/append/update/delete)
│   ├── master_data.py        # Master Data + Seed ข้อมูลตั้งต้น
│   ├── branch_report.py      # รายงานปิดยอดรายวัน (สาขา)
│   ├── audit.py              # ตรวจสอบ Audit + DIFF สีแดง
│   ├── purchase.py           # จัดซื้อ + Stock
│   ├── production.py         # ฝ่ายผลิต
│   ├── hr.py                 # HR + เงินเดือน
│   ├── finance.py            # บัญชีธนาคาร + ค่าใช้จ่าย
│   ├── accounting.py         # Marketing + Reconcile
│   └── dashboard.py          # Dashboard + Export Power BI
└── utils/
    ├── id_generator.py       # สร้าง ID อัตโนมัติ
    ├── validators.py         # ตรวจสอบข้อมูล
    └── calculations.py       # สูตรคำนวณ
```

---

## 📊 Excel Database (41 Sheets)

| กลุ่ม | Sheets |
|-------|--------|
| Master Data | branch_groups, area_master, branches, item_categories, items, products, sales_channels, users, roles |
| Branch Daily Report | branch_daily_reports, branch_front_sales_packaging, branch_drink_sales_detail, branch_material_balance, branch_packaging_balance, delivery_packaging_sales, branch_other_stock_balance, branch_special_remark, branch_sales_recheck |
| Audit | audit_sessions, audit_packaging_balance, audit_packaging_diff, true_stock_balance, daily_stock_usage, daily_packaging_cost |
| Purchase & Stock | purchase_orders, purchase_order_items, stock_in_to_branch, stock_movements |
| Production | production_batches, production_material_used |
| HR | employees, payroll_periods, payroll_records, late_deduction_rules |
| Finance | bank_accounts, bank_transactions, daily_sales_accounting, branch_expenses |
| Marketing | marketing_daily_sales, marketing_daily_sales_items, sales_reconcile |

---

## 🗂️ เมนูและการใช้งาน

### 📋 Master Data
- เพิ่ม / แก้ไข / ลบ สาขา, Items, Products
- ดูข้อมูลอ้างอิง (กลุ่มสาขา, พื้นที่, ช่องทางขาย)

### 📊 Branch Daily Report
- **พนักงานสาขา** กรอกรายงานปิดยอดประจำวัน
- ระบบคำนวณ `total_received` อัตโนมัติ
- แสดง DIFF สีแดงทันทีถ้ายอดไม่ตรง

### 🔎 Audit
- **ฝ่ายตรวจสอบ** กรอกจำนวนตรวจนับจริง
- ระบบเปรียบเทียบกับข้อมูลสาขาอัตโนมัติ
- DIFF = 0 → เขียว | DIFF ≠ 0 → **แดง ตัวใหญ่**
- บันทึก True Stock จากข้อมูล Audit เท่านั้น

### 🛒 Purchase / Stock
- บันทึกใบสั่งซื้อ
- เบิกของเข้าสาขา
- ดู Stock คงเหลือ + แจ้งเตือนต่ำกว่าขั้นต่ำ

### 🏭 Production
- บันทึก Batch การผลิตแป้งสำเร็จ
- บันทึกวัตถุดิบที่ใช้ → stock_movements อัตโนมัติ

### 👥 HR
- จัดการข้อมูลพนักงาน (CRUD + ค้นหา)
- สร้างรอบจ่ายเงินเดือน (เดือนละ 2 รอบ)
- คำนวณรายได้: ค่าแรง + เบี้ย − หัก − ประกันสังคม
- Export รายงาน Excel

### 💰 Finance
- จัดการบัญชีธนาคาร
- บันทึกเงินเข้า / เงินออก (ยอดอัปเดตอัตโนมัติ)
- บันทึกยอดขายฝ่ายบัญชี
- บันทึกค่าใช้จ่ายสาขา

### 📒 Accounting (Marketing)
- บันทึกยอดขายฝ่ายการตลาดแยกสินค้า
- Reconcile เทียบยอด 3 ฝ่าย (สาขา / บัญชี / การตลาด)
- DIFF สีแดงเมื่อยอดไม่ตรง

### 📈 Dashboard
- Executive Dashboard (KPI รวม, ยอดขายแยกสาขา)
- Branch Performance (กำไร/ขาดทุนแต่ละสาขา)
- Stock Control (คงเหลือ + แจ้งเตือนใกล้หมด)
- Fraud & Audit (รายการ DIFF + พฤติกรรมพนักงาน)
- **Export Power BI** (ดาวน์โหลด Excel 4 Views)

---

## 🔴 กฎสำคัญของระบบ

1. **ข้อมูลสาขา** และ **ข้อมูล Audit** แยก Field กันเสมอ
2. **Stock จริง** ใช้ตัวเลขจาก Audit เท่านั้น ห้ามใช้ตัวเลขสาขา
3. DIFF ≠ 0 ต้องแสดง **สีแดง** ตัวใหญ่ ชัดเจน
4. กลุ่มสาขา / พื้นที่ ต้องเลือกจาก Master เท่านั้น

---

## 🔧 Export สำหรับ Power BI

1. เปิดเมนู **📈 Dashboard → Export Power BI**
2. กดปุ่ม **ดาวน์โหลด PowerBI_Export.xlsx**
3. เปิด Power BI Desktop → Get Data → Excel
4. เชื่อม `branch_id` เป็น Relationship ระหว่าง View

---

## 📞 สนับสนุน

ระบบพัฒนาโดย Claude AI สำหรับ ร้านรุนขนมไข่ไส้เนย สงขลา
