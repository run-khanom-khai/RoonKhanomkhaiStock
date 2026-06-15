import os

# ===== PATH CONFIG =====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH  = os.path.join(DATA_DIR, "roon_database.xlsx")
os.makedirs(DATA_DIR, exist_ok=True)

# ===== APP CONFIG =====
APP_TITLE  = "ROON KHANOMKHAI – ระบบบริหารจัดการร้าน"
APP_ICON   = "🥚"
APP_LAYOUT = "wide"

# ===== MASTER DATA =====
SHEET_BRANCH_GROUPS   = "branch_groups"
SHEET_AREA_MASTER     = "area_master"
SHEET_BRANCHES        = "branches"
SHEET_ITEM_CATEGORIES = "item_categories"
SHEET_ITEMS           = "items"
SHEET_PRODUCTS        = "products"
SHEET_SALES_CHANNELS  = "sales_channels"
SHEET_USERS           = "users"
SHEET_ROLES           = "roles"

# ===== BRANCH DAILY REPORT =====
SHEET_BRANCH_DAILY_REPORTS       = "branch_daily_reports"
SHEET_BRANCH_FRONT_SALES_PKG     = "branch_front_sales_packaging"
SHEET_BRANCH_DRINK_SALES         = "branch_drink_sales_detail"
SHEET_BRANCH_MATERIAL_BALANCE    = "branch_material_balance"
SHEET_BRANCH_PACKAGING_BALANCE   = "branch_packaging_balance"
SHEET_DELIVERY_PACKAGING_SALES   = "delivery_packaging_sales"
SHEET_BRANCH_OTHER_STOCK_BALANCE = "branch_other_stock_balance"
SHEET_BRANCH_SPECIAL_REMARK      = "branch_special_remark"
SHEET_BRANCH_SALES_RECHECK       = "branch_sales_recheck"

# ===== AUDIT =====
SHEET_AUDIT_SESSIONS          = "audit_sessions"
SHEET_AUDIT_PACKAGING_BALANCE = "audit_packaging_balance"
SHEET_AUDIT_PACKAGING_DIFF    = "audit_packaging_diff"
SHEET_TRUE_STOCK_BALANCE      = "true_stock_balance"
SHEET_DAILY_STOCK_USAGE       = "daily_stock_usage"
SHEET_DAILY_PACKAGING_COST    = "daily_packaging_cost"

# ===== PURCHASE & STOCK =====
SHEET_PURCHASE_ORDERS      = "purchase_orders"
SHEET_PURCHASE_ORDER_ITEMS = "purchase_order_items"
SHEET_STOCK_IN_TO_BRANCH   = "stock_in_to_branch"
SHEET_STOCK_MOVEMENTS      = "stock_movements"

# ===== PRODUCTION =====
SHEET_PRODUCTION_BATCHES       = "production_batches"
SHEET_PRODUCTION_MATERIAL_USED = "production_material_used"

# ===== HR =====
SHEET_EMPLOYEES          = "employees"
SHEET_PAYROLL_PERIODS    = "payroll_periods"
SHEET_PAYROLL_RECORDS    = "payroll_records"
SHEET_LATE_DEDUCTION_RULES = "late_deduction_rules"

# ===== FINANCE & ACCOUNTING =====
SHEET_BANK_ACCOUNTS         = "bank_accounts"
SHEET_BANK_TRANSACTIONS     = "bank_transactions"
SHEET_DAILY_SALES_ACCOUNTING = "daily_sales_accounting"
SHEET_BRANCH_EXPENSES       = "branch_expenses"

# ===== MARKETING =====
SHEET_MARKETING_DAILY_SALES       = "marketing_daily_sales"
SHEET_MARKETING_DAILY_SALES_ITEMS = "marketing_daily_sales_items"
SHEET_SALES_RECONCILE             = "sales_reconcile"

# ===== LOOKUP LISTS =====
PURCHASE_CATEGORIES = [
    "วัตถุดิบ","อุปกรณ์สำนักงาน","ทรัพย์สิน",
    "เครื่องเขียน","วัสดุสิ้นเปลือง","อื่น ๆ",
]
MOVEMENT_TYPES = [
    "purchase_in","transfer_out","transfer_in",
    "used","sold","audit_adjust","waste","production_in",
]
EMPLOYEE_STATUSES = ["active","resigned","on_leave"]
POSITIONS = ["พนักงานสาขา","หัวหน้าสาขา","ฝ่ายผลิต","ฝ่ายบัญชี","ฝ่ายการเงิน",
             "ฝ่ายจัดซื้อ","ฝ่ายตรวจสอบ","ฝ่ายการตลาด","ผู้บริหาร","อื่น ๆ"]

# ===== ALL SHEETS =====
ALL_SHEETS = [
    # Master
    SHEET_BRANCH_GROUPS, SHEET_AREA_MASTER, SHEET_BRANCHES,
    SHEET_ITEM_CATEGORIES, SHEET_ITEMS, SHEET_PRODUCTS,
    SHEET_SALES_CHANNELS, SHEET_USERS, SHEET_ROLES,
    # Branch Daily Report
    SHEET_BRANCH_DAILY_REPORTS, SHEET_BRANCH_FRONT_SALES_PKG,
    SHEET_BRANCH_DRINK_SALES, SHEET_BRANCH_MATERIAL_BALANCE,
    SHEET_BRANCH_PACKAGING_BALANCE, SHEET_DELIVERY_PACKAGING_SALES,
    SHEET_BRANCH_OTHER_STOCK_BALANCE, SHEET_BRANCH_SPECIAL_REMARK,
    SHEET_BRANCH_SALES_RECHECK,
    # Audit
    SHEET_AUDIT_SESSIONS, SHEET_AUDIT_PACKAGING_BALANCE,
    SHEET_AUDIT_PACKAGING_DIFF, SHEET_TRUE_STOCK_BALANCE,
    SHEET_DAILY_STOCK_USAGE, SHEET_DAILY_PACKAGING_COST,
    # Purchase & Stock
    SHEET_PURCHASE_ORDERS, SHEET_PURCHASE_ORDER_ITEMS,
    SHEET_STOCK_IN_TO_BRANCH, SHEET_STOCK_MOVEMENTS,
    # Production
    SHEET_PRODUCTION_BATCHES, SHEET_PRODUCTION_MATERIAL_USED,
    # HR
    SHEET_EMPLOYEES, SHEET_PAYROLL_PERIODS,
    SHEET_PAYROLL_RECORDS, SHEET_LATE_DEDUCTION_RULES,
    # Finance & Accounting
    SHEET_BANK_ACCOUNTS, SHEET_BANK_TRANSACTIONS,
    SHEET_DAILY_SALES_ACCOUNTING, SHEET_BRANCH_EXPENSES,
    # Marketing
    SHEET_MARKETING_DAILY_SALES, SHEET_MARKETING_DAILY_SALES_ITEMS,
    SHEET_SALES_RECONCILE,
]
