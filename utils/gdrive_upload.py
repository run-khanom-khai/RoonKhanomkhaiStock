"""
gdrive_upload.py  –  Google Drive File Upload Helper
เก็บไฟล์แนบ (ใบเสร็จ, สลิป) ไว้ใน Google Drive แทน Google Sheets
ไม่มีปัญหา cell limit, รองรับไฟล์ใหญ่ได้ไม่จำกัด
"""
import io
import os
import streamlit as st

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

FOLDER_NAME = "roon_petty_cash_attachments"

# ── MIME type mapping ────────────────────────────────────────
MIME_MAP = {
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".pdf":  "application/pdf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


# ── Build Drive service ──────────────────────────────────────
def _get_drive_service():
    """สร้าง Google Drive service โดยดึง credentials จาก secrets หรือ local file"""
    try:
        # Streamlit Cloud → ดึงจาก st.secrets
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    except Exception:
        # Local → ดึงจากไฟล์ credentials.json
        json_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "credentials.json"
        )
        creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


# ── Get or Create Folder ─────────────────────────────────────
def _get_or_create_folder(service, folder_name: str) -> str:
    """ค้นหา folder ใน Google Drive ถ้าไม่มีสร้างใหม่ คืน folder_id"""
    # ค้นหา folder เดิม
    query = (
        f"name='{folder_name}' "
        f"and mimeType='application/vnd.google-apps.folder' "
        f"and trashed=false"
    )
    results = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
        pageSize=1,
    ).execute()
    files = results.get("files", [])

    if files:
        return files[0]["id"]

    # สร้าง folder ใหม่
    folder_meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = service.files().create(
        body=folder_meta, fields="id"
    ).execute()
    folder_id = folder.get("id")

    # ตั้ง permission ให้ anyone with link view ได้
    service.permissions().create(
        fileId=folder_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()

    return folder_id


# ── Upload File ──────────────────────────────────────────────
def upload_file_to_drive(
    uploaded_file,
    subfolder: str = "",
    request_id: str = "",
) -> dict:
    """
    อัปโหลดไฟล์ขึ้น Google Drive
    Returns: {"file_id": str, "url": str, "name": str, "mime_type": str}
    Raises: Exception ถ้า upload ไม่สำเร็จ
    """
    service = _get_drive_service()

    # ดึง folder_id
    folder_name = FOLDER_NAME
    if subfolder:
        folder_name = f"{FOLDER_NAME}/{subfolder}"
    folder_id = _get_or_create_folder(service, FOLDER_NAME)

    # ถ้ามี subfolder (เช่น request_id) สร้างอีกชั้น
    if request_id:
        # ค้นหา subfolder ภายใน folder หลัก
        query = (
            f"name='{request_id}' "
            f"and mimeType='application/vnd.google-apps.folder' "
            f"and '{folder_id}' in parents "
            f"and trashed=false"
        )
        results = service.files().list(
            q=query, spaces="drive",
            fields="files(id, name)", pageSize=1
        ).execute()
        sub_files = results.get("files", [])

        if sub_files:
            folder_id = sub_files[0]["id"]
        else:
            sub_meta = {
                "name": request_id,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [folder_id],
            }
            sub = service.files().create(
                body=sub_meta, fields="id"
            ).execute()
            folder_id = sub.get("id")
            # ตั้ง permission
            service.permissions().create(
                fileId=folder_id,
                body={"type": "anyone", "role": "reader"},
            ).execute()

    # หา MIME type
    ext = os.path.splitext(uploaded_file.name)[-1].lower()
    mime_type = MIME_MAP.get(ext, "application/octet-stream")

    # อ่านไฟล์
    uploaded_file.seek(0)
    file_bytes = uploaded_file.read()

    # Upload
    file_meta = {
        "name": uploaded_file.name,
        "parents": [folder_id],
    }
    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes),
        mimetype=mime_type,
        resumable=False,
    )
    uploaded = service.files().create(
        body=file_meta,
        media_body=media,
        fields="id, name, mimeType, webViewLink, webContentLink",
    ).execute()

    file_id = uploaded.get("id")

    # ตั้ง permission ให้ anyone with link view ได้
    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()

    view_url = uploaded.get(
        "webViewLink",
        f"https://drive.google.com/file/d/{file_id}/view"
    )

    return {
        "file_id":   file_id,
        "url":       view_url,
        "name":      uploaded_file.name,
        "mime_type": mime_type,
    }


# ── Get File URL ─────────────────────────────────────────────
def get_file_url(file_id: str) -> str:
    """คืน URL สำหรับดูไฟล์จาก Drive"""
    if not file_id:
        return ""
    return f"https://drive.google.com/file/d/{file_id}/view"


def get_thumbnail_url(file_id: str) -> str:
    """คืน URL thumbnail (สำหรับรูปภาพ)"""
    if not file_id:
        return ""
    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w400"


# ── Delete File ──────────────────────────────────────────────
def delete_file_from_drive(file_id: str) -> bool:
    """ลบไฟล์จาก Drive (soft delete = ย้ายไป trash)"""
    try:
        service = _get_drive_service()
        service.files().update(
            fileId=file_id,
            body={"trashed": True}
        ).execute()
        return True
    except Exception:
        return False


# ── Validate File ────────────────────────────────────────────
def validate_uploaded_file(uploaded_file, max_mb: float = 10.0) -> tuple[bool, str]:
    """
    ตรวจสอบไฟล์ก่อนอัปโหลด
    Returns: (is_valid: bool, error_message: str)
    """
    if uploaded_file is None:
        return False, "ไม่มีไฟล์"

    # ตรวจ extension
    ext = os.path.splitext(uploaded_file.name)[-1].lower()
    allowed = [".jpg", ".jpeg", ".png", ".pdf"]
    if ext not in allowed:
        return False, f"ไฟล์ประเภท '{ext}' ไม่รองรับ — รองรับเฉพาะ JPG, PNG, PDF"

    # ตรวจขนาด
    uploaded_file.seek(0, 2)          # ไปท้ายไฟล์
    size_bytes = uploaded_file.tell()  # อ่านขนาด
    uploaded_file.seek(0)              # reset

    max_bytes = max_mb * 1024 * 1024
    if size_bytes > max_bytes:
        size_mb = size_bytes / 1024 / 1024
        return False, f"ไฟล์ขนาด {size_mb:.1f} MB เกินกำหนด {max_mb} MB"

    return True, ""
