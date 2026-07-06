"""
gdrive_upload.py  –  Google Drive Upload Helper
ถ้า Drive ไม่พร้อม → fallback เก็บ base64 ใน Sheets (จำกัด 200KB)
"""
import io
import os
import base64
import streamlit as st

HARDCODED_FOLDER_ID = "1x4ZGPnYXSa98qfBbsRAKiEoG6seA-Y2B"

MIME_MAP = {
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".pdf":  "application/pdf",
}

MAX_B64_BYTES = 40000  # ~30KB safe limit for Sheets cell


def _get_drive_service():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    SCOPES = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    except Exception:
        json_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "credentials.json"
        )
        from google.oauth2.service_account import Credentials as C
        creds = C.from_service_account_file(json_path, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _compress_image(file_bytes: bytes, max_bytes: int = 150000) -> bytes:
    """บีบอัดรูปภาพถ้าใหญ่เกินไป"""
    try:
        from PIL import Image
        if len(file_bytes) <= max_bytes:
            return file_bytes
        img = Image.open(io.BytesIO(file_bytes))
        # แปลงเป็น RGB ถ้าจำเป็น
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        # ลดขนาด
        quality = 85
        while quality > 20:
            buf = io.BytesIO()
            img.save(buf, format='JPEG', quality=quality, optimize=True)
            if buf.tell() <= max_bytes:
                return buf.getvalue()
            quality -= 15
            # ลดขนาดรูปด้วย
            w, h = img.size
            img = img.resize((int(w*0.7), int(h*0.7)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=20)
        return buf.getvalue()
    except Exception:
        return file_bytes


def upload_file_to_drive(uploaded_file, request_id: str = "", subfolder: str = "") -> dict:
    """
    อัปโหลดไฟล์ขึ้น Google Drive
    ถ้าไม่สำเร็จ → เก็บ base64 แทน
    Returns: {"file_id": str, "url": str, "name": str, "mime_type": str, "b64": str}
    """
    from googleapiclient.http import MediaIoBaseUpload

    uploaded_file.seek(0)
    file_bytes = uploaded_file.read()
    uploaded_file.seek(0)

    ext = os.path.splitext(uploaded_file.name)[-1].lower()
    mime_type = MIME_MAP.get(ext, "application/octet-stream")

    # ── ลอง Google Drive ก่อน ──────────────────────────────
    try:
        service   = _get_drive_service()
        folder_id = HARDCODED_FOLDER_ID
        try:
            folder_id = st.secrets["gdrive"]["folder_id"]
        except Exception:
            pass

        file_meta = {"name": uploaded_file.name, "parents": [folder_id]}
        media = MediaIoBaseUpload(
            io.BytesIO(file_bytes), mimetype=mime_type, resumable=False
        )
        uploaded = service.files().create(
            body=file_meta, media_body=media,
            fields="id, name, webViewLink"
        ).execute()
        file_id  = uploaded.get("id","")
        view_url = uploaded.get("webViewLink",
                                f"https://drive.google.com/file/d/{file_id}/view")
        # ตั้ง permission public
        try:
            service.permissions().create(
                fileId=file_id,
                body={"type":"anyone","role":"reader"}
            ).execute()
        except Exception:
            pass

        return {
            "file_id":   file_id,
            "url":       view_url,
            "name":      uploaded_file.name,
            "mime_type": mime_type,
            "b64":       "",
            "storage":   "gdrive",
        }

    except Exception as drive_err:
        # ── Fallback: เก็บ base64 ใน Sheets ──────────────
        # บีบอัดรูปก่อนถ้าเป็นรูปภาพ
        if ext in [".jpg",".jpeg",".png"]:
            file_bytes = _compress_image(file_bytes, max_bytes=30000)

        b64_data = base64.b64encode(file_bytes).decode("utf-8")

        if len(b64_data) > MAX_B64_BYTES:
            raise Exception(
                f"ไฟล์ขนาดใหญ่เกินไป ({len(file_bytes)//1024} KB) "
                f"กรุณาลดขนาดรูปให้น้อยกว่า 30KB ก่อนแนบครับ "
                f"(Drive error: {drive_err})"
            )

        return {
            "file_id":   f"b64_{request_id}_{uploaded_file.name}",
            "url":       "",
            "name":      uploaded_file.name,
            "mime_type": mime_type,
            "b64":       b64_data,
            "storage":   "base64",
        }


def get_file_url(file_id: str) -> str:
    if not file_id or file_id.startswith("b64_"):
        return ""
    return f"https://drive.google.com/file/d/{file_id}/view"


def get_thumbnail_url(file_id: str) -> str:
    if not file_id or file_id.startswith("b64_"):
        return ""
    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w400"


def delete_file_from_drive(file_id: str) -> bool:
    if not file_id or file_id.startswith("b64_"):
        return True
    try:
        service = _get_drive_service()
        service.files().update(fileId=file_id, body={"trashed":True}).execute()
        return True
    except Exception:
        return False


def validate_uploaded_file(uploaded_file, max_mb: float = 10.0) -> tuple:
    if uploaded_file is None:
        return False, "ไม่มีไฟล์"
    ext = os.path.splitext(uploaded_file.name)[-1].lower()
    if ext not in [".jpg",".jpeg",".png",".pdf"]:
        return False, f"ไม่รองรับไฟล์ประเภท '{ext}'"
    uploaded_file.seek(0, 2)
    size = uploaded_file.tell()
    uploaded_file.seek(0)
    if size > max_mb * 1024 * 1024:
        return False, f"ไฟล์ขนาด {size/1024/1024:.1f} MB เกินกำหนด {max_mb} MB"
    return True, ""
