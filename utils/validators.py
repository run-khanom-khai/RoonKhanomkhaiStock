def required(value, field_name: str) -> str | None:
    if not value or str(value).strip() == "":
        return f"กรุณากรอก {field_name}"
    return None


def positive_number(value, field_name: str) -> str | None:
    try:
        if float(value) < 0:
            return f"{field_name} ต้องเป็นตัวเลขที่มากกว่า 0"
    except (TypeError, ValueError):
        return f"{field_name} ต้องเป็นตัวเลข"
    return None
