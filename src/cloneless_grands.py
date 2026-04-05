#!/usr/bin/env python3
"""Automate Cloneless Grands map processing and club campaign publishing."""

from __future__ import annotations

import argparse
import base64
import copy
import datetime as dt
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import tempfile
from typing import Any
import xml.etree.ElementTree as ET
import zipfile

import requests

UBI_APP_ID = "86263886-327a-4328-ac69-527f0d20a237"
UBI_AUTH_URL = "https://public-ubiservices.ubi.com/v3/profiles/sessions"
CORE_BASE_URL = "https://prod.trackmania.core.nadeo.online"
LIVE_BASE_URL = "https://live-services.trackmania.nadeo.live"


DEFAULT_CONFIG: dict[str, Any] = {
    "env": {
        "load_dotenv": True,
        "dotenv_path": ".env",
        "override_existing_env": False,
    },
    "auth": {
        "mode": "service_account",
        "service_account_login_env": "TM_SERVICE_ACCOUNT_LOGIN",
        "service_account_password_env": "TM_SERVICE_ACCOUNT_PASSWORD",
        "email_env": "UBI_EMAIL",
        "password_env": "UBI_PASSWORD",
        "user_agent": "ClonelessGrandsBot/1.0 (contact:you@example.com)",
    },
    "weekly_grands": {
        "length": 1,
        "offset": 0,
    },
    "club": {
        "club_id": 130653,
        "folder_id": 0,
        "upsert_by_name": True,
        "activate": True,
        "public": True,
        "featured": False,
        "upload_activity_media_from_map_thumbnail": True,
        "media_theme": "",
        "activity_position": None,
    },
    "campaign": {
        "name_template": "w{week:02d} {source_map_name_clean}",
        "truncate_to_20": True,
    },
    "map": {
        "uid_prefix": "CLONELESS_",
        "name_template": "w{week:02d} {source_map_name_clean}",
        "variant_name_template": "{map_name_base} {lap_count}L",
        "author_login": "",
        "author_nickname_template": "{source_author_nickname}",
        "lap_variants": [1, 2, 3, 5, 10, 20, 30, 60, 120, 256],
        "transform_mode": "pure_python",
        "strip_validation_replay": False,
        "strip_exe": "tools/strip-validation/stripValidationReplay.exe",
        "strip_note": "Cloneless Grands automated processing",
        "allow_reuse_existing_uid": True,
        "uid_rewriter": {
            "mode": "internal_replace",
            "command_template": "",
        },
    },
    "club_bucket": {
        "enabled": False,
        "bucket_id": 0,
    },
    "club_background": {
        "enabled": True,
        "format": "background",
        "prefer_weekly_campaign_media": True,
    },
    "ordering": {
        "enabled": True,
        "pinned_activity_name": "Information",
        "pinned_activity_type": "news",
        "pinned_position": 0,
        "processed_campaign_position": 1,
    },
    "paths": {
        "work_dir": "work",
        "state_file": "work/state.json",
    },
    "state": {
        "skip_processed": True,
    },
    "http": {
        "timeout_sec": 45,
    },
}


class ConfigError(RuntimeError):
    pass


class ApiError(RuntimeError):
    pass


TM2020_STRIPPED_RACE_VALIDATE_GHOST = bytes.fromhex("0000000004000000ffffffff")
TM2020_HEADER_DESC_VERSION = 13
THUMBNAIL_START_MARKER = b"<Thumbnail.jpg>"
THUMBNAIL_END_MARKER = b"</Thumbnail.jpg>"
TM2020_HEADER_DESC_CHUNK_ID = 0x03043002
TM2020_HEADER_XML_CHUNK_ID = 0x03043005
TM2020_HEADER_THUMBNAIL_CHUNK_ID = 0x03043007
TM2020_HEADER_AUTHOR_CHUNK_ID = 0x03043008
TM2020_BODY_AUTHOR_CHUNK_ID = 0x03043042
TM2020_BODY_LAP_CHUNK_ID = 0x03043018
TM2020_BODY_CHALLENGE_CHUNK_ID = 0x03043011
TM2020_CP004_CHUNK_ID = 0x0305B004
TM2020_CP00A_CHUNK_ID = 0x0305B00A


def log(msg: str) -> None:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Config file does not exist: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON in config file {path}: {exc}") from exc


def save_json_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def parse_dotenv_line(line: str) -> tuple[str, str] | None:
    text = line.strip()
    if not text or text.startswith("#"):
        return None

    if text.startswith("export "):
        text = text[7:].strip()

    if "=" not in text:
        return None

    key, value = text.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None

    if value and value[0] in {"'", '"'} and value[-1:] == value[0]:
        quote = value[0]
        value = value[1:-1]
        if quote == '"':
            value = value.encode("utf-8").decode("unicode_escape")
    else:
        comment_at = value.find(" #")
        if comment_at != -1:
            value = value[:comment_at].rstrip()

    return key, value


def load_dotenv(path: Path, *, override: bool = False) -> int:
    if not path.exists():
        return 0

    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        parsed = parse_dotenv_line(raw_line)
        if not parsed:
            continue
        key, value = parsed
        if override or key not in os.environ:
            os.environ[key] = value
            loaded += 1
    return loaded


def as_int(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def sanitize_for_filename(text: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip())
    text = text.strip("._")
    return text[:80] or "item"


def clean_trackmania_name(text: str) -> str:
    stripped = re.sub(r"\$[0-9A-Fa-f]{1,3}", "", text)
    stripped = re.sub(r"\$[A-Za-z]", "", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    stripped = re.sub(r"^[A-Za-z0-9]{1,3}\s*-\s+", "", stripped)
    return stripped


def normalize_lap_variants(values: Any) -> list[int]:
    if not isinstance(values, list) or not values:
        raise ConfigError("map.lap_variants must be a non-empty list of integers.")

    result: list[int] = []
    seen: set[int] = set()
    for raw in values:
        lap_count = as_int(raw, -1)
        if lap_count <= 0:
            raise ConfigError(
                f"map.lap_variants contains invalid lap count: {raw!r}"
            )
        if lap_count in seen:
            continue
        seen.add(lap_count)
        result.append(lap_count)
    return result


def render_template(template: str, context: dict[str, Any]) -> str:
    try:
        return template.format(**context)
    except KeyError as exc:
        raise ConfigError(
            f"Missing template key '{exc.args[0]}' in: {template}"
        ) from exc


def build_prefixed_uid(source_uid: str, prefix: str) -> str:
    if source_uid.startswith(prefix):
        return source_uid
    if len(prefix) >= len(source_uid):
        raise ConfigError(
            f"UID prefix '{prefix}' (len {len(prefix)}) must be shorter than source UID '{source_uid}' (len {len(source_uid)})."
        )
    return prefix + source_uid[len(prefix) :]


def assert_ascii(text: str, label: str) -> None:
    try:
        text.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ConfigError(f"{label} must be ASCII. Got: {text!r}") from exc


def validate_config(cfg: dict[str, Any]) -> None:
    auth_mode = str(cfg["auth"].get("mode", "service_account")).strip().lower()
    if auth_mode not in {"service_account", "ubisoft"}:
        raise ConfigError("auth.mode must be 'service_account' or 'ubisoft'.")

    transform_mode = str(cfg["map"].get("transform_mode", "pure_python")).strip()
    if transform_mode not in {"pure_python", "legacy"}:
        raise ConfigError("map.transform_mode must be 'pure_python' or 'legacy'.")

    normalize_lap_variants(cfg["map"].get("lap_variants"))

    if transform_mode == "legacy":
        strip_exe = cfg["map"]["strip_exe"]
        if not strip_exe:
            raise ConfigError("map.strip_exe is required when transform_mode='legacy'.")

        strip_exe_path = Path(strip_exe)
        if not strip_exe_path.exists():
            raise ConfigError(f"strip executable not found: {strip_exe_path}")

    uid_prefix = cfg["map"]["uid_prefix"]
    if not uid_prefix:
        raise ConfigError("map.uid_prefix must not be empty.")
    assert_ascii(uid_prefix, "map.uid_prefix")
    author_login = str(cfg["map"].get("author_login", "")).strip()
    if author_login:
        assert_ascii(author_login, "map.author_login")

    mode = cfg["map"]["uid_rewriter"]["mode"]
    if mode not in {"internal_replace", "external_command"}:
        raise ConfigError(
            "map.uid_rewriter.mode must be 'internal_replace' or 'external_command'."
        )

    if (
        mode == "external_command"
        and not cfg["map"]["uid_rewriter"]["command_template"]
    ):
        raise ConfigError(
            "map.uid_rewriter.command_template is required when mode='external_command'."
        )

    club_media_format = str(cfg["club_background"]["format"]).strip().lower()
    if club_media_format not in {
        "background",
        "icon",
        "decal",
        "vertical",
        "screen8x1",
        "screen16x1",
        "screen16x9",
    }:
        raise ConfigError(
            "club_background.format must be one of: "
            "background, icon, decal, vertical, screen8x1, screen16x1, screen16x9."
        )

    pinned_position = as_int(cfg["ordering"]["pinned_position"], -1)
    if pinned_position < 0:
        raise ConfigError("ordering.pinned_position must be >= 0.")

    processed_campaign_position = as_int(
        cfg["ordering"]["processed_campaign_position"], -1
    )
    if processed_campaign_position < 0:
        raise ConfigError("ordering.processed_campaign_position must be >= 0.")


def run_command(args: list[str], *, shell: bool = False) -> None:
    log(f"Running command: {' '.join(args) if not shell else args[0]}")
    try:
        subprocess.run(args if not shell else args[0], check=True, shell=shell)
    except OSError as exc:
        raise RuntimeError(f"Command failed to start: {exc}") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Command failed with exit code {exc.returncode}") from exc


def close_zip_handles(obj: Any, seen: set[int] | None = None) -> None:
    if seen is None:
        seen = set()

    obj_id = id(obj)
    if obj_id in seen:
        return
    seen.add(obj_id)

    if isinstance(obj, zipfile.ZipFile):
        try:
            obj.close()
        except OSError:
            pass
        return

    if isinstance(obj, dict):
        for value in obj.values():
            close_zip_handles(value, seen)
        return

    if isinstance(obj, list):
        for value in obj:
            close_zip_handles(value, seen)


def import_gbxpy() -> tuple[Any, Any]:
    try:
        from gbxpy.parser import generate_file as gbx_generate_file
        from gbxpy.parser import parse_file as gbx_parse_file
    except ImportError as exc:
        raise RuntimeError(
            "Pure-Python map transform requires the 'construct' package. Run 'pip install -r requirements.txt'."
        ) from exc
    return gbx_parse_file, gbx_generate_file


def import_pillow() -> tuple[Any, Any, Any, Any]:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise RuntimeError(
            "Map thumbnail updates require Pillow. Run 'pip install -r requirements.txt'."
        ) from exc
    from io import BytesIO

    return Image, ImageDraw, ImageFont, BytesIO


def calculate_auto_medal_ms(author_ms: int, factor: float) -> int:
    raw = (author_ms * factor + 1000.0) / 1000.0
    return int(raw // 1) * 1000


def decode_jwt_payload(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload)
        parsed = json.loads(decoded)
    except (ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def scale_author_time_ms(
    source_author_ms: int, source_lap_count: int, target_lap_count: int
) -> int:
    if source_lap_count <= 0:
        raise RuntimeError(f"Invalid source lap count: {source_lap_count}")
    if target_lap_count <= 0:
        raise RuntimeError(f"Invalid target lap count: {target_lap_count}")
    return max(1, int(round(source_author_ms * target_lap_count / source_lap_count)))


def calculate_variant_has_clones(lap_count: int) -> int:
    return 0 if lap_count == 1 else 1


def build_variant_uid(
    source_uid: str,
    prefix: str,
    lap_count: int,
    source_lap_count: int,
) -> str:
    base_uid = build_prefixed_uid(source_uid, prefix)
    if lap_count == source_lap_count:
        return base_uid

    lap_tag = f"L{lap_count:03d}_"
    remaining = len(source_uid) - len(prefix) - len(lap_tag)
    if remaining <= 0:
        raise ConfigError(
            "UID generation needs more room for lap variants. "
            f"source_uid={source_uid!r}, prefix={prefix!r}, lap_tag={lap_tag!r}"
        )
    return prefix + lap_tag + source_uid[-remaining:]


def order_lap_variants(lap_variants: list[int], source_lap_count: int) -> list[int]:
    ordered = sorted(lap_variants)
    if source_lap_count in ordered:
        return [source_lap_count] + [
            lap_count for lap_count in ordered if lap_count != source_lap_count
        ]
    return ordered


def build_lap_variants(
    cfg: dict[str, Any],
    context: dict[str, Any],
    *,
    source_lap_count: int,
    source_author_time_ms: int,
    uid_overrides: dict[int, str] | None = None,
) -> list[dict[str, Any]]:
    lap_variants = order_lap_variants(
        normalize_lap_variants(cfg["map"]["lap_variants"]), source_lap_count
    )
    map_name_base = (
        render_template(cfg["map"]["name_template"], context).strip()
        or context["source_map_name"]
    )
    variant_name_template = str(
        cfg["map"].get("variant_name_template", "{map_name_base} {lap_count}L")
    )

    variants: list[dict[str, Any]] = []
    seen_uids: set[str] = set()
    target_author_login = str(
        context.get("target_author_login", context.get("source_author_login", ""))
    ).strip()
    target_author_nickname = str(
        context.get(
            "target_author_nickname", context.get("source_author_nickname", "")
        )
    ).strip()
    target_author_zone = str(context.get("source_author_zone", "")).strip()
    target_upload_author_account_id = str(
        context.get("target_upload_author_account_id", "")
    ).strip()
    for lap_count in lap_variants:
        is_default_variant = lap_count == source_lap_count
        author_time_ms = scale_author_time_ms(
            source_author_time_ms, source_lap_count, lap_count
        )
        variant_context = {
            **context,
            "lap_count": lap_count,
            "lap_suffix": "" if is_default_variant else f"{lap_count}L",
            "map_name_base": map_name_base,
            "source_lap_count": source_lap_count,
            "author_time_ms": author_time_ms,
        }
        variant_uid = (
            str(uid_overrides.get(lap_count, "")).strip() if uid_overrides else ""
        ) or build_variant_uid(
            context["source_map_uid"],
            cfg["map"]["uid_prefix"],
            lap_count,
            source_lap_count,
        )
        if variant_uid in seen_uids:
            raise RuntimeError(
                f"Duplicate variant UID detected for source {context['source_map_uid']}: {variant_uid}"
            )
        seen_uids.add(variant_uid)
        if is_default_variant:
            map_name = map_name_base
        else:
            map_name = render_template(variant_name_template, variant_context).strip()
            if not map_name:
                map_name = f"{map_name_base} {lap_count}L"

        variants.append(
            {
                "lap_count": lap_count,
                "map_uid": variant_uid,
                "map_name": map_name,
                "is_default_variant": is_default_variant,
                "thumbnail_title": map_name_base if is_default_variant else build_thumbnail_label(lap_count),
                "thumbnail_badge_text": "" if is_default_variant else f"{lap_count}L",
                "author_time_ms": author_time_ms,
                "gold_time_ms": calculate_auto_medal_ms(author_time_ms, 1.06),
                "silver_time_ms": calculate_auto_medal_ms(author_time_ms, 1.20),
                "bronze_time_ms": calculate_auto_medal_ms(author_time_ms, 1.50),
                "author_login": target_author_login,
                "author_nickname": target_author_nickname,
                "author_zone": target_author_zone,
                "upload_author_account_id": target_upload_author_account_id,
                "has_clones": calculate_variant_has_clones(lap_count),
            }
        )

    return variants


def parse_tm2020_header_desc_chunk(raw: bytes) -> tuple[int, list[int]]:
    if len(raw) != 57:
        raise RuntimeError(
            f"Unsupported TM2020 header desc chunk length: expected 57, got {len(raw)}"
        )
    version = raw[0]
    if version != TM2020_HEADER_DESC_VERSION:
        raise RuntimeError(
            "Unsupported TM2020 header desc version: "
            f"expected {TM2020_HEADER_DESC_VERSION}, got {version}"
        )
    values = list(struct.unpack("<14i", raw[1:]))
    return version, values


def build_tm2020_header_desc_chunk(version: int, values: list[int]) -> bytes:
    if version != TM2020_HEADER_DESC_VERSION:
        raise RuntimeError(
            "Unsupported TM2020 header desc version while rebuilding: "
            f"{version}"
        )
    if len(values) != 14:
        raise RuntimeError(
            f"TM2020 header desc chunk expects 14 ints, got {len(values)}"
        )
    return bytes([version]) + struct.pack("<14i", *values)


def parse_tm2020_header_author_chunk(raw: bytes) -> dict[str, Any]:
    if len(raw) < 20:
        raise RuntimeError(
            f"Unsupported TM2020 header author chunk length: expected >= 20, got {len(raw)}"
        )

    version, reserved = struct.unpack("<2I", raw[:8])
    offset = 8
    values: list[str] = []
    for label in ("author_login", "author_nickname", "author_zone"):
        if offset + 4 > len(raw):
            raise RuntimeError(
                f"Header author chunk ended before {label} length could be read."
            )
        length = struct.unpack("<I", raw[offset : offset + 4])[0]
        offset += 4
        if offset + length > len(raw):
            raise RuntimeError(
                f"Header author chunk ended before {label} payload could be read."
            )
        values.append(raw[offset : offset + length].decode("utf-8"))
        offset += length

    return {
        "version": version,
        "reserved": reserved,
        "author_login": values[0],
        "author_nickname": values[1],
        "author_zone": values[2],
        "trailer": raw[offset:],
    }


def build_tm2020_header_author_chunk(payload: dict[str, Any]) -> bytes:
    parts = [
        struct.pack(
            "<2I",
            as_int(payload.get("version"), 1),
            as_int(payload.get("reserved"), 0),
        )
    ]
    for key in ("author_login", "author_nickname", "author_zone"):
        encoded = str(payload.get(key, "")).encode("utf-8")
        parts.append(struct.pack("<I", len(encoded)))
        parts.append(encoded)
    parts.append(bytes(payload.get("trailer", b"")))
    return b"".join(parts)


def build_target_author_metadata(
    cfg: dict[str, Any],
    context: dict[str, Any],
    seed: dict[str, Any],
    *,
    upload_author_account_id: str,
) -> dict[str, str]:
    source_author_login = str(seed.get("source_author_login", "")).strip()
    source_author_nickname = str(seed.get("source_author_nickname", "")).strip()
    source_author_zone = str(seed.get("source_author_zone", "")).strip()
    target_author_login = str(cfg["map"].get("author_login", "")).strip()
    if not target_author_login:
        target_author_login = source_author_login

    nickname_template = str(
        cfg["map"].get("author_nickname_template", "{source_author_nickname}")
    )
    template_context = {
        **context,
        "source_author_login": source_author_login,
        "source_author_nickname": source_author_nickname,
        "source_author_zone": source_author_zone,
        "original_author_login": source_author_login,
        "original_author_nickname": source_author_nickname,
        "original_author_zone": source_author_zone,
    }
    target_author_nickname = (
        render_template(nickname_template, template_context).strip()
        if nickname_template
        else source_author_nickname
    )
    if not target_author_nickname:
        target_author_nickname = source_author_nickname or target_author_login

    return {
        "source_author_login": source_author_login,
        "source_author_nickname": source_author_nickname,
        "source_author_zone": source_author_zone,
        "target_author_login": target_author_login,
        "target_author_nickname": target_author_nickname,
        "target_upload_author_account_id": upload_author_account_id,
    }


def read_map_variant_seed(source_file: Path) -> dict[str, Any]:
    parse_file, _ = import_gbxpy()
    data = parse_file(str(source_file), recursive=False)
    try:
        header_desc_raw = bytes(data["header"]["data"][0])
        _, header_desc_values = parse_tm2020_header_desc_chunk(header_desc_raw)
        header_author = parse_tm2020_header_author_chunk(bytes(data["header"]["data"][5]))
        body_laps_raw = data["body"].get(0x03043018)
        if not body_laps_raw or "_unknownChunkId" not in body_laps_raw:
            raise RuntimeError("Could not locate TM2020 lap chunk (0x03043018).")
        body_lap_values = struct.unpack("<2i", bytes(body_laps_raw["_unknownChunkId"]))
        xml_root = ET.fromstring(str(data["header"]["data"][3]["xml"]))
        times = xml_root.find("times")
        desc = xml_root.find("desc")
        if times is None or desc is None:
            raise RuntimeError("Map XML does not contain <times> or <desc>.")

        return {
            "source_author_time_ms": int(times.attrib["authortime"]),
            "source_lap_count": int(desc.attrib["nblaps"]),
            "header_author_time_ms": header_desc_values[4],
            "header_lap_count": header_desc_values[13],
            "body_lap_count": body_lap_values[1],
            "source_author_login": str(header_author["author_login"]),
            "source_author_nickname": str(header_author["author_nickname"]),
            "source_author_zone": str(header_author["author_zone"]),
        }
    finally:
        close_zip_handles(data)


def extract_thumbnail_jpeg(raw_chunk: bytes) -> tuple[int, bytes, bytes]:
    if len(raw_chunk) < 8:
        raise RuntimeError("Thumbnail chunk is too small to parse.")
    version, jpeg_size = struct.unpack("<2i", raw_chunk[:8])
    start = raw_chunk.find(THUMBNAIL_START_MARKER)
    end = raw_chunk.find(THUMBNAIL_END_MARKER)
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Thumbnail markers are missing from the map header chunk.")
    jpeg_start = start + len(THUMBNAIL_START_MARKER)
    jpeg_bytes = raw_chunk[jpeg_start:end]
    if jpeg_size != len(jpeg_bytes):
        raise RuntimeError(
            "Thumbnail chunk JPEG size mismatch: "
            f"declared={jpeg_size} actual={len(jpeg_bytes)}"
        )
    return version, jpeg_bytes, raw_chunk[end:]


def build_thumbnail_chunk(version: int, jpeg_bytes: bytes, suffix: bytes) -> bytes:
    return struct.pack("<2i", version, len(jpeg_bytes)) + THUMBNAIL_START_MARKER + jpeg_bytes + suffix


def build_thumbnail_label(lap_count: int) -> str:
    return f"{lap_count} LAP" if lap_count == 1 else f"{lap_count} LAPS"


def render_variant_thumbnail(jpeg_bytes: bytes, variant: dict[str, Any]) -> bytes:
    Image, ImageDraw, ImageFont, BytesIO = import_pillow()
    lap_count = as_int(variant.get("lap_count"), -1)

    with Image.open(BytesIO(jpeg_bytes)) as image:
        image = image.convert("RGB")
        flip_top_bottom = (
            Image.Transpose.FLIP_TOP_BOTTOM
            if hasattr(Image, "Transpose")
            else Image.FLIP_TOP_BOTTOM
        )
        display_image = image.transpose(flip_top_bottom)
        draw = ImageDraw.Draw(display_image, "RGBA")
        width, height = display_image.size
        band_height = max(82, height // 7 + 12)
        top = height - band_height
        draw.rectangle((0, top, width, height), fill=(12, 12, 12, 180))

        label = str(variant.get("thumbnail_title", "")).strip() or build_thumbnail_label(
            lap_count
        )
        subtitle = "CLONELESS GRANDS"
        font_candidates = [
            ("arialbd.ttf", max(28, height // 11)),
            ("arial.ttf", max(28, height // 11)),
            ("DejaVuSans-Bold.ttf", max(28, height // 11)),
        ]
        subtitle_candidates = [
            ("arial.ttf", max(14, height // 24)),
            ("DejaVuSans.ttf", max(14, height // 24)),
        ]

        title_font = None
        for font_name, font_size in font_candidates:
            try:
                title_font = ImageFont.truetype(font_name, font_size)
                break
            except OSError:
                continue
        if title_font is None:
            title_font = ImageFont.load_default()

        subtitle_font = None
        for font_name, font_size in subtitle_candidates:
            try:
                subtitle_font = ImageFont.truetype(font_name, font_size)
                break
            except OSError:
                continue
        if subtitle_font is None:
            subtitle_font = ImageFont.load_default()

        title_bbox = draw.textbbox((0, 0), label, font=title_font)
        title_width = title_bbox[2] - title_bbox[0]
        title_height = title_bbox[3] - title_bbox[1]
        subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
        subtitle_height = subtitle_bbox[3] - subtitle_bbox[1]
        x = 44
        base_y = top + max(8, (band_height - title_height - subtitle_height - 8) // 2)
        title_y = max(top + 4, base_y - 16)
        subtitle_y = max(top + 30, base_y + title_height + 20)

        draw.text((x, title_y), label, font=title_font, fill=(255, 255, 255, 255))
        draw.text(
            (x, subtitle_y),
            subtitle,
            font=subtitle_font,
            fill=(230, 230, 230, 255),
        )

        badge_text = str(variant.get("thumbnail_badge_text", "")).strip()
        if badge_text and title_width < width - 48:
            badge_font = subtitle_font
            badge_bbox = draw.textbbox((0, 0), badge_text, font=badge_font)
            badge_width = badge_bbox[2] - badge_bbox[0]
            badge_height = badge_bbox[3] - badge_bbox[1]
            badge_padding_x = 16
            badge_padding_y = 10
            badge_total_width = badge_width + badge_padding_x * 2
            badge_half_width = badge_total_width // 2
            badge_shift_left = int(round(badge_total_width * 1.3))
            badge_center_x = int(width * 0.80) - badge_shift_left
            badge_right_margin = width - 24
            badge_right = min(
                badge_right_margin,
                badge_center_x + badge_half_width,
            )
            badge_left = badge_right - badge_total_width
            badge_shift_right = max(0, (badge_right_margin - badge_right) // 2)
            badge_left += badge_shift_right
            badge_right += badge_shift_right
            badge_top = top + max(
                8, (band_height - badge_height - badge_padding_y * 2) // 2 - 4
            )
            badge_bottom = badge_top + badge_height + badge_padding_y * 2
            draw.rounded_rectangle(
                (badge_left, badge_top, badge_right, badge_bottom),
                radius=18,
                fill=(255, 255, 255, 230),
            )
            draw.text(
                (badge_left + badge_padding_x, badge_top + badge_padding_y - 10),
                badge_text,
                font=badge_font,
                fill=(20, 20, 20, 255),
            )

        out = BytesIO()
        display_image.transpose(flip_top_bottom).save(
            out, format="JPEG", quality=92, optimize=True
        )
        return out.getvalue()


def render_activity_media_thumbnail(jpeg_bytes: bytes) -> bytes:
    Image, _, _, BytesIO = import_pillow()

    with Image.open(BytesIO(jpeg_bytes)) as image:
        image = image.convert("RGB")
        flip_top_bottom = (
            Image.Transpose.FLIP_TOP_BOTTOM
            if hasattr(Image, "Transpose")
            else Image.FLIP_TOP_BOTTOM
        )
        out = BytesIO()
        image.transpose(flip_top_bottom).save(
            out, format="JPEG", quality=92, optimize=True
        )
        return out.getvalue()


def strip_validation_ghost_pure_python(data: Any) -> None:
    challenge_chunk = data["body"].get(0x03043011)
    if not challenge_chunk or "challengeParameters" not in challenge_chunk:
        raise RuntimeError("Could not locate challengeParameters chunk in map body.")

    challenge_parameters = challenge_chunk["challengeParameters"]
    body = challenge_parameters.get("body")
    if not isinstance(body, dict):
        raise RuntimeError("challengeParameters body is missing or malformed.")

    ghost_chunk = body.get(0x0305B00F)
    if ghost_chunk and "_unknownChunkId" in ghost_chunk:
        ghost_chunk["_unknownChunkId"] = TM2020_STRIPPED_RACE_VALIDATE_GHOST
        return

    legacy_chunk = body.get(0x0305B00D)
    if legacy_chunk and "raceValidateGhost" in legacy_chunk:
        legacy_chunk["raceValidateGhost"] = {"_index": -1}
        return

    raise RuntimeError("Could not locate a writable race-validation-ghost chunk.")


def create_stripped_map_pure_python(source_file: Path, stripped_file: Path) -> None:
    parse_file, generate_file = import_gbxpy()
    stripped_file.parent.mkdir(parents=True, exist_ok=True)

    data = parse_file(str(source_file), recursive=False)
    try:
        strip_validation_ghost_pure_python(data)
        stripped_file.write_bytes(generate_file(data))
    finally:
        close_zip_handles(data)


def build_gbxjsoneditor_variant_file(
    source_file: Path,
    output_file: Path,
    *,
    variant: dict[str, Any],
) -> None:
    repo_root = Path(__file__).resolve().parent.parent
    exe_path = repo_root / "tools" / "gbx-json-editor" / "GbxJsonEditor.Cli.exe"
    if not exe_path.exists():
        raise RuntimeError(f"GbxJsonEditor executable not found: {exe_path}")

    payload = {
        "type": "Map",
        "outputSuffix": "varianttmp",
        "operations": [
            {"op": "set", "path": "MapUid", "value": str(variant["map_uid"])},
            {"op": "set", "path": "MapName", "value": str(variant["map_name"])},
            {"op": "set", "path": "NbLaps", "value": as_int(variant["lap_count"], -1)},
            {
                "op": "set",
                "path": "HasClones",
                "value": bool(as_int(variant.get("has_clones"), 0)),
            },
        ],
    }
    author_login = str(variant.get("author_login", "")).strip()
    if author_login:
        payload["operations"].append(
            {"op": "set", "path": "AuthorLogin", "value": author_login}
        )
        payload["operations"].append(
            {"op": "set", "path": "MapInfo.Author", "value": author_login}
        )
    author_nickname = str(variant.get("author_nickname", "")).strip()
    if author_nickname:
        payload["operations"].append(
            {"op": "set", "path": "AuthorNickname", "value": author_nickname}
        )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as temp_file:
        temp_file.write(json.dumps(payload))
        instructions_path = Path(temp_file.name)

    generated_path = source_file.with_name(
        source_file.name.replace(".Map.Gbx", "_varianttmp.Map.Gbx")
    )
    try:
        run_command([str(exe_path), str(source_file), str(instructions_path), "varianttmp"])
        if not generated_path.exists():
            raise RuntimeError(f"Expected generated file not found: {generated_path}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(generated_path), str(output_file))
    finally:
        instructions_path.unlink(missing_ok=True)
        generated_path.unlink(missing_ok=True)


def parse_header_chunk_entries(blob: bytes) -> tuple[int, int, list[dict[str, Any]]]:
    header_size = struct.unpack_from("<I", blob, 13)[0]
    header_start = 17
    num_entries = struct.unpack_from("<I", blob, header_start)[0]
    entries_start = header_start + 4
    data_start = entries_start + num_entries * 8

    entries: list[dict[str, Any]] = []
    chunk_offset = data_start
    for index in range(num_entries):
        chunk_id = struct.unpack_from("<I", blob, entries_start + index * 8)[0]
        meta = int.from_bytes(
            blob[entries_start + index * 8 + 4 : entries_start + index * 8 + 8],
            "little",
        )
        size = meta & 0x7FFFFFFF
        heavy = bool(meta & 0x80000000)
        entries.append(
            {
                "index": index,
                "chunk_id": chunk_id,
                "size": size,
                "heavy": heavy,
                "offset": chunk_offset,
            }
        )
        chunk_offset += size

    return header_size, entries_start, entries


def replace_header_chunk_bytes(
    blob: bytes,
    *,
    chunk_id: int,
    new_chunk_bytes: bytes,
) -> bytes:
    header_size, entries_start, entries = parse_header_chunk_entries(blob)
    target_entry = next(
        (entry for entry in entries if entry["chunk_id"] == chunk_id), None
    )
    if target_entry is None:
        raise RuntimeError(f"Header chunk not found: 0x{chunk_id:08X}")

    old_size = int(target_entry["size"])
    chunk_offset = int(target_entry["offset"])
    meta = (len(new_chunk_bytes) & 0x7FFFFFFF) | (
        0x80000000 if bool(target_entry["heavy"]) else 0
    )

    mutable = bytearray(blob)
    mutable[
        entries_start + int(target_entry["index"]) * 8 + 4 : entries_start + int(target_entry["index"]) * 8 + 8
    ] = meta.to_bytes(4, "little")
    struct.pack_into("<I", mutable, 13, header_size - old_size + len(new_chunk_bytes))

    return (
        bytes(mutable[:chunk_offset])
        + new_chunk_bytes
        + bytes(mutable[chunk_offset + old_size :])
    )


def patch_header_desc_bytes(blob: bytes, *, variant: dict[str, Any]) -> bytes:
    header_size, entries_start, entries = parse_header_chunk_entries(blob)
    target_entry = next(
        (
            entry
            for entry in entries
            if entry["chunk_id"] == TM2020_HEADER_DESC_CHUNK_ID
        ),
        None,
    )
    if target_entry is None:
        raise RuntimeError("Could not find header desc chunk (0x03043002).")

    chunk_offset = int(target_entry["offset"])
    old_chunk = blob[chunk_offset : chunk_offset + int(target_entry["size"])]
    version, values = parse_tm2020_header_desc_chunk(old_chunk)
    values[1] = as_int(variant["bronze_time_ms"])
    values[2] = as_int(variant["silver_time_ms"])
    values[3] = as_int(variant["gold_time_ms"])
    values[4] = as_int(variant["author_time_ms"])
    values[6] = 1
    values[13] = as_int(variant["lap_count"])
    new_chunk = build_tm2020_header_desc_chunk(version, values)

    if len(new_chunk) != len(old_chunk):
        raise RuntimeError("Header desc chunk size changed unexpectedly.")

    return blob.replace(old_chunk, new_chunk, 1)


def patch_header_xml_bytes(blob: bytes, *, variant: dict[str, Any]) -> bytes:
    _, _, entries = parse_header_chunk_entries(blob)
    target_entry = next(
        (
            entry
            for entry in entries
            if entry["chunk_id"] == TM2020_HEADER_XML_CHUNK_ID
        ),
        None,
    )
    if target_entry is None:
        raise RuntimeError("Could not find header XML chunk (0x03043005).")

    from gbxpy.gbx_structs import header_chunks

    chunk_offset = int(target_entry["offset"])
    old_chunk = blob[chunk_offset : chunk_offset + int(target_entry["size"])]
    xml_payload = header_chunks[TM2020_HEADER_XML_CHUNK_ID].parse(old_chunk)
    xml_root = ET.fromstring(str(xml_payload["xml"]))
    ident = xml_root.find("ident")
    desc = xml_root.find("desc")
    times = xml_root.find("times")
    if ident is None or desc is None or times is None:
        raise RuntimeError("Map XML does not contain ident/desc/times nodes.")

    ident.set("uid", str(variant["map_uid"]))
    ident.set("name", str(variant["map_name"]))
    author_login = str(variant.get("author_login", "")).strip()
    if author_login:
        ident.set("author", author_login)
    author_zone = str(variant.get("author_zone", "")).strip()
    if author_zone:
        ident.set("authorzone", author_zone)
    desc.set("nblaps", str(as_int(variant["lap_count"], -1)))
    times.set("bronze", str(as_int(variant["bronze_time_ms"], -1)))
    times.set("silver", str(as_int(variant["silver_time_ms"], -1)))
    times.set("gold", str(as_int(variant["gold_time_ms"], -1)))
    times.set("authortime", str(as_int(variant["author_time_ms"], -1)))
    times.set(
        "hasclones",
        str(calculate_variant_has_clones(as_int(variant["lap_count"], -1))),
    )

    new_xml = ET.tostring(xml_root, encoding="unicode")
    new_chunk = header_chunks[TM2020_HEADER_XML_CHUNK_ID].build({"xml": new_xml})
    return replace_header_chunk_bytes(
        blob,
        chunk_id=TM2020_HEADER_XML_CHUNK_ID,
        new_chunk_bytes=new_chunk,
    )


def patch_header_thumbnail_bytes(blob: bytes, *, variant: dict[str, Any]) -> bytes:
    _, _, entries = parse_header_chunk_entries(blob)
    target_entry = next(
        (
            entry
            for entry in entries
            if entry["chunk_id"] == TM2020_HEADER_THUMBNAIL_CHUNK_ID
        ),
        None,
    )
    if target_entry is None:
        raise RuntimeError("Could not find header thumbnail chunk (0x03043007).")

    chunk_offset = int(target_entry["offset"])
    old_chunk = blob[chunk_offset : chunk_offset + int(target_entry["size"])]
    thumbnail_version, thumbnail_jpeg, thumbnail_suffix = extract_thumbnail_jpeg(
        old_chunk
    )
    variant_thumbnail_jpeg = render_variant_thumbnail(thumbnail_jpeg, variant)
    new_chunk = build_thumbnail_chunk(
        thumbnail_version, variant_thumbnail_jpeg, thumbnail_suffix
    )
    return replace_header_chunk_bytes(
        blob,
        chunk_id=TM2020_HEADER_THUMBNAIL_CHUNK_ID,
        new_chunk_bytes=new_chunk,
    )


def patch_header_author_bytes(blob: bytes, *, variant: dict[str, Any]) -> bytes:
    _, _, entries = parse_header_chunk_entries(blob)
    target_entry = next(
        (
            entry
            for entry in entries
            if entry["chunk_id"] == TM2020_HEADER_AUTHOR_CHUNK_ID
        ),
        None,
    )
    if target_entry is None:
        raise RuntimeError("Could not find header author chunk (0x03043008).")

    chunk_offset = int(target_entry["offset"])
    old_chunk = blob[chunk_offset : chunk_offset + int(target_entry["size"])]
    payload = parse_tm2020_header_author_chunk(old_chunk)
    author_login = str(variant.get("author_login", "")).strip()
    if author_login:
        payload["author_login"] = author_login
    author_nickname = str(variant.get("author_nickname", "")).strip()
    if author_nickname:
        payload["author_nickname"] = author_nickname
    author_zone = str(variant.get("author_zone", "")).strip()
    if author_zone:
        payload["author_zone"] = author_zone
    new_chunk = build_tm2020_header_author_chunk(payload)
    return replace_header_chunk_bytes(
        blob,
        chunk_id=TM2020_HEADER_AUTHOR_CHUNK_ID,
        new_chunk_bytes=new_chunk,
    )


def get_body_layout(blob: bytes) -> tuple[int, int, int, int, int]:
    header_size = struct.unpack_from("<I", blob, 13)[0]
    pos = 17 + header_size
    pos += 4
    num_external_nodes = struct.unpack_from("<I", blob, pos)[0]
    pos += 4
    if num_external_nodes != 0:
        raise RuntimeError(
            f"Unsupported external node count while patching body: {num_external_nodes}"
        )
    uncompressed_size_offset = pos
    uncompressed_size = struct.unpack_from("<I", blob, pos)[0]
    pos += 4
    compressed_size_offset = pos
    compressed_size = struct.unpack_from("<I", blob, pos)[0]
    pos += 4
    body_offset = pos
    return (
        uncompressed_size_offset,
        compressed_size_offset,
        body_offset,
        uncompressed_size,
        compressed_size,
    )


def patch_body_medal_time_bytes(
    map_file: Path,
    *,
    variant: dict[str, Any],
) -> bytes:
    from gbxpy import mini_lzo
    from gbxpy.gbx_structs import body_chunks

    parse_file, _ = import_gbxpy()
    data = parse_file(str(map_file), recursive=False)
    try:
        old_header_desc = bytes(data["header"]["data"][0])
        version, header_desc_values = parse_tm2020_header_desc_chunk(old_header_desc)
        header_desc_values[1] = as_int(variant["bronze_time_ms"], -1)
        header_desc_values[2] = as_int(variant["silver_time_ms"], -1)
        header_desc_values[3] = as_int(variant["gold_time_ms"], -1)
        header_desc_values[4] = as_int(variant["author_time_ms"], -1)
        header_desc_values[6] = 1
        header_desc_values[13] = as_int(variant["lap_count"], -1)
        new_header_desc = build_tm2020_header_desc_chunk(version, header_desc_values)

        challenge_parameters = data["body"][TM2020_BODY_CHALLENGE_CHUNK_ID][
            "challengeParameters"
        ]["body"]
        cp004 = challenge_parameters[TM2020_CP004_CHUNK_ID]
        cp00a = challenge_parameters[TM2020_CP00A_CHUNK_ID]
        cp004_start, cp004_end = cp004["_iopos"]
        cp00a_start, cp00a_end = cp00a["_iopos"]

        cp004_new = body_chunks[TM2020_CP004_CHUNK_ID].build(
            {
                "bronzeTime": as_int(variant["bronze_time_ms"], -1),
                "silverTime": as_int(variant["silver_time_ms"], -1),
                "goldTime": as_int(variant["gold_time_ms"], -1),
                "authorTime": as_int(variant["author_time_ms"], -1),
                "u01": cp004["u01"],
            }
        )
        cp00a_new = body_chunks[TM2020_CP00A_CHUNK_ID].build(
            {
                "tip": cp00a["tip"],
                "bronzeTime": as_int(variant["bronze_time_ms"], -1),
                "silverTime": as_int(variant["silver_time_ms"], -1),
                "goldTime": as_int(variant["gold_time_ms"], -1),
                "authorTime": as_int(variant["author_time_ms"], -1),
                "timeLimit": cp00a["timeLimit"],
                "authorScore": cp00a["authorScore"],
            }
        )
    finally:
        close_zip_handles(data)

    blob = map_file.read_bytes()
    if blob.count(old_header_desc) != 1:
        raise RuntimeError("Expected to find the header desc chunk bytes exactly once.")
    blob = blob.replace(old_header_desc, new_header_desc, 1)

    (
        uncompressed_size_offset,
        compressed_size_offset,
        body_offset,
        uncompressed_size,
        compressed_size,
    ) = get_body_layout(blob)

    body = bytearray(
        mini_lzo.decompress(
            blob[body_offset : body_offset + compressed_size], uncompressed_size
        )
    )
    if cp004_end - cp004_start != len(cp004_new):
        raise RuntimeError("ChallengeParameters chunk 0x0305B004 size changed.")
    if cp00a_end - cp00a_start != len(cp00a_new):
        raise RuntimeError("ChallengeParameters chunk 0x0305B00A size changed.")

    body[cp004_start:cp004_end] = cp004_new
    body[cp00a_start:cp00a_end] = cp00a_new

    compressed_body = mini_lzo.compress(bytes(body))
    mutable = bytearray(blob)
    struct.pack_into("<I", mutable, uncompressed_size_offset, len(body))
    struct.pack_into("<I", mutable, compressed_size_offset, len(compressed_body))
    return bytes(mutable[:body_offset]) + compressed_body


def create_variant_map_safe(
    source_file: Path,
    output_file: Path,
    *,
    variant: dict[str, Any],
) -> None:
    build_gbxjsoneditor_variant_file(source_file, output_file, variant=variant)

    blob = patch_body_medal_time_bytes(output_file, variant=variant)
    blob = patch_header_xml_bytes(blob, variant=variant)
    blob = patch_header_author_bytes(blob, variant=variant)
    blob = patch_header_thumbnail_bytes(blob, variant=variant)
    output_file.write_bytes(blob)


def apply_variant_metadata_pure_python(
    data: Any,
    *,
    source_uid: str,
    source_map_name: str,
    variant: dict[str, Any],
) -> None:
    new_uid = str(variant["map_uid"])
    map_name = str(variant["map_name"])
    lap_count = as_int(variant["lap_count"], -1)
    author_time_ms = as_int(variant["author_time_ms"], -1)
    gold_time_ms = as_int(variant["gold_time_ms"], -1)
    silver_time_ms = as_int(variant["silver_time_ms"], -1)
    bronze_time_ms = as_int(variant["bronze_time_ms"], -1)
    author_login = str(variant.get("author_login", "")).strip()
    author_nickname = str(variant.get("author_nickname", "")).strip()
    author_zone = str(variant.get("author_zone", "")).strip()

    header_common = data["header"]["data"][1]
    body_map = data["body"].get(0x0304301F)
    if not isinstance(header_common, dict) or not isinstance(body_map, dict):
        raise RuntimeError("Could not locate TM2020 map header/body metadata chunks.")

    header_map_info = header_common.get("mapInfo")
    body_map_info = body_map.get("mapInfo")
    if not isinstance(header_map_info, dict) or not isinstance(body_map_info, dict):
        raise RuntimeError("Could not locate TM2020 mapInfo structures.")

    if header_map_info.get("id") != source_uid or body_map_info.get("id") != source_uid:
        raise RuntimeError(
            "Refusing to rewrite map UID because stripped source metadata no longer "
            f"matches {source_uid!r}."
        )

    header_map_info["id"] = new_uid
    body_map_info["id"] = new_uid
    if author_login:
        header_map_info["author"] = author_login
        body_map_info["author"] = author_login
    header_common["mapName"] = map_name
    body_map["mapName"] = map_name

    header_desc_raw = bytes(data["header"]["data"][0])
    version, header_desc_values = parse_tm2020_header_desc_chunk(header_desc_raw)
    header_desc_values[1] = bronze_time_ms
    header_desc_values[2] = silver_time_ms
    header_desc_values[3] = gold_time_ms
    header_desc_values[4] = author_time_ms
    header_desc_values[6] = 1
    header_desc_values[13] = lap_count
    data["header"]["data"][0] = build_tm2020_header_desc_chunk(
        version, header_desc_values
    )

    thumbnail_chunk = bytes(data["header"]["data"][4])
    thumbnail_version, thumbnail_jpeg, thumbnail_suffix = extract_thumbnail_jpeg(
        thumbnail_chunk
    )
    variant_thumbnail_jpeg = render_variant_thumbnail(thumbnail_jpeg, variant)
    data["header"]["data"][4] = build_thumbnail_chunk(
        thumbnail_version,
        variant_thumbnail_jpeg,
        thumbnail_suffix,
    )
    author_chunk = parse_tm2020_header_author_chunk(bytes(data["header"]["data"][5]))
    if author_login:
        author_chunk["author_login"] = author_login
    if author_nickname:
        author_chunk["author_nickname"] = author_nickname
    if author_zone:
        author_chunk["author_zone"] = author_zone
    data["header"]["data"][5] = build_tm2020_header_author_chunk(author_chunk)

    body_author_chunk = data["body"].get(TM2020_BODY_AUTHOR_CHUNK_ID)
    if body_author_chunk and "_unknownChunkId" in body_author_chunk:
        body_author = parse_tm2020_header_author_chunk(
            bytes(body_author_chunk["_unknownChunkId"])
        )
        if author_login:
            body_author["author_login"] = author_login
        if author_nickname:
            body_author["author_nickname"] = author_nickname
        if author_zone:
            body_author["author_zone"] = author_zone
        body_author_chunk["_unknownChunkId"] = build_tm2020_header_author_chunk(
            body_author
        )

    lap_chunk = data["body"].get(0x03043018)
    if not lap_chunk or "_unknownChunkId" not in lap_chunk:
        raise RuntimeError("Could not locate TM2020 lap chunk (0x03043018).")
    lap_chunk["_unknownChunkId"] = struct.pack("<2i", 1, lap_count)

    challenge_chunk = data["body"].get(0x03043011)
    if not challenge_chunk or "challengeParameters" not in challenge_chunk:
        raise RuntimeError("Could not locate challengeParameters chunk in map body.")
    challenge_parameters = challenge_chunk["challengeParameters"].get("body")
    if not isinstance(challenge_parameters, dict):
        raise RuntimeError("challengeParameters body is missing or malformed.")

    if 0x0305B004 in challenge_parameters:
        challenge_parameters[0x0305B004]["bronzeTime"] = bronze_time_ms
        challenge_parameters[0x0305B004]["silverTime"] = silver_time_ms
        challenge_parameters[0x0305B004]["goldTime"] = gold_time_ms
        challenge_parameters[0x0305B004]["authorTime"] = author_time_ms
    if 0x0305B00A in challenge_parameters:
        challenge_parameters[0x0305B00A]["bronzeTime"] = bronze_time_ms
        challenge_parameters[0x0305B00A]["silverTime"] = silver_time_ms
        challenge_parameters[0x0305B00A]["goldTime"] = gold_time_ms
        challenge_parameters[0x0305B00A]["authorTime"] = author_time_ms

    xml_root = ET.fromstring(str(data["header"]["data"][3]["xml"]))
    ident = xml_root.find("ident")
    desc = xml_root.find("desc")
    times = xml_root.find("times")
    if ident is None or desc is None or times is None:
        raise RuntimeError("Map XML does not contain ident/desc/times nodes.")
    ident.set("uid", new_uid)
    ident.set("name", map_name)
    if author_login:
        ident.set("author", author_login)
    if author_zone:
        ident.set("authorzone", author_zone)
    desc.set("nblaps", str(lap_count))
    times.set("bronze", str(bronze_time_ms))
    times.set("silver", str(silver_time_ms))
    times.set("gold", str(gold_time_ms))
    times.set("authortime", str(author_time_ms))
    times.set("hasclones", str(calculate_variant_has_clones(lap_count)))
    data["header"]["data"][3]["xml"] = ET.tostring(xml_root, encoding="unicode")


def create_variant_map_pure_python(
    stripped_file: Path,
    output_file: Path,
    *,
    source_uid: str,
    source_map_name: str,
    variant: dict[str, Any],
) -> None:
    parse_file, generate_file = import_gbxpy()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    data = parse_file(str(stripped_file), recursive=False)
    try:
        apply_variant_metadata_pure_python(
            data,
            source_uid=source_uid,
            source_map_name=source_map_name,
            variant=variant,
        )
        output_file.write_bytes(generate_file(data))
    finally:
        close_zip_handles(data)


def rewrite_uid_internal(
    source_file: Path, output_file: Path, old_uid: str, new_uid: str
) -> int:
    old_bytes = old_uid.encode("ascii")
    new_bytes = new_uid.encode("ascii")
    if len(old_bytes) != len(new_bytes):
        raise RuntimeError(
            "Internal UID replacement requires old and new UID to have equal length."
        )

    blob = source_file.read_bytes()
    count = blob.count(old_bytes)
    if count == 0:
        raise RuntimeError(f"Could not find old UID '{old_uid}' in file {source_file}")

    output_file.write_bytes(blob.replace(old_bytes, new_bytes))
    return count


def rewrite_uid_external(
    source_file: Path,
    output_file: Path,
    old_uid: str,
    new_uid: str,
    command_template: str,
) -> None:
    command = command_template.format(
        input=str(source_file),
        output=str(output_file),
        old_uid=old_uid,
        new_uid=new_uid,
    )
    run_command([command], shell=True)
    if not output_file.exists():
        raise RuntimeError(
            f"UID rewriter command finished but output file does not exist: {output_file}"
        )


def build_playlist_entries(map_uids: list[str]) -> list[dict[str, Any]]:
    return [
        {"position": index, "mapUid": map_uid}
        for index, map_uid in enumerate(map_uids)
    ]


def existing_uploaded_map_matches_variant(
    existing_map: dict[str, Any],
    *,
    variant: dict[str, Any],
    upload_author_account_id: str,
) -> bool:
    if not existing_map:
        return False
    if (
        upload_author_account_id
        and str(existing_map.get("author", "")).strip() != upload_author_account_id
    ):
        return False
    comparisons = (
        ("name", str(variant["map_name"])),
        ("authorTime", as_int(variant["author_time_ms"])),
        ("goldTime", as_int(variant["gold_time_ms"])),
        ("silverTime", as_int(variant["silver_time_ms"])),
        ("bronzeTime", as_int(variant["bronze_time_ms"])),
    )
    for field, expected in comparisons:
        actual = existing_map.get(field)
        if isinstance(expected, int):
            if as_int(actual, -1) != expected:
                return False
        elif str(actual) != expected:
            return False
    return True


class TrackmaniaApi:
    def __init__(self, user_agent: str, timeout_sec: int) -> None:
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.user_agent = user_agent
        self.core_token: str | None = None
        self.live_token: str | None = None
        self.account_id: str = ""
        self.account_name: str = ""

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        data: Any | None = None,
        files: Any | None = None,
        allow_not_found: bool = False,
    ) -> dict[str, Any] | list[Any]:
        merged_headers = {"User-Agent": self.user_agent}
        if headers:
            merged_headers.update(headers)

        response = self.session.request(
            method=method,
            url=url,
            headers=merged_headers,
            params=params,
            json=json_body,
            data=data,
            files=files,
            timeout=self.timeout_sec,
        )

        if response.status_code == 404 and allow_not_found:
            return {"_not_found": True}

        if response.status_code >= 400:
            body = response.text.strip()
            try:
                parsed = response.json()
                body = json.dumps(parsed, ensure_ascii=False)
            except ValueError:
                pass
            raise ApiError(f"{method} {url} failed ({response.status_code}): {body}")

        if not response.text:
            return {}

        try:
            return response.json()
        except ValueError as exc:
            raise ApiError(
                f"{method} {url} returned non-JSON body: {response.text[:400]}"
            ) from exc

    def _auth_header(self, token: str) -> dict[str, str]:
        return {"Authorization": f"nadeo_v1 t={token}"}

    def _extract_access_token(self, payload: dict[str, Any] | list[Any], label: str) -> str:
        token = payload.get("accessToken") if isinstance(payload, dict) else None
        if not token:
            raise ApiError(f"{label} auth response missing accessToken: {payload}")
        return str(token)

    def _request_ubisoft_token(self, ticket: str, audience: str) -> str:
        payload = self._request_json(
            "POST",
            f"{CORE_BASE_URL}/v2/authentication/token/ubiservices",
            headers={
                "Authorization": f"ubi_v1 t={ticket}",
                "Content-Type": "application/json",
            },
            json_body={"audience": audience},
        )
        return self._extract_access_token(payload, audience)

    def _request_service_account_token(
        self, login: str, password: str, audience: str
    ) -> str:
        basic = base64.b64encode(f"{login}:{password}".encode("utf-8")).decode("ascii")
        payload = self._request_json(
            "POST",
            f"{CORE_BASE_URL}/v2/authentication/token/basic",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/json",
            },
            json_body={"audience": audience},
        )
        return self._extract_access_token(payload, audience)

    def _populate_account_context(self) -> None:
        token_payload = decode_jwt_payload(self.core_token or "")
        self.account_id = str(token_payload.get("sub", "")).strip()
        self.account_name = str(token_payload.get("aun", "")).strip()

    def authorize_ubisoft(self, email: str, password: str) -> None:
        basic = base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")
        ubi_headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/json",
            "Ubi-AppId": UBI_APP_ID,
        }

        log("Authenticating with Ubisoft...")
        ubi_payload = self._request_json(
            "POST", UBI_AUTH_URL, headers=ubi_headers, data=""
        )
        ticket = ubi_payload.get("ticket") if isinstance(ubi_payload, dict) else None
        if not ticket:
            raise ApiError(
                f"Ubisoft auth response does not contain ticket: {ubi_payload}"
            )

        log("Requesting NadeoLiveServices token...")
        self.live_token = self._request_ubisoft_token(ticket, "NadeoLiveServices")

        log("Requesting NadeoServices token...")
        self.core_token = self._request_ubisoft_token(ticket, "NadeoServices")

        self._populate_account_context()

    def authorize_service_account(self, login: str, password: str) -> None:
        log("Authenticating with Trackmania service account...")
        log("Requesting NadeoLiveServices token...")
        self.live_token = self._request_service_account_token(
            login, password, "NadeoLiveServices"
        )

        log("Requesting NadeoServices token...")
        self.core_token = self._request_service_account_token(
            login, password, "NadeoServices"
        )

        self._populate_account_context()

    def authorize(self, mode: str, login: str, password: str) -> None:
        auth_mode = mode.strip().lower()
        if auth_mode == "service_account":
            self.authorize_service_account(login, password)
            return
        if auth_mode == "ubisoft":
            self.authorize_ubisoft(login, password)
            return
        raise ApiError(f"Unsupported auth mode: {mode}")

    def get_weekly_grands(self, length: int, offset: int) -> dict[str, Any]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        payload = self._request_json(
            "GET",
            f"{LIVE_BASE_URL}/api/campaign/weekly-grands",
            headers=self._auth_header(self.live_token),
            params={"length": length, "offset": offset},
        )
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected weekly-grands response shape: {payload}")
        return payload

    def get_live_map_info(
        self, map_uid: str, allow_missing: bool = False
    ) -> dict[str, Any] | None:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        payload = self._request_json(
            "GET",
            f"{LIVE_BASE_URL}/api/token/map/{map_uid}",
            headers=self._auth_header(self.live_token),
            allow_not_found=allow_missing,
        )
        if isinstance(payload, dict) and payload.get("_not_found"):
            return None
        if (
            isinstance(payload, dict)
            and payload.get("error") == "NotFoundHttpException"
        ):
            return None if allow_missing else None
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected map info response for UID {map_uid}: {payload}")
        if allow_missing and ("uid" not in payload and "mapUid" not in payload):
            return None
        return payload

    def download_file(self, url: str, destination: Path) -> None:
        if not self.core_token:
            raise RuntimeError("Not authenticated for core services.")
        headers = {
            "User-Agent": self.user_agent,
            "Authorization": f"nadeo_v1 t={self.core_token}",
        }
        with self.session.get(
            url, headers=headers, stream=True, timeout=self.timeout_sec
        ) as resp:
            if resp.status_code >= 400:
                raise ApiError(
                    f"Map download failed ({resp.status_code}): {resp.text[:300]}"
                )
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        handle.write(chunk)

    def download_bytes(self, url: str, *, auth: str = "none") -> bytes:
        headers = {"User-Agent": self.user_agent}
        if auth == "core":
            if not self.core_token:
                raise RuntimeError("Not authenticated for core services.")
            headers["Authorization"] = f"nadeo_v1 t={self.core_token}"
        elif auth == "live":
            if not self.live_token:
                raise RuntimeError("Not authenticated for live services.")
            headers["Authorization"] = f"nadeo_v1 t={self.live_token}"

        response = self.session.get(url, headers=headers, timeout=self.timeout_sec)
        if response.status_code >= 400:
            raise ApiError(
                f"Binary download failed ({response.status_code}): {response.text[:300]}"
            )
        return response.content

    def upload_activity_media_bytes(
        self, club_id: int, activity_id: int, media_blob: bytes
    ) -> str:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        headers = {
            "User-Agent": self.user_agent,
            "Authorization": f"nadeo_v1 t={self.live_token}",
        }
        response = self.session.post(
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/activity/{activity_id}/upload",
            headers=headers,
            data=media_blob,
            timeout=self.timeout_sec,
        )
        if response.status_code >= 400:
            raise ApiError(
                f"Activity media upload failed ({response.status_code}): {response.text[:300]}"
            )
        return response.text.strip()

    def upload_club_media_bytes(
        self, club_id: int, media_format: str, media_blob: bytes
    ) -> dict[str, Any]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        headers = {
            "User-Agent": self.user_agent,
            "Authorization": f"nadeo_v1 t={self.live_token}",
        }
        response = self.session.post(
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/media/upload",
            headers=headers,
            params={"format": media_format},
            data=media_blob,
            timeout=self.timeout_sec,
        )
        if response.status_code >= 400:
            raise ApiError(
                f"Club media upload failed ({response.status_code}): {response.text[:300]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiError(
                f"Club media upload returned non-JSON body: {response.text[:400]}"
            ) from exc
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected club media upload response shape: {payload}")
        return payload

    def upload_map(
        self,
        map_file: Path,
        source_map_info: dict[str, Any],
        *,
        map_id: str | None = None,
        map_uid: str,
        map_name: str,
        author_account_id: str | None = None,
        author_time_ms: int | None = None,
        gold_time_ms: int | None = None,
        silver_time_ms: int | None = None,
        bronze_time_ms: int | None = None,
    ) -> dict[str, Any]:
        if not self.core_token:
            raise RuntimeError("Not authenticated for core services.")
        files = [
            (
                "authorScore",
                (
                    None,
                    str(
                        author_time_ms
                        if author_time_ms is not None
                        else as_int(source_map_info.get("authorTime"), -1)
                    ),
                ),
            ),
            (
                "goldScore",
                (
                    None,
                    str(
                        gold_time_ms
                        if gold_time_ms is not None
                        else as_int(source_map_info.get("goldTime"), -1)
                    ),
                ),
            ),
            (
                "silverScore",
                (
                    None,
                    str(
                        silver_time_ms
                        if silver_time_ms is not None
                        else as_int(source_map_info.get("silverTime"), -1)
                    ),
                ),
            ),
            (
                "bronzeScore",
                (
                    None,
                    str(
                        bronze_time_ms
                        if bronze_time_ms is not None
                        else as_int(source_map_info.get("bronzeTime"), -1)
                    ),
                ),
            ),
            (
                "author",
                (
                    None,
                    str(
                        author_account_id
                        if author_account_id is not None
                        else source_map_info.get("author", "")
                    ),
                ),
            ),
            (
                "collectionName",
                (None, str(source_map_info.get("collectionName", "Stadium"))),
            ),
            ("mapStyle", (None, str(source_map_info.get("mapStyle", "")))),
            ("mapType", (None, str(source_map_info.get("mapType", "")))),
            ("mapUid", (None, map_uid)),
            ("name", (None, map_name)),
            (
                "nadeoservices-core-parameters",
                (None, '{"isPlayable":true}', "application/json"),
            ),
            ("data", (map_file.name, map_file.open("rb"), "application/octet-stream")),
        ]
        try:
            payload = self._request_json(
                "POST",
                f"{CORE_BASE_URL}/maps/{map_id}" if map_id else f"{CORE_BASE_URL}/maps/",
                headers=self._auth_header(self.core_token),
                files=files,
            )
        finally:
            data_file = files[-1][1][1]
            data_file.close()
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected map upload response: {payload}")
        return payload

    def get_club_activities(
        self, club_id: int, active: bool | None = True
    ) -> list[dict[str, Any]]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")

        all_items: list[dict[str, Any]] = []
        length = 250
        offset = 0

        while True:
            params: dict[str, Any] = {"length": length, "offset": offset}
            if active is not None:
                params["active"] = str(active).lower()
            payload = self._request_json(
                "GET",
                f"{LIVE_BASE_URL}/api/token/club/{club_id}/activity",
                headers=self._auth_header(self.live_token),
                params=params,
            )
            if not isinstance(payload, dict):
                raise ApiError(f"Unexpected activities response: {payload}")
            items = payload.get("activityList", [])
            if not isinstance(items, list):
                raise ApiError(f"Unexpected activityList payload: {payload}")
            for item in items:
                if isinstance(item, dict):
                    all_items.append(item)

            item_count = as_int(payload.get("itemCount"), 0)
            offset += length
            if offset >= item_count or not items:
                break

        return all_items

    def create_campaign(
        self, club_id: int, name: str, playlist_map_uids: list[str], folder_id: int
    ) -> dict[str, Any]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        payload = self._request_json(
            "POST",
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/campaign/create",
            headers=self._auth_header(self.live_token),
            json_body={
                "name": name,
                "playlist": build_playlist_entries(playlist_map_uids),
                "folderId": folder_id,
            },
        )
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected create campaign response: {payload}")
        return payload

    def edit_campaign(
        self, club_id: int, campaign_id: int, name: str, playlist_map_uids: list[str]
    ) -> dict[str, Any]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        payload = self._request_json(
            "POST",
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/campaign/{campaign_id}/edit",
            headers=self._auth_header(self.live_token),
            json_body={
                "name": name,
                "playlist": build_playlist_entries(playlist_map_uids),
            },
        )
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected edit campaign response: {payload}")
        return payload

    def edit_activity(
        self, club_id: int, activity_id: int, body: dict[str, Any]
    ) -> dict[str, Any]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        payload = self._request_json(
            "POST",
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/activity/{activity_id}/edit",
            headers=self._auth_header(self.live_token),
            json_body=body,
        )
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected edit activity response: {payload}")
        return payload

    def add_map_to_bucket(self, club_id: int, bucket_id: int, map_uid: str) -> None:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        self._request_json(
            "POST",
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/bucket/{bucket_id}/add",
            headers=self._auth_header(self.live_token),
            json_body={"itemIdList": [map_uid]},
        )


def ensure_campaign_name(name: str, truncate_to_20: bool) -> str:
    trimmed = name.strip()
    if len(trimmed) <= 20:
        return trimmed
    if not truncate_to_20:
        raise RuntimeError(
            f"Campaign name is too long ({len(trimmed)} chars; max 20): {trimmed!r}. "
            "Adjust campaign.name_template or enable campaign.truncate_to_20."
        )
    shortened = trimmed[:20].rstrip()
    log(f"Campaign name truncated to 20 chars: {shortened!r}")
    return shortened


def append_unique_url(candidates: list[str], value: Any) -> None:
    if not isinstance(value, str):
        return
    url = value.strip()
    if not url:
        return
    if url in candidates:
        return
    candidates.append(url)


def download_first_candidate_bytes(
    api: "TrackmaniaApi", candidates: list[str]
) -> tuple[bytes | None, str | None, str | None]:
    last_error: str | None = None
    for candidate_url in candidates:
        auth_order = ["none", "core"]
        if "core.trackmania.nadeo.live" in candidate_url:
            auth_order = ["core", "none"]
        for auth_mode in auth_order:
            try:
                return (
                    api.download_bytes(candidate_url, auth=auth_mode),
                    candidate_url,
                    None,
                )
            except ApiError as exc:
                last_error = str(exc)
    return None, None, last_error


def extract_map_uid(map_payload: dict[str, Any], fallback_uid: str) -> str:
    for key in ("mapUid", "uid"):
        value = map_payload.get(key)
        if isinstance(value, str) and value:
            return value
    return fallback_uid


def find_existing_campaign_activity(
    api: TrackmaniaApi,
    club_id: int,
    campaign_name: str,
) -> dict[str, Any] | None:
    for active in (True, None):
        try:
            activities = api.get_club_activities(club_id, active=active)
        except ApiError:
            if active is None:
                raise
            continue
        for activity in activities:
            if activity.get("activityType") != "campaign":
                continue
            current_name = str(activity.get("name", "")).strip()
            if current_name.lower() == campaign_name.strip().lower():
                return activity
    return None


def find_existing_campaign_activity_by_ids(
    api: TrackmaniaApi,
    club_id: int,
    *,
    activity_id: int = 0,
    campaign_id: int = 0,
) -> dict[str, Any] | None:
    if activity_id <= 0 and campaign_id <= 0:
        return None

    for active in (True, None):
        try:
            activities = api.get_club_activities(club_id, active=active)
        except ApiError:
            if active is None:
                raise
            continue
        for activity in activities:
            if activity.get("activityType") != "campaign":
                continue
            if activity_id > 0 and as_int(activity.get("activityId"), 0) == activity_id:
                return activity
            if campaign_id > 0 and as_int(activity.get("campaignId"), 0) == campaign_id:
                return activity
    return None


def activity_identifier(activity: dict[str, Any]) -> int:
    activity_id = as_int(activity.get("activityId"), 0)
    if activity_id > 0:
        return activity_id
    return as_int(activity.get("id"), 0)


def find_activity_by_name(
    activities: list[dict[str, Any]],
    name: str,
    *,
    activity_type: str = "",
) -> dict[str, Any] | None:
    wanted_name = name.strip().lower()
    wanted_type = activity_type.strip().lower()
    if not wanted_name:
        return None
    for activity in activities:
        current_name = str(activity.get("name", "")).strip().lower()
        if current_name != wanted_name:
            continue
        if wanted_type:
            current_type = str(activity.get("activityType", "")).strip().lower()
            if current_type != wanted_type:
                continue
        return activity
    return None


def enforce_activity_order(
    api: TrackmaniaApi,
    *,
    club_id: int,
    processed_activity_id: int,
    cfg: dict[str, Any],
) -> tuple[bool, bool]:
    pinned_moved = False
    campaign_moved = False

    if processed_activity_id <= 0 or not cfg["ordering"]["enabled"]:
        return pinned_moved, campaign_moved

    pinned_name = str(cfg["ordering"]["pinned_activity_name"]).strip()
    pinned_type = str(cfg["ordering"]["pinned_activity_type"]).strip().lower()
    pinned_position = as_int(cfg["ordering"]["pinned_position"], 0)
    campaign_position = as_int(cfg["ordering"]["processed_campaign_position"], 1)

    if campaign_position <= pinned_position:
        campaign_position = pinned_position + 1

    try:
        activities = api.get_club_activities(club_id, active=True)
    except ApiError as exc:
        log(f"Order step skipped: failed to fetch activities ({exc}).")
        return pinned_moved, campaign_moved

    pinned_activity = find_activity_by_name(
        activities, pinned_name, activity_type=pinned_type
    )
    if pinned_activity:
        pinned_activity_id = activity_identifier(pinned_activity)
        current_pin_position = as_int(pinned_activity.get("position"), -1)
        if pinned_activity_id > 0 and current_pin_position != pinned_position:
            try:
                api.edit_activity(
                    club_id, pinned_activity_id, {"position": pinned_position}
                )
                pinned_moved = True
                log(
                    f"Moved pinned activity '{pinned_name}' to position {pinned_position}."
                )
                activities = api.get_club_activities(club_id, active=True)
            except ApiError as exc:
                log(
                    f"Pinned activity move failed ('{pinned_name}' -> {pinned_position}): {exc}"
                )
    else:
        log(
            f"Pinned activity not found ('{pinned_name}', type '{pinned_type or 'any'}')."
        )

    processed_activity = next(
        (
            activity
            for activity in activities
            if activity_identifier(activity) == processed_activity_id
        ),
        None,
    )
    if not processed_activity:
        log(
            f"Processed activity not found for ordering (activityId={processed_activity_id})."
        )
        return pinned_moved, campaign_moved

    current_campaign_position = as_int(processed_activity.get("position"), -1)
    if current_campaign_position != campaign_position:
        try:
            api.edit_activity(
                club_id, processed_activity_id, {"position": campaign_position}
            )
            campaign_moved = True
            log(
                f"Moved processed campaign activity {processed_activity_id} to position {campaign_position}."
            )
        except ApiError as exc:
            log(
                f"Processed campaign move failed ({processed_activity_id} -> {campaign_position}): {exc}"
            )

    return pinned_moved, campaign_moved


def process_one_campaign(
    api: TrackmaniaApi,
    cfg: dict[str, Any],
    campaign: dict[str, Any],
    *,
    dry_run: bool,
    force: bool,
    recorded_variant_uids: dict[int, str] | None = None,
    previous_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    playlist = campaign.get("playlist")
    if not isinstance(playlist, list) or not playlist:
        raise RuntimeError(f"Campaign has no playlist: {campaign}")
    first = playlist[0]
    if not isinstance(first, dict) or "mapUid" not in first:
        raise RuntimeError(f"Campaign playlist first item has no mapUid: {campaign}")

    source_map_uid = str(first["mapUid"])
    source_campaign_name = str(campaign.get("name", "Weekly Grand"))
    week = as_int(campaign.get("week"), -1)
    year = as_int(campaign.get("year"), -1)
    season_uid = str(campaign.get("seasonUid", source_map_uid))

    source_map_info = api.get_live_map_info(source_map_uid)
    if not source_map_info:
        raise RuntimeError(f"Could not load source map info for UID {source_map_uid}")

    source_map_name = str(source_map_info.get("name", source_map_uid))
    source_map_name_clean = clean_trackmania_name(source_map_name) or source_map_name
    base_map_uid = build_prefixed_uid(source_map_uid, cfg["map"]["uid_prefix"])

    context = {
        "source_campaign_name": source_campaign_name,
        "source_map_name": source_map_name,
        "source_map_name_clean": source_map_name_clean,
        "source_map_uid": source_map_uid,
        "new_map_uid": base_map_uid,
        "week": week,
        "year": year,
        "season_uid": season_uid,
    }

    campaign_name_raw = render_template(cfg["campaign"]["name_template"], context)
    campaign_name = ensure_campaign_name(
        campaign_name_raw, cfg["campaign"]["truncate_to_20"]
    )

    log(f"Weekly source: {source_campaign_name} | map UID: {source_map_uid}")
    log(f"Target campaign name: {campaign_name}")

    uploaded_map_uids: list[str] = []
    playlist_map_uids: list[str] = []
    primary_uploaded_map_uid = base_map_uid
    primary_activity_media_blob: bytes | None = None
    thumbnail_candidates: list[str] = []
    variants: list[dict[str, Any]] = []
    source_lap_count = -1
    source_thumbnail_jpeg: bytes | None = None

    work_dir = Path(cfg["paths"]["work_dir"])
    raw_dir = work_dir / "raw"
    processed_dir = work_dir / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    safe_stub = sanitize_for_filename(f"{year}_w{week}_{source_campaign_name}")
    source_file = raw_dir / f"{safe_stub}_{source_map_uid}.Map.Gbx"
    stripped_file = processed_dir / f"{safe_stub}_{source_map_uid}.stripped.Map.Gbx"

    download_url = source_map_info.get("downloadUrl")
    if not isinstance(download_url, str) or not download_url:
        raise RuntimeError(
            f"Source map does not expose downloadUrl: {source_map_info}"
        )

    if dry_run:
        planned_variant_laps = normalize_lap_variants(cfg["map"]["lap_variants"])
        planned_laps = ", ".join(f"{lap}L" for lap in planned_variant_laps)
        if source_file.exists():
            try:
                seed = read_map_variant_seed(source_file)
                context.update(
                    build_target_author_metadata(
                        cfg,
                        context,
                        seed,
                        upload_author_account_id=api.account_id,
                    )
                )
                variants = build_lap_variants(
                    cfg,
                    context,
                    source_lap_count=seed["source_lap_count"],
                    source_author_time_ms=seed["source_author_time_ms"],
                    uid_overrides=recorded_variant_uids,
                )
                playlist_map_uids = [str(variant["map_uid"]) for variant in variants]
                for variant in variants:
                    if as_int(variant["lap_count"], -1) == seed["source_lap_count"]:
                        primary_uploaded_map_uid = str(variant["map_uid"])
                        break
            except Exception as exc:
                log(f"[DRY-RUN] Variant planning fallback (could not parse cached source map): {exc}")
        log(f"[DRY-RUN] Planned lap variants: {planned_laps}")
        log(f"[DRY-RUN] Download source map -> {source_file}")
        log(f"[DRY-RUN] Strip validation replay -> {stripped_file}")
        for lap_count in planned_variant_laps:
            log(f"[DRY-RUN] Build/upload {lap_count}L variant")
    else:
        log("Download source map")
        api.download_file(download_url, source_file)

        seed = read_map_variant_seed(source_file)
        source_lap_count = seed["source_lap_count"]
        if (
            source_lap_count != seed["header_lap_count"]
            or source_lap_count != seed["body_lap_count"]
        ):
            raise RuntimeError(
                "Source map lap count mismatch across XML/header/body: "
                f"{seed}"
            )

        context.update(
            build_target_author_metadata(
                cfg,
                context,
                seed,
                upload_author_account_id=api.account_id,
            )
        )
        variants = build_lap_variants(
            cfg,
            context,
            source_lap_count=source_lap_count,
            source_author_time_ms=seed["source_author_time_ms"],
            uid_overrides=recorded_variant_uids,
        )
        source_thumbnail_jpeg = read_variant_map_metadata(source_file)["thumbnail_jpeg"]
        planned_laps = ", ".join(f"{variant['lap_count']}L" for variant in variants)
        log(
            "Source map settings: "
            f"laps={source_lap_count}, author={seed['source_author_time_ms']}ms | "
            f"variants={planned_laps}"
        )

        transform_mode = str(cfg["map"].get("transform_mode", "pure_python"))
        if transform_mode == "pure_python":
            if bool(cfg["map"].get("strip_validation_replay", False)):
                create_stripped_map_pure_python(source_file, stripped_file)
                log("Base strip complete (pure python)")
            else:
                shutil.copyfile(source_file, stripped_file)
                log("Validation replay preserved; base map copied without strip")
        else:
            if len(variants) != 1:
                raise RuntimeError(
                    "Legacy transform mode only supports a single uploaded map. "
                    "Use map.transform_mode='pure_python' for lap variants."
                )
            strip_cmd = [
                cfg["map"]["strip_exe"],
                str(source_file),
                str(stripped_file),
            ]
            strip_note = cfg["map"]["strip_note"]
            if strip_note:
                strip_cmd.append(strip_note)
            run_command(strip_cmd)

        for variant in variants:
            lap_count = as_int(variant["lap_count"], -1)
            variant_uid = str(variant["map_uid"])
            variant_map_name = str(variant["map_name"])
            variant_file = (
                processed_dir
                / f"{safe_stub}_{lap_count:03d}L_{variant_uid}.Map.Gbx"
            )
            existing_map: dict[str, Any] | None = None

            needs_rebuild = force or not variant_file.exists()
            if (
                not needs_rebuild
                and source_thumbnail_jpeg is not None
                and variant_file.exists()
            ):
                local_check = check_variant_file_compliance(
                    variant_file,
                    expected_variant=variant,
                    source_thumbnail_jpeg=source_thumbnail_jpeg,
                )
                if not local_check["ok"]:
                    needs_rebuild = True
                    issue_summary = "; ".join(local_check["issues"])
                    log(f"Rebuild local {lap_count}L variant: {issue_summary}")

            if needs_rebuild:
                if transform_mode == "pure_python":
                    create_variant_map_safe(
                        stripped_file,
                        variant_file,
                        variant=variant,
                    )
                    log(
                        "Variant map built: "
                        f"{lap_count}L -> {variant_file.name} "
                        f"(AT {variant['author_time_ms']}ms)"
                    )
                else:
                    mode = cfg["map"]["uid_rewriter"]["mode"]
                    if mode == "internal_replace":
                        replaced = rewrite_uid_internal(
                            stripped_file, variant_file, source_map_uid, variant_uid
                        )
                        log(
                            "UID rewrite complete (internal), "
                            f"lap={lap_count}L replaced={replaced}"
                        )
                    else:
                        rewrite_uid_external(
                            stripped_file,
                            variant_file,
                            source_map_uid,
                            variant_uid,
                            cfg["map"]["uid_rewriter"]["command_template"],
                        )
                        log(f"UID rewrite complete (external), lap={lap_count}L")

            uploaded_map_payload: dict[str, Any] | None = None
            if cfg["map"]["allow_reuse_existing_uid"] or force:
                existing_map = api.get_live_map_info(variant_uid, allow_missing=True)
            if cfg["map"]["allow_reuse_existing_uid"] and not force:
                upload_author_account_id = str(
                    variant.get("upload_author_account_id", "")
                ).strip()
                if existing_map and existing_uploaded_map_matches_variant(
                    existing_map,
                    variant=variant,
                    upload_author_account_id=upload_author_account_id,
                ):
                    log(f"Reuse existing {lap_count}L map: {variant_uid}")
                    uploaded_map_payload = existing_map
                elif existing_map:
                    log(
                        f"Existing {lap_count}L map differs from target metadata; update required."
                    )

            if not uploaded_map_payload:
                existing_map_id = (
                    str(existing_map.get("mapId", "")).strip() if existing_map else ""
                )
                if existing_map_id:
                    log(f"Update existing {lap_count}L map in Nadeo Core")
                else:
                    log(f"Upload {lap_count}L map to Nadeo Core")
                uploaded_map_payload = api.upload_map(
                    variant_file,
                    source_map_info,
                    map_id=existing_map_id or None,
                    map_uid=variant_uid,
                    map_name=variant_map_name,
                    author_account_id=str(
                        variant.get("upload_author_account_id", "")
                    ).strip()
                    or api.account_id,
                    author_time_ms=as_int(variant["author_time_ms"]),
                    gold_time_ms=as_int(variant["gold_time_ms"]),
                    silver_time_ms=as_int(variant["silver_time_ms"]),
                    bronze_time_ms=as_int(variant["bronze_time_ms"]),
                )

            uploaded_map_uid = extract_map_uid(uploaded_map_payload, variant_uid)
            uploaded_map_uids.append(uploaded_map_uid)
            if lap_count == source_lap_count or not primary_uploaded_map_uid:
                primary_uploaded_map_uid = uploaded_map_uid
                primary_activity_media_blob = render_activity_media_thumbnail(
                    read_variant_map_metadata(variant_file)["thumbnail_jpeg"]
                )

            log(
                f"Map ready: {lap_count}L -> uid={uploaded_map_uid} ({variant_map_name})"
            )
            append_unique_url(
                thumbnail_candidates, uploaded_map_payload.get("thumbnailUrl")
            )
            append_unique_url(
                thumbnail_candidates, uploaded_map_payload.get("mediaUrlPngLarge")
            )
            append_unique_url(
                thumbnail_candidates, uploaded_map_payload.get("mediaUrlPngMedium")
            )
            append_unique_url(
                thumbnail_candidates, uploaded_map_payload.get("mediaUrlPngSmall")
            )
            append_unique_url(thumbnail_candidates, uploaded_map_payload.get("mediaUrl"))

    append_unique_url(thumbnail_candidates, source_map_info.get("thumbnailUrl"))
    append_unique_url(thumbnail_candidates, source_map_info.get("mediaUrlPngLarge"))
    append_unique_url(thumbnail_candidates, source_map_info.get("mediaUrlPngMedium"))
    append_unique_url(thumbnail_candidates, source_map_info.get("mediaUrlPngSmall"))
    append_unique_url(thumbnail_candidates, source_map_info.get("mediaUrl"))
    append_unique_url(thumbnail_candidates, source_map_info.get("fileUrl"))

    append_unique_url(thumbnail_candidates, campaign.get("mediaUrl"))
    append_unique_url(thumbnail_candidates, campaign.get("mediaUrlPngLarge"))
    append_unique_url(thumbnail_candidates, campaign.get("mediaUrlPngMedium"))
    append_unique_url(thumbnail_candidates, campaign.get("mediaUrlPngSmall"))

    if uploaded_map_uids:
        playlist_map_uids = uploaded_map_uids[:]
    if not dry_run and not playlist_map_uids:
        raise RuntimeError("No variant maps were prepared for campaign playlist.")

    for uploaded_map_uid in playlist_map_uids:
        uploaded_map_info = api.get_live_map_info(uploaded_map_uid, allow_missing=True)
        if not uploaded_map_info:
            continue
        append_unique_url(thumbnail_candidates, uploaded_map_info.get("thumbnailUrl"))
        append_unique_url(
            thumbnail_candidates, uploaded_map_info.get("mediaUrlPngLarge")
        )
        append_unique_url(
            thumbnail_candidates, uploaded_map_info.get("mediaUrlPngMedium")
        )
        append_unique_url(
            thumbnail_candidates, uploaded_map_info.get("mediaUrlPngSmall")
        )
        append_unique_url(thumbnail_candidates, uploaded_map_info.get("mediaUrl"))

    background_candidates: list[str] = []
    if cfg["club_background"]["prefer_weekly_campaign_media"]:
        append_unique_url(background_candidates, campaign.get("mediaUrl"))
        append_unique_url(background_candidates, campaign.get("mediaUrlPngLarge"))
        append_unique_url(background_candidates, campaign.get("mediaUrlPngMedium"))
        append_unique_url(background_candidates, campaign.get("mediaUrlPngSmall"))
    for url in thumbnail_candidates:
        append_unique_url(background_candidates, url)
    if not cfg["club_background"]["prefer_weekly_campaign_media"]:
        append_unique_url(background_candidates, campaign.get("mediaUrl"))
        append_unique_url(background_candidates, campaign.get("mediaUrlPngLarge"))
        append_unique_url(background_candidates, campaign.get("mediaUrlPngMedium"))
        append_unique_url(background_candidates, campaign.get("mediaUrlPngSmall"))

    club_id = as_int(cfg["club"]["club_id"], 0)
    folder_id = as_int(cfg["club"]["folder_id"], 0)

    existing_activity = None
    if previous_result:
        prev_activity_id = as_int(previous_result.get("activity_id"), 0)
        prev_campaign_id = as_int(previous_result.get("campaign_id"), 0)
        if prev_activity_id > 0 or prev_campaign_id > 0:
            existing_activity = find_existing_campaign_activity_by_ids(
                api,
                club_id,
                activity_id=prev_activity_id,
                campaign_id=prev_campaign_id,
            )
            if existing_activity:
                log(
                    "Reuse activity from state: "
                    f"activityId={existing_activity.get('activityId')} campaignId={existing_activity.get('campaignId')}"
                )

    if cfg["club"]["upsert_by_name"]:
        if not existing_activity:
            existing_activity = find_existing_campaign_activity(
                api, club_id, campaign_name
            )
            if existing_activity:
                log(
                    "Reuse activity by name: "
                    f"activityId={existing_activity.get('activityId')} campaignId={existing_activity.get('campaignId')}"
                )

    if dry_run:
        planned_variant_count = len(normalize_lap_variants(cfg["map"]["lap_variants"]))
        if existing_activity:
            log(
                "[DRY-RUN] Edit campaign "
                f"{existing_activity.get('campaignId')} with {planned_variant_count} map variants"
            )
        else:
            log(
                f"[DRY-RUN] Create campaign in club {club_id} with {planned_variant_count} map variants"
            )
        if cfg["club"]["upload_activity_media_from_map_thumbnail"]:
            if primary_activity_media_blob is not None or thumbnail_candidates:
                log("[DRY-RUN] Upload activity media from map image bytes")
            else:
                log("[DRY-RUN] Skip activity media upload (no image URL)")
        if cfg["ordering"]["enabled"]:
            pinned_name = str(cfg["ordering"]["pinned_activity_name"]).strip()
            pinned_position = as_int(cfg["ordering"]["pinned_position"], 0)
            campaign_position = as_int(
                cfg["ordering"]["processed_campaign_position"], 1
            )
            if campaign_position <= pinned_position:
                campaign_position = pinned_position + 1
            log(
                "[DRY-RUN] Enforce ordering: "
                f"'{pinned_name}' -> {pinned_position}, processed campaign -> {campaign_position}."
            )
        if cfg["club_background"]["enabled"]:
            if background_candidates:
                media_format = str(cfg["club_background"]["format"]).strip().lower()
                log(
                    f"[DRY-RUN] Upload club '{media_format}' media from weekly/map image"
                )
            else:
                log("[DRY-RUN] Skip club media upload (no image URL)")
        if cfg["club_bucket"]["enabled"]:
            log(
                f"[DRY-RUN] Add {planned_variant_count} map UIDs to bucket {cfg['club_bucket']['bucket_id']}"
            )
        return {
            "season_uid": season_uid,
            "source_map_uid": source_map_uid,
            "new_map_uid": primary_uploaded_map_uid,
            "map_uids": playlist_map_uids,
            "campaign_name": campaign_name,
            "dry_run": True,
        }

    if existing_activity:
        campaign_id = as_int(existing_activity.get("campaignId"), 0)
        if campaign_id <= 0:
            raise RuntimeError(
                f"Existing activity has invalid campaignId: {existing_activity}"
            )
        campaign_payload = api.edit_campaign(
            club_id, campaign_id, campaign_name, playlist_map_uids
        )
        activity_id = as_int(existing_activity.get("activityId"), 0)
    else:
        campaign_payload = api.create_campaign(
            club_id, campaign_name, playlist_map_uids, folder_id
        )
        activity_id = as_int(campaign_payload.get("activityId"), 0)
        campaign_id = as_int(campaign_payload.get("campaignId"), 0)
        log(f"Campaign created: activityId={activity_id}, campaignId={campaign_id}")

    edit_body: dict[str, Any] = {}
    if cfg["club"]["activate"] is not None:
        edit_body["active"] = 1 if bool(cfg["club"]["activate"]) else 0
    if cfg["club"]["public"] is not None:
        edit_body["public"] = 1 if bool(cfg["club"]["public"]) else 0
    if cfg["club"]["featured"] is not None:
        edit_body["featured"] = 1 if bool(cfg["club"]["featured"]) else 0
    if cfg["club"]["folder_id"] is not None:
        edit_body["folderId"] = folder_id
    if cfg["club"]["media_theme"]:
        edit_body["mediaTheme"] = cfg["club"]["media_theme"]
    if cfg["club"]["activity_position"] is not None:
        edit_body["position"] = as_int(cfg["club"]["activity_position"])

    if activity_id > 0 and edit_body:
        api.edit_activity(club_id, activity_id, edit_body)
        log(f"Activity updated: {activity_id}")

    ordering_pinned_moved = False
    ordering_campaign_moved = False
    if activity_id > 0 and cfg["ordering"]["enabled"]:
        ordering_pinned_moved, ordering_campaign_moved = enforce_activity_order(
            api,
            club_id=club_id,
            processed_activity_id=activity_id,
            cfg=cfg,
        )

    media_uploaded = False
    if activity_id > 0 and cfg["club"]["upload_activity_media_from_map_thumbnail"]:
        if primary_activity_media_blob is not None:
            log("Upload activity media (local primary variant thumbnail)")
            upload_result = api.upload_activity_media_bytes(
                club_id, activity_id, primary_activity_media_blob
            )
            media_uploaded = True
            log(f"Activity media upload result: {upload_result or 'OK'}")
        elif not thumbnail_candidates:
            log("Activity media upload skipped: no image URL")
        else:
            media_blob, media_url_used, last_error = download_first_candidate_bytes(
                api, thumbnail_candidates
            )
            if media_blob is None:
                log(
                    f"Activity media download failed; upload skipped. Last error: {last_error or 'unknown error'}"
                )
            else:
                log(f"Upload activity media ({media_url_used})")
                upload_result = api.upload_activity_media_bytes(
                    club_id, activity_id, media_blob
                )
                media_uploaded = True
                log(f"Activity media upload result: {upload_result or 'OK'}")

    club_media_uploaded = False
    club_media_url = ""
    if cfg["club_background"]["enabled"]:
        media_format = str(cfg["club_background"]["format"]).strip().lower()
        if not background_candidates:
            log(f"Club media upload skipped ({media_format}): no image URL")
        else:
            media_blob, media_url_used, last_error = download_first_candidate_bytes(
                api, background_candidates
            )
            if media_blob is None:
                log(
                    f"Club media download failed ({media_format}); "
                    f"upload skipped. Last error: {last_error or 'unknown error'}"
                )
            else:
                log(f"Upload club '{media_format}' media ({media_url_used})")
                club_payload = api.upload_club_media_bytes(
                    club_id, media_format, media_blob
                )
                club_media_uploaded = True
                media_key = f"{media_format}Url"
                uploaded_url = club_payload.get(media_key)
                if isinstance(uploaded_url, str):
                    club_media_url = uploaded_url
                log(f"Club '{media_format}' media upload complete")

    if cfg["club_bucket"]["enabled"]:
        bucket_id = as_int(cfg["club_bucket"]["bucket_id"], 0)
        if bucket_id <= 0:
            raise RuntimeError(
                "club_bucket.enabled=true but club_bucket.bucket_id is not valid."
            )
        for uploaded_map_uid in playlist_map_uids:
            api.add_map_to_bucket(club_id, bucket_id, uploaded_map_uid)
            log(f"Map added to bucket {bucket_id}: {uploaded_map_uid}")

    return {
        "season_uid": season_uid,
        "source_map_uid": source_map_uid,
        "source_lap_count": source_lap_count,
        "new_map_uid": primary_uploaded_map_uid,
        "map_uids": playlist_map_uids,
        "variant_records": [
            {
                "lap_count": as_int(variant["lap_count"], -1),
                "map_uid": str(variant["map_uid"]),
                "map_name": str(variant["map_name"]),
            }
            for variant in variants
        ],
        "campaign_name": campaign_name,
        "week": week,
        "year": year,
        "activity_id": activity_id,
        "campaign_id": as_int(campaign_payload.get("campaignId"), 0),
        "activity_media_uploaded": media_uploaded,
        "ordering_pinned_moved": ordering_pinned_moved,
        "ordering_campaign_moved": ordering_campaign_moved,
        "club_media_uploaded": club_media_uploaded,
        "club_media_url": club_media_url,
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dry_run": False,
    }


def read_variant_map_metadata(map_file: Path) -> dict[str, Any]:
    parse_file, _ = import_gbxpy()
    data = parse_file(str(map_file), recursive=False)
    try:
        header_desc_version, header_desc_values = parse_tm2020_header_desc_chunk(
            bytes(data["header"]["data"][0])
        )
        header_author = parse_tm2020_header_author_chunk(bytes(data["header"]["data"][5]))
        xml_root = ET.fromstring(str(data["header"]["data"][3]["xml"]))
        ident = xml_root.find("ident")
        desc = xml_root.find("desc")
        times = xml_root.find("times")
        if ident is None or desc is None or times is None:
            raise RuntimeError("Map XML does not contain ident/desc/times nodes.")

        thumbnail_version, thumbnail_jpeg, _ = extract_thumbnail_jpeg(
            bytes(data["header"]["data"][4])
        )
        challenge_parameters = data["body"][0x03043011]["challengeParameters"]["body"]
        body_004 = challenge_parameters.get(0x0305B004)
        body_00A = challenge_parameters.get(0x0305B00A)
        body_author_chunk = data["body"].get(TM2020_BODY_AUTHOR_CHUNK_ID)
        body_author = (
            parse_tm2020_header_author_chunk(bytes(body_author_chunk["_unknownChunkId"]))
            if body_author_chunk and "_unknownChunkId" in body_author_chunk
            else None
        )

        return {
            "header_desc_version": header_desc_version,
            "header_uid": str(data["header"]["data"][1]["mapInfo"]["id"]),
            "body_uid": str(data["body"][0x0304301F]["mapInfo"]["id"]),
            "xml_uid": ident.attrib.get("uid", ""),
            "header_name": str(data["header"]["data"][1]["mapName"]),
            "body_name": str(data["body"][0x0304301F]["mapName"]),
            "xml_name": ident.attrib.get("name", ""),
            "header_author_login": str(data["header"]["data"][1]["mapInfo"]["author"]),
            "body_author_login": str(data["body"][0x0304301F]["mapInfo"]["author"]),
            "xml_author_login": ident.attrib.get("author", ""),
            "xml_author_zone": ident.attrib.get("authorzone", ""),
            "header_author_chunk_login": str(header_author["author_login"]),
            "header_author_nickname": str(header_author["author_nickname"]),
            "header_author_zone": str(header_author["author_zone"]),
            "body_author_chunk_login": (
                str(body_author["author_login"]) if body_author else ""
            ),
            "body_author_chunk_nickname": (
                str(body_author["author_nickname"]) if body_author else ""
            ),
            "body_author_chunk_zone": (
                str(body_author["author_zone"]) if body_author else ""
            ),
            "header_lap_count": header_desc_values[13],
            "body_lap_count": struct.unpack(
                "<2i", bytes(data["body"][0x03043018]["_unknownChunkId"])
            )[1],
            "xml_lap_count": as_int(desc.attrib.get("nblaps"), -1),
            "xml_has_clones": as_int(times.attrib.get("hasclones"), -1),
            "header_author_time_ms": header_desc_values[4],
            "header_gold_time_ms": header_desc_values[3],
            "header_silver_time_ms": header_desc_values[2],
            "header_bronze_time_ms": header_desc_values[1],
            "xml_author_time_ms": as_int(times.attrib.get("authortime"), -1),
            "xml_gold_time_ms": as_int(times.attrib.get("gold"), -1),
            "xml_silver_time_ms": as_int(times.attrib.get("silver"), -1),
            "xml_bronze_time_ms": as_int(times.attrib.get("bronze"), -1),
            "cp004_author_time_ms": as_int(body_004.get("authorTime")) if body_004 else -1,
            "cp004_gold_time_ms": as_int(body_004.get("goldTime")) if body_004 else -1,
            "cp004_silver_time_ms": as_int(body_004.get("silverTime")) if body_004 else -1,
            "cp004_bronze_time_ms": as_int(body_004.get("bronzeTime")) if body_004 else -1,
            "cp00a_author_time_ms": as_int(body_00A.get("authorTime")) if body_00A else -1,
            "cp00a_gold_time_ms": as_int(body_00A.get("goldTime")) if body_00A else -1,
            "cp00a_silver_time_ms": as_int(body_00A.get("silverTime")) if body_00A else -1,
            "cp00a_bronze_time_ms": as_int(body_00A.get("bronzeTime")) if body_00A else -1,
            "thumbnail_version": thumbnail_version,
            "thumbnail_jpeg": thumbnail_jpeg,
        }
    finally:
        close_zip_handles(data)


def check_variant_file_compliance(
    map_file: Path,
    *,
    expected_variant: dict[str, Any],
    source_thumbnail_jpeg: bytes,
) -> dict[str, Any]:
    issues: list[str] = []
    if not map_file.exists():
        return {"file": str(map_file), "ok": False, "issues": ["file is missing"]}

    metadata = read_variant_map_metadata(map_file)
    expected_uid = str(expected_variant["map_uid"])
    expected_name = str(expected_variant["map_name"])
    expected_lap_count = as_int(expected_variant["lap_count"], -1)
    expected_author = as_int(expected_variant["author_time_ms"], -1)
    expected_gold = as_int(expected_variant["gold_time_ms"], -1)
    expected_silver = as_int(expected_variant["silver_time_ms"], -1)
    expected_bronze = as_int(expected_variant["bronze_time_ms"], -1)
    expected_author_login = str(expected_variant.get("author_login", "")).strip()
    expected_author_nickname = str(expected_variant.get("author_nickname", "")).strip()
    expected_author_zone = str(expected_variant.get("author_zone", "")).strip()
    expected_has_clones = calculate_variant_has_clones(expected_lap_count)

    if f"{expected_lap_count:03d}L" not in map_file.name:
        issues.append("filename does not contain the expected lap tag")
    if metadata["header_uid"] != expected_uid or metadata["body_uid"] != expected_uid:
        issues.append("header/body UID does not match expected variant UID")
    if metadata["xml_uid"] != expected_uid:
        issues.append("XML UID does not match expected variant UID")
    if metadata["header_name"] != expected_name or metadata["body_name"] != expected_name:
        issues.append("header/body map name does not match expected variant name")
    if metadata["xml_name"] != expected_name:
        issues.append("XML map name does not match expected variant name")
    if expected_author_login:
        if (
            metadata["header_author_login"] != expected_author_login
            or metadata["body_author_login"] != expected_author_login
        ):
            issues.append("header/body author login does not match expected value")
        if metadata["xml_author_login"] != expected_author_login:
            issues.append("XML author login does not match expected value")
        if metadata["header_author_chunk_login"] != expected_author_login:
            issues.append("header author-info login does not match expected value")
        if metadata["body_author_chunk_login"] != expected_author_login:
            issues.append("body author-info login does not match expected value")
    if expected_author_nickname:
        if metadata["header_author_nickname"] != expected_author_nickname:
            issues.append("header author nickname does not match expected value")
        if metadata["body_author_chunk_nickname"] != expected_author_nickname:
            issues.append("body author nickname does not match expected value")
    if expected_author_zone:
        if metadata["xml_author_zone"] != expected_author_zone:
            issues.append("XML author zone does not match expected source value")
        if metadata["header_author_zone"] != expected_author_zone:
            issues.append("header author zone does not match expected source value")
        if metadata["body_author_chunk_zone"] != expected_author_zone:
            issues.append("body author zone does not match expected source value")
    if metadata["header_lap_count"] != expected_lap_count:
        issues.append("header lap count does not match expected value")
    if metadata["body_lap_count"] != expected_lap_count:
        issues.append("body lap count does not match expected value")
    if metadata["xml_lap_count"] != expected_lap_count:
        issues.append("XML lap count does not match expected value")
    if metadata["xml_has_clones"] != expected_has_clones:
        issues.append("XML hasclones flag does not match expected value")

    medal_expectations = {
        "author": expected_author,
        "gold": expected_gold,
        "silver": expected_silver,
        "bronze": expected_bronze,
    }
    medal_sources = {
        "header": (
            metadata["header_author_time_ms"],
            metadata["header_gold_time_ms"],
            metadata["header_silver_time_ms"],
            metadata["header_bronze_time_ms"],
        ),
        "xml": (
            metadata["xml_author_time_ms"],
            metadata["xml_gold_time_ms"],
            metadata["xml_silver_time_ms"],
            metadata["xml_bronze_time_ms"],
        ),
        "cp004": (
            metadata["cp004_author_time_ms"],
            metadata["cp004_gold_time_ms"],
            metadata["cp004_silver_time_ms"],
            metadata["cp004_bronze_time_ms"],
        ),
        "cp00a": (
            metadata["cp00a_author_time_ms"],
            metadata["cp00a_gold_time_ms"],
            metadata["cp00a_silver_time_ms"],
            metadata["cp00a_bronze_time_ms"],
        ),
    }
    expected_tuple = (
        medal_expectations["author"],
        medal_expectations["gold"],
        medal_expectations["silver"],
        medal_expectations["bronze"],
    )
    for source_name, source_values in medal_sources.items():
        if source_values != expected_tuple:
            issues.append(f"{source_name} medal values do not match expected timings")

    if metadata["thumbnail_jpeg"] == source_thumbnail_jpeg:
        issues.append("thumbnail was not updated from the source map thumbnail")

    return {
        "file": str(map_file),
        "ok": not issues,
        "issues": issues,
        "lap_count": expected_lap_count,
        "map_uid": expected_uid,
        "map_name": expected_name,
    }


def check_one_campaign_compliance(
    api: TrackmaniaApi,
    cfg: dict[str, Any],
    campaign: dict[str, Any],
    recorded_variant_uids: dict[int, str] | None = None,
) -> dict[str, Any]:
    playlist = campaign.get("playlist")
    if not isinstance(playlist, list) or not playlist:
        raise RuntimeError(f"Campaign has no playlist: {campaign}")
    first = playlist[0]
    if not isinstance(first, dict) or "mapUid" not in first:
        raise RuntimeError(f"Campaign playlist first item has no mapUid: {campaign}")

    source_map_uid = str(first["mapUid"])
    source_campaign_name = str(campaign.get("name", "Weekly Grand"))
    week = as_int(campaign.get("week"), -1)
    year = as_int(campaign.get("year"), -1)

    source_map_info = api.get_live_map_info(source_map_uid)
    if not source_map_info:
        raise RuntimeError(f"Could not load source map info for UID {source_map_uid}")

    source_map_name = str(source_map_info.get("name", source_map_uid))
    source_map_name_clean = clean_trackmania_name(source_map_name) or source_map_name
    base_map_uid = build_prefixed_uid(source_map_uid, cfg["map"]["uid_prefix"])
    context = {
        "source_campaign_name": source_campaign_name,
        "source_map_name": source_map_name,
        "source_map_name_clean": source_map_name_clean,
        "source_map_uid": source_map_uid,
        "new_map_uid": base_map_uid,
        "week": week,
        "year": year,
        "season_uid": str(campaign.get("seasonUid", source_map_uid)),
    }
    campaign_name_raw = render_template(cfg["campaign"]["name_template"], context)
    campaign_name = ensure_campaign_name(
        campaign_name_raw, cfg["campaign"]["truncate_to_20"]
    )

    work_dir = Path(cfg["paths"]["work_dir"])
    raw_dir = work_dir / "raw"
    processed_dir = work_dir / "processed"
    safe_stub = sanitize_for_filename(f"{year}_w{week}_{source_campaign_name}")
    source_file = raw_dir / f"{safe_stub}_{source_map_uid}.Map.Gbx"
    if not source_file.exists():
        return {
            "campaign_name": campaign_name,
            "ok": False,
            "issues": [f"source raw map is missing: {source_file}"],
            "variant_checks": [],
            "campaign_activity_found": False,
        }

    seed = read_map_variant_seed(source_file)
    context.update(
        build_target_author_metadata(
            cfg,
            context,
            seed,
            upload_author_account_id=api.account_id,
        )
    )
    variants = build_lap_variants(
        cfg,
        context,
        source_lap_count=seed["source_lap_count"],
        source_author_time_ms=seed["source_author_time_ms"],
        uid_overrides=recorded_variant_uids,
    )
    source_thumbnail_jpeg = read_variant_map_metadata(source_file)["thumbnail_jpeg"]

    variant_checks: list[dict[str, Any]] = []
    issues: list[str] = []
    for variant in variants:
        lap_count = as_int(variant["lap_count"], -1)
        variant_file = (
            processed_dir
            / f"{safe_stub}_{lap_count:03d}L_{variant['map_uid']}.Map.Gbx"
        )
        check = check_variant_file_compliance(
            variant_file,
            expected_variant=variant,
            source_thumbnail_jpeg=source_thumbnail_jpeg,
        )
        variant_checks.append(check)
        issues.extend(check["issues"])

    campaign_activity = find_existing_campaign_activity(
        api, as_int(cfg["club"]["club_id"], 0), campaign_name
    )
    if not campaign_activity:
        issues.append(f"club campaign activity not found by name: {campaign_name}")

    return {
        "campaign_name": campaign_name,
        "ok": not issues,
        "issues": issues,
        "variant_checks": variant_checks,
        "campaign_activity_found": campaign_activity is not None,
        "expected_map_uids": [str(variant["map_uid"]) for variant in variants],
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"processed": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"processed": {}}
    if not isinstance(payload, dict):
        return {"processed": {}}
    if "processed" not in payload or not isinstance(payload["processed"], dict):
        payload["processed"] = {}
    return payload


def collect_recorded_variant_uids(
    state: dict[str, Any], source_map_uid: str
) -> dict[int, str]:
    recorded: dict[int, str] = {}
    processed = state.get("processed", {})
    if not isinstance(processed, dict):
        return recorded

    for result in processed.values():
        if not isinstance(result, dict):
            continue
        if str(result.get("source_map_uid", "")).strip() != source_map_uid:
            continue
        variant_records = result.get("variant_records")
        if not isinstance(variant_records, list):
            continue
        for record in variant_records:
            if not isinstance(record, dict):
                continue
            lap_count = as_int(record.get("lap_count"), -1)
            map_uid = str(record.get("map_uid", "")).strip()
            if lap_count <= 0 or not map_uid:
                continue
            previous_uid = recorded.get(lap_count)
            if previous_uid and previous_uid != map_uid:
                raise RuntimeError(
                    f"Conflicting recorded UID for source {source_map_uid} lap {lap_count}: "
                    f"{previous_uid} vs {map_uid}"
                )
            recorded[lap_count] = map_uid
    return recorded


def campaign_order_key(result: dict[str, Any]) -> tuple[int, int, str]:
    year = as_int(result.get("year"), -1)
    week = as_int(result.get("week"), -1)
    if week < 0:
        match = re.search(r"\bw(\d{1,2})\b", str(result.get("campaign_name", "")), re.I)
        if match:
            week = as_int(match.group(1), -1)
    timestamp_utc = str(result.get("timestamp_utc", "")).strip()
    return year, week, timestamp_utc


def enforce_known_campaign_order(
    api: TrackmaniaApi,
    *,
    club_id: int,
    cfg: dict[str, Any],
    state: dict[str, Any],
) -> tuple[bool, int]:
    pinned_moved = False
    campaign_moves = 0

    if club_id <= 0 or not cfg["ordering"]["enabled"]:
        return pinned_moved, campaign_moves

    pinned_name = str(cfg["ordering"]["pinned_activity_name"]).strip()
    pinned_type = str(cfg["ordering"]["pinned_activity_type"]).strip().lower()
    pinned_position = as_int(cfg["ordering"]["pinned_position"], 0)
    first_campaign_position = as_int(cfg["ordering"]["processed_campaign_position"], 1)
    if first_campaign_position <= pinned_position:
        first_campaign_position = pinned_position + 1

    try:
        activities = api.get_club_activities(club_id, active=True)
    except ApiError as exc:
        log(f"Campaign order step skipped: failed to fetch activities ({exc}).")
        return pinned_moved, campaign_moves

    pinned_activity = find_activity_by_name(
        activities, pinned_name, activity_type=pinned_type
    )
    if pinned_activity:
        pinned_activity_id = activity_identifier(pinned_activity)
        current_pin_position = as_int(pinned_activity.get("position"), -1)
        if pinned_activity_id > 0 and current_pin_position != pinned_position:
            try:
                api.edit_activity(
                    club_id, pinned_activity_id, {"position": pinned_position}
                )
                pinned_moved = True
                log(
                    f"Moved pinned activity '{pinned_name}' to position {pinned_position}."
                )
                activities = api.get_club_activities(club_id, active=True)
            except ApiError as exc:
                log(
                    f"Pinned activity move failed ('{pinned_name}' -> {pinned_position}): {exc}"
                )
    else:
        log(
            f"Pinned activity not found ('{pinned_name}', type '{pinned_type or 'any'}')."
        )

    activities_by_id = {
        activity_identifier(activity): activity
        for activity in activities
        if activity_identifier(activity) > 0
    }
    ordered_campaigns: list[tuple[tuple[int, int, str], int, str]] = []
    seen_activity_ids: set[int] = set()
    processed = state.get("processed", {})
    if not isinstance(processed, dict):
        return pinned_moved, campaign_moves

    for result in processed.values():
        if not isinstance(result, dict):
            continue
        activity_id = as_int(result.get("activity_id"), 0)
        if activity_id <= 0 or activity_id in seen_activity_ids:
            continue
        activity = activities_by_id.get(activity_id)
        if not activity or activity.get("activityType") != "campaign":
            continue
        seen_activity_ids.add(activity_id)
        ordered_campaigns.append(
            (campaign_order_key(result), activity_id, str(result.get("campaign_name", "")))
        )

    ordered_campaigns.sort(key=lambda item: item[0], reverse=True)

    for index, (_, activity_id, campaign_name) in enumerate(ordered_campaigns):
        target_position = first_campaign_position + index
        current_position = as_int(
            activities_by_id.get(activity_id, {}).get("position"), -1
        )
        if current_position == target_position:
            continue
        try:
            api.edit_activity(club_id, activity_id, {"position": target_position})
            campaign_moves += 1
            log(
                f"Moved campaign activity {activity_id} ({campaign_name}) to position {target_position}."
            )
        except ApiError as exc:
            log(
                f"Campaign move failed ({activity_id} -> {target_position}, {campaign_name}): {exc}"
            )

    return pinned_moved, campaign_moves


def main() -> int:
    default_config_path = Path(__file__).resolve().parent.parent / "config.json"
    parser = argparse.ArgumentParser(description="Automate Cloneless Grands workflow.")
    parser.add_argument(
        "--config", default=str(default_config_path), help="Path to JSON config file."
    )
    parser.add_argument(
        "--offset", type=int, default=None, help="Override weekly_grands.offset."
    )
    parser.add_argument(
        "--length", type=int, default=None, help="Override weekly_grands.length."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Run read-only planning mode."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore processed-state and force processing.",
    )
    parser.add_argument(
        "--check-compliance",
        action="store_true",
        help="Read-only compliance check for generated lap variants and campaign presence.",
    )
    parser.add_argument(
        "--print-latest-weekly",
        action="store_true",
        help="Print metadata for the current latest Weekly Grand campaign and exit.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    try:
        cfg = deep_merge(DEFAULT_CONFIG, load_json_file(config_path))
        if args.offset is not None:
            cfg["weekly_grands"]["offset"] = args.offset
        if args.length is not None:
            cfg["weekly_grands"]["length"] = args.length
        validate_config(cfg)
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    if cfg["env"]["load_dotenv"]:
        dotenv_path = Path(cfg["env"]["dotenv_path"])
        if not dotenv_path.is_absolute():
            dotenv_path = (config_path.parent / dotenv_path).resolve()
        loaded = load_dotenv(
            dotenv_path, override=bool(cfg["env"]["override_existing_env"])
        )
        if loaded > 0:
            log(f"Dotenv loaded: {loaded} entries ({dotenv_path})")
        else:
            log(f"Dotenv loaded: 0 entries ({dotenv_path})")

    auth_mode = str(cfg["auth"].get("mode", "service_account")).strip().lower()
    if auth_mode == "service_account":
        login_env = str(cfg["auth"]["service_account_login_env"])
        password_env = str(cfg["auth"]["service_account_password_env"])
        credentials_label = "service account"
    else:
        login_env = str(cfg["auth"]["email_env"])
        password_env = str(cfg["auth"]["password_env"])
        credentials_label = "Ubisoft"

    login = os.getenv(login_env, "")
    password = os.getenv(password_env, "")
    if not login or not password:
        print(
            f"Missing {credentials_label} credentials. Set env vars {login_env} and {password_env}.",
            file=sys.stderr,
        )
        return 2

    api = TrackmaniaApi(
        cfg["auth"]["user_agent"], as_int(cfg["http"]["timeout_sec"], 45)
    )
    try:
        api.authorize(auth_mode, login, password)
    except Exception as exc:
        print(f"Authentication failed: {exc}", file=sys.stderr)
        return 1

    weekly_length = as_int(cfg["weekly_grands"]["length"], 1)
    weekly_offset = as_int(cfg["weekly_grands"]["offset"], 0)

    try:
        weekly_payload = api.get_weekly_grands(weekly_length, weekly_offset)
    except Exception as exc:
        print(f"Failed to fetch weekly grands: {exc}", file=sys.stderr)
        return 1

    campaign_list = weekly_payload.get("campaignList", [])
    if not isinstance(campaign_list, list) or not campaign_list:
        log("No Weekly Grands campaigns returned.")
        return 0

    if args.print_latest_weekly:
        latest_campaign = next(
            (campaign for campaign in campaign_list if isinstance(campaign, dict)),
            None,
        )
        if not latest_campaign:
            print("No valid Weekly Grands campaign payload found.", file=sys.stderr)
            return 1
        playlist = latest_campaign.get("playlist")
        first = playlist[0] if isinstance(playlist, list) and playlist else {}
        source_map_uid = (
            str(first.get("mapUid", "")).strip() if isinstance(first, dict) else ""
        )
        print(
            json.dumps(
                {
                    "week": as_int(latest_campaign.get("week"), -1),
                    "year": as_int(latest_campaign.get("year"), -1),
                    "season_uid": str(
                        latest_campaign.get("seasonUid", source_map_uid)
                    ).strip(),
                    "campaign_name": str(latest_campaign.get("name", "")).strip(),
                    "source_map_uid": source_map_uid,
                },
                indent=2,
            )
        )
        return 0

    state_path = Path(cfg["paths"]["state_file"])
    state = load_state(state_path)

    if args.check_compliance:
        compliance_results: list[dict[str, Any]] = []
        has_issues = False
        for campaign in campaign_list:
            if not isinstance(campaign, dict):
                continue
            recorded_variant_uids: dict[int, str] = {}
            playlist = campaign.get("playlist")
            if isinstance(playlist, list) and playlist:
                first = playlist[0]
                if isinstance(first, dict):
                    source_map_uid = str(first.get("mapUid", "")).strip()
                    if source_map_uid:
                        recorded_variant_uids = collect_recorded_variant_uids(
                            state, source_map_uid
                        )
            try:
                result = check_one_campaign_compliance(
                    api,
                    cfg,
                    campaign,
                    recorded_variant_uids=recorded_variant_uids,
                )
            except Exception as exc:
                result = {
                    "campaign_name": str(campaign.get("name", "Weekly Grand")),
                    "ok": False,
                    "issues": [str(exc)],
                    "variant_checks": [],
                    "campaign_activity_found": False,
                }
            compliance_results.append(result)
            if not result.get("ok", False):
                has_issues = True

        print(
            json.dumps(
                {
                    "mode": "check_compliance",
                    "ok": not has_issues,
                    "results": compliance_results,
                },
                indent=2,
            )
        )
        return 1 if has_issues else 0

    results: list[dict[str, Any]] = []

    for campaign in campaign_list:
        if not isinstance(campaign, dict):
            continue
        season_uid = str(campaign.get("seasonUid", ""))
        processed = state["processed"].get(season_uid)
        if processed and cfg["state"]["skip_processed"] and not args.force:
            log(f"Skip processed seasonUid={season_uid} (use --force to re-run)")
            continue
        try:
            previous_result = processed if isinstance(processed, dict) else None
            recorded_variant_uids: dict[int, str] = {}
            playlist = campaign.get("playlist")
            if isinstance(playlist, list) and playlist:
                first = playlist[0]
                if isinstance(first, dict):
                    source_map_uid = str(first.get("mapUid", "")).strip()
                    if source_map_uid:
                        recorded_variant_uids = collect_recorded_variant_uids(
                            state, source_map_uid
                        )
            result = process_one_campaign(
                api,
                cfg,
                campaign,
                dry_run=args.dry_run,
                force=args.force,
                recorded_variant_uids=recorded_variant_uids,
                previous_result=previous_result,
            )
            results.append(result)
            if not args.dry_run and season_uid:
                state["processed"][season_uid] = result
                save_json_file(state_path, state)
        except Exception as exc:
            print(
                f"Failed while processing campaign {campaign.get('name')}: {exc}",
                file=sys.stderr,
            )
            return 1

    if not args.dry_run and cfg["ordering"]["enabled"]:
        enforce_known_campaign_order(
            api,
            club_id=as_int(cfg["club"]["club_id"], 0),
            cfg=cfg,
            state=state,
        )

    if not results:
        log("No campaigns processed")
        return 0

    log("Run completed")
    print(json.dumps({"results": results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
