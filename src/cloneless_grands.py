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
import subprocess
import sys
from typing import Any
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
        "transform_mode": "pure_python",
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
    transform_mode = str(cfg["map"].get("transform_mode", "pure_python")).strip()
    if transform_mode not in {"pure_python", "legacy"}:
        raise ConfigError("map.transform_mode must be 'pure_python' or 'legacy'.")

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


def rewrite_map_info_uids(data: Any, source_uid: str, new_uid: str) -> int:
    updated = 0

    def walk(node: Any) -> None:
        nonlocal updated
        if isinstance(node, dict):
            map_info = node.get("mapInfo")
            if isinstance(map_info, dict) and map_info.get("id") == source_uid:
                map_info["id"] = new_uid
                updated += 1
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(data)
    return updated


def transform_map_pure_python(
    source_file: Path,
    stripped_file: Path,
    output_file: Path,
    source_uid: str,
    new_uid: str,
) -> int:
    parse_file, generate_file = import_gbxpy()
    stripped_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    data = parse_file(str(source_file), recursive=False)
    try:
        strip_validation_ghost_pure_python(data)
        stripped_file.write_bytes(generate_file(data))

        updated = rewrite_map_info_uids(data, source_uid, new_uid)
        if updated < 2:
            raise RuntimeError(
                f"Expected to rewrite at least 2 mapInfo ids, updated={updated}."
            )
        output_file.write_bytes(generate_file(data))
        return updated
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


class TrackmaniaApi:
    def __init__(self, user_agent: str, timeout_sec: int) -> None:
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.user_agent = user_agent
        self.core_token: str | None = None
        self.live_token: str | None = None

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

    def authorize(self, email: str, password: str) -> None:
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
        live_payload = self._request_json(
            "POST",
            f"{CORE_BASE_URL}/v2/authentication/token/ubiservices",
            headers={
                "Authorization": f"ubi_v1 t={ticket}",
                "Content-Type": "application/json",
            },
            json_body={"audience": "NadeoLiveServices"},
        )
        self.live_token = (
            live_payload.get("accessToken") if isinstance(live_payload, dict) else None
        )
        if not self.live_token:
            raise ApiError(
                f"NadeoLiveServices auth response missing accessToken: {live_payload}"
            )

        log("Requesting NadeoServices token...")
        core_payload = self._request_json(
            "POST",
            f"{CORE_BASE_URL}/v2/authentication/token/ubiservices",
            headers={
                "Authorization": f"ubi_v1 t={ticket}",
                "Content-Type": "application/json",
            },
            json_body={"audience": "NadeoServices"},
        )
        self.core_token = (
            core_payload.get("accessToken") if isinstance(core_payload, dict) else None
        )
        if not self.core_token:
            raise ApiError(
                f"NadeoServices auth response missing accessToken: {core_payload}"
            )

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
        map_uid: str,
        map_name: str,
    ) -> dict[str, Any]:
        if not self.core_token:
            raise RuntimeError("Not authenticated for core services.")
        files = [
            ("authorScore", (None, str(as_int(source_map_info.get("authorTime"), -1)))),
            ("goldScore", (None, str(as_int(source_map_info.get("goldTime"), -1)))),
            ("silverScore", (None, str(as_int(source_map_info.get("silverTime"), -1)))),
            ("bronzeScore", (None, str(as_int(source_map_info.get("bronzeTime"), -1)))),
            ("author", (None, str(source_map_info.get("author", "")))),
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
                f"{CORE_BASE_URL}/maps/",
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
        self, club_id: int, name: str, map_uid: str, folder_id: int
    ) -> dict[str, Any]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        payload = self._request_json(
            "POST",
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/campaign/create",
            headers=self._auth_header(self.live_token),
            json_body={
                "name": name,
                "playlist": [{"position": 0, "mapUid": map_uid}],
                "folderId": folder_id,
            },
        )
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected create campaign response: {payload}")
        return payload

    def edit_campaign(
        self, club_id: int, campaign_id: int, name: str, map_uid: str
    ) -> dict[str, Any]:
        if not self.live_token:
            raise RuntimeError("Not authenticated for live services.")
        payload = self._request_json(
            "POST",
            f"{LIVE_BASE_URL}/api/token/club/{club_id}/campaign/{campaign_id}/edit",
            headers=self._auth_header(self.live_token),
            json_body={
                "name": name,
                "playlist": [{"position": 0, "mapUid": map_uid}],
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
    new_map_uid = build_prefixed_uid(source_map_uid, cfg["map"]["uid_prefix"])

    context = {
        "source_campaign_name": source_campaign_name,
        "source_map_name": source_map_name,
        "source_map_name_clean": source_map_name_clean,
        "source_map_uid": source_map_uid,
        "new_map_uid": new_map_uid,
        "week": week,
        "year": year,
        "season_uid": season_uid,
    }

    map_name = (
        render_template(cfg["map"]["name_template"], context).strip() or source_map_name
    )
    campaign_name_raw = render_template(cfg["campaign"]["name_template"], context)
    campaign_name = ensure_campaign_name(
        campaign_name_raw, cfg["campaign"]["truncate_to_20"]
    )

    log(f"Weekly source: {source_campaign_name} | map UID: {source_map_uid}")
    log(f"Cloneless UID: {new_map_uid}")
    log(f"Target campaign name: {campaign_name}")

    uploaded_map_uid = new_map_uid
    uploaded_map_payload: dict[str, Any] | None = None
    thumbnail_candidates: list[str] = []

    existing_map = None
    if cfg["map"]["allow_reuse_existing_uid"]:
        existing_map = api.get_live_map_info(new_map_uid, allow_missing=True)
        if existing_map:
            log(f"Map UID already exists; reusing map: {new_map_uid}")
            uploaded_map_payload = existing_map
            append_unique_url(thumbnail_candidates, existing_map.get("thumbnailUrl"))
            append_unique_url(
                thumbnail_candidates, existing_map.get("mediaUrlPngLarge")
            )
            append_unique_url(
                thumbnail_candidates, existing_map.get("mediaUrlPngMedium")
            )
            append_unique_url(
                thumbnail_candidates, existing_map.get("mediaUrlPngSmall")
            )
            append_unique_url(thumbnail_candidates, existing_map.get("mediaUrl"))

    work_dir = Path(cfg["paths"]["work_dir"])
    raw_dir = work_dir / "raw"
    processed_dir = work_dir / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    safe_stub = sanitize_for_filename(f"{year}_w{week}_{source_campaign_name}")
    source_file = raw_dir / f"{safe_stub}_{source_map_uid}.Map.Gbx"
    stripped_file = processed_dir / f"{safe_stub}_{source_map_uid}.stripped.Map.Gbx"
    cloneless_file = processed_dir / f"{safe_stub}_{new_map_uid}.Map.Gbx"

    if not uploaded_map_payload:
        download_url = source_map_info.get("downloadUrl")
        if not isinstance(download_url, str) or not download_url:
            raise RuntimeError(
                f"Source map does not expose downloadUrl: {source_map_info}"
            )

        if dry_run:
            log(f"[DRY-RUN] Download source map -> {source_file}")
            log(f"[DRY-RUN] Strip validation replay -> {stripped_file}")
            log(f"[DRY-RUN] Rewrite UID -> {cloneless_file}")
            log(f"[DRY-RUN] Upload map UID -> {new_map_uid}")
            uploaded_map_uid = new_map_uid
        else:
            log("Download source map")
            api.download_file(download_url, source_file)

            transform_mode = str(cfg["map"].get("transform_mode", "pure_python"))
            if transform_mode == "pure_python":
                updated = transform_map_pure_python(
                    source_file,
                    stripped_file,
                    cloneless_file,
                    source_map_uid,
                    new_map_uid,
                )
                log(f"Map transform complete (pure python), mapInfo ids updated={updated}")
            else:
                strip_cmd = [
                    cfg["map"]["strip_exe"],
                    str(source_file),
                    str(stripped_file),
                ]
                strip_note = cfg["map"]["strip_note"]
                if strip_note:
                    strip_cmd.append(strip_note)
                run_command(strip_cmd)

                mode = cfg["map"]["uid_rewriter"]["mode"]
                if mode == "internal_replace":
                    replaced = rewrite_uid_internal(
                        stripped_file, cloneless_file, source_map_uid, new_map_uid
                    )
                    log(f"UID rewrite complete (internal), replaced={replaced}")
                else:
                    rewrite_uid_external(
                        stripped_file,
                        cloneless_file,
                        source_map_uid,
                        new_map_uid,
                        cfg["map"]["uid_rewriter"]["command_template"],
                    )
                    log("UID rewrite complete (external)")

            log("Upload map to Nadeo Core")
            uploaded_map_payload = api.upload_map(
                cloneless_file,
                source_map_info,
                map_uid=new_map_uid,
                map_name=map_name,
            )
            uploaded_map_uid = extract_map_uid(uploaded_map_payload, new_map_uid)
            log(f"Map upload complete, uid={uploaded_map_uid}")
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
            append_unique_url(
                thumbnail_candidates, uploaded_map_payload.get("mediaUrl")
            )

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

    if uploaded_map_uid:
        uploaded_map_info = api.get_live_map_info(uploaded_map_uid, allow_missing=True)
        if uploaded_map_info:
            append_unique_url(
                thumbnail_candidates, uploaded_map_info.get("thumbnailUrl")
            )
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
        if existing_activity:
            log(
                f"[DRY-RUN] Edit campaign {existing_activity.get('campaignId')} with map UID {uploaded_map_uid}"
            )
        else:
            log(
                f"[DRY-RUN] Create campaign in club {club_id} with map UID {uploaded_map_uid}"
            )
        if cfg["club"]["upload_activity_media_from_map_thumbnail"]:
            if thumbnail_candidates:
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
                f"[DRY-RUN] Add map UID {uploaded_map_uid} to bucket {cfg['club_bucket']['bucket_id']}"
            )
        return {
            "season_uid": season_uid,
            "source_map_uid": source_map_uid,
            "new_map_uid": uploaded_map_uid,
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
            club_id, campaign_id, campaign_name, uploaded_map_uid
        )
        activity_id = as_int(existing_activity.get("activityId"), 0)
    else:
        campaign_payload = api.create_campaign(
            club_id, campaign_name, uploaded_map_uid, folder_id
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
        if not thumbnail_candidates:
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
        api.add_map_to_bucket(club_id, bucket_id, uploaded_map_uid)
        log(f"Map added to bucket {bucket_id}: {uploaded_map_uid}")

    return {
        "season_uid": season_uid,
        "source_map_uid": source_map_uid,
        "new_map_uid": uploaded_map_uid,
        "campaign_name": campaign_name,
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

    email = os.getenv(cfg["auth"]["email_env"], "")
    password = os.getenv(cfg["auth"]["password_env"], "")
    if not email or not password:
        print(
            f"Missing Ubisoft credentials. Set env vars {cfg['auth']['email_env']} and {cfg['auth']['password_env']}.",
            file=sys.stderr,
        )
        return 2

    api = TrackmaniaApi(
        cfg["auth"]["user_agent"], as_int(cfg["http"]["timeout_sec"], 45)
    )
    try:
        api.authorize(email, password)
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

    state_path = Path(cfg["paths"]["state_file"])
    state = load_state(state_path)

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
            result = process_one_campaign(
                api,
                cfg,
                campaign,
                dry_run=args.dry_run,
                force=args.force,
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

    if not results:
        log("No campaigns processed")
        return 0

    log("Run completed")
    print(json.dumps({"results": results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
