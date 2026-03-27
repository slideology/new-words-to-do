import json
import logging
import secrets
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import requests

from config import FEISHU_CONFIG


logger = logging.getLogger(__name__)
APPEND_CHUNK_SIZE = 1000


def _column_letter(index):
    result = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


class FeishuClient:
    def __init__(
        self,
        app_id,
        app_secret,
        auth_mode="user",
        redirect_uri="http://127.0.0.1:8787/callback",
        user_token_file=".feishu_user_token.json",
        scopes=None,
        timeout=10,
    ):
        self.app_id = app_id
        self.app_secret = app_secret
        self.auth_mode = auth_mode
        self.redirect_uri = redirect_uri
        self.user_token_file = user_token_file
        self.scopes = scopes or []
        self.timeout = timeout
        self._tenant_access_token = None
        self._app_access_token = None

    @classmethod
    def from_config(cls):
        if not FEISHU_CONFIG["enabled"]:
            raise RuntimeError("Feishu integration is disabled")
        if not FEISHU_CONFIG["app_id"] or not FEISHU_CONFIG["app_secret"]:
            raise RuntimeError("Missing FEISHU_APP_ID or FEISHU_APP_SECRET")
        return cls(
            app_id=FEISHU_CONFIG["app_id"],
            app_secret=FEISHU_CONFIG["app_secret"],
            auth_mode=FEISHU_CONFIG["auth_mode"],
            redirect_uri=FEISHU_CONFIG["redirect_uri"],
            user_token_file=FEISHU_CONFIG["user_token_file"],
            scopes=FEISHU_CONFIG["scopes"],
        )

    def token_path(self):
        return Path(self.user_token_file)

    def build_authorization_url(self, state=None):
        actual_state = state or secrets.token_urlsafe(24)
        params = {
            "app_id": self.app_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.scopes),
            "state": actual_state,
        }
        return (
            f"https://accounts.feishu.cn/open-apis/authen/v1/authorize?{urlencode(params)}",
            actual_state,
        )

    def save_user_token(self, payload):
        token_data = {
            "access_token": payload["access_token"],
            "refresh_token": payload.get("refresh_token", ""),
            "expires_at": int(time.time()) + int(payload.get("expires_in", 0)),
            "refresh_expires_at": int(time.time()) + int(payload.get("refresh_expires_in", 0)),
            "token_type": payload.get("token_type", "Bearer"),
        }
        self.token_path().write_text(
            json.dumps(token_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def load_user_token(self):
        path = self.token_path()
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def get_tenant_access_token(self):
        if self._tenant_access_token:
            return self._tenant_access_token

        response = requests.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to get tenant_access_token: {data}")
        self._tenant_access_token = data["tenant_access_token"]
        return self._tenant_access_token

    def get_app_access_token(self):
        if self._app_access_token:
            return self._app_access_token

        response = requests.post(
            "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal/",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to get app_access_token: {data}")
        self._app_access_token = data["app_access_token"]
        return self._app_access_token

    def exchange_code_for_user_token(self, code):
        app_access_token = self.get_app_access_token()
        response = requests.post(
            "https://open.feishu.cn/open-apis/authen/v1/access_token",
            headers={
                "Authorization": f"Bearer {app_access_token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={
                "grant_type": "authorization_code",
                "code": code,
                "app_id": self.app_id,
                "app_secret": self.app_secret,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to get user_access_token: {data}")
        token_payload = data.get("data", {})
        self.save_user_token(token_payload)
        return token_payload

    def refresh_user_access_token(self, refresh_token):
        app_access_token = self.get_app_access_token()
        response = requests.post(
            "https://open.feishu.cn/open-apis/authen/v1/refresh_access_token",
            headers={
                "Authorization": f"Bearer {app_access_token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "app_id": self.app_id,
                "app_secret": self.app_secret,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to refresh user_access_token: {data}")
        token_payload = data.get("data", {})
        self.save_user_token(token_payload)
        return token_payload

    def get_user_access_token(self):
        token_data = self.load_user_token()
        if not token_data:
            raise RuntimeError(
                "Feishu user token not found. Run `python setup_feishu_user_auth.py` first."
            )

        if token_data.get("expires_at", 0) - 60 > time.time():
            return token_data["access_token"]

        refresh_token = token_data.get("refresh_token", "")
        if not refresh_token:
            raise RuntimeError(
                "Feishu user token expired and refresh token is missing. Run `python setup_feishu_user_auth.py` again."
            )

        refreshed = self.refresh_user_access_token(refresh_token)
        return refreshed["access_token"]

    def build_headers(self, as_user=None):
        use_user = self.auth_mode == "user" if as_user is None else as_user
        token = self.get_user_access_token() if use_user else self.get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    def get_wiki_node(self, wiki_token, as_user=None):
        response = requests.get(
            "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
            headers=self.build_headers(as_user=as_user),
            params={"token": wiki_token},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to get wiki node: {data}")
        return data.get("data", {}).get("node", {})

    def create_spreadsheet(self, title, as_user=None):
        response = requests.post(
            "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets",
            headers=self.build_headers(as_user=as_user),
            json={"title": title},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to create spreadsheet: {data}")
        return data.get("data", {}).get("spreadsheet", {})

    def get_sheet_metainfo(self, spreadsheet_token, as_user=None):
        response = requests.get(
            f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo",
            headers=self.build_headers(as_user=as_user),
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to get sheet metadata: {data}")
        return data.get("data", {}).get("sheets", [])

    def get_sheet_id_by_title(self, spreadsheet_token, title, as_user=None):
        for sheet in self.get_sheet_metainfo(spreadsheet_token, as_user=as_user):
            if sheet.get("title") == title:
                return sheet.get("sheetId")
        return None

    def get_sheet_id_by_token(self, spreadsheet_token, as_user=None):
        sheets = self.get_sheet_metainfo(spreadsheet_token, as_user=as_user)
        if not sheets:
            raise RuntimeError("No sheet found in spreadsheet")
        return sheets[0]["sheetId"]

    def batch_update_sheets(self, spreadsheet_token, requests_payload, as_user=None):
        response = requests.post(
            f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/sheets_batch_update",
            headers=self.build_headers(as_user=as_user),
            json={"requests": requests_payload},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to batch update sheets: {data}")
        return data.get("data", {})

    def ensure_sheet(self, spreadsheet_token, title, as_user=None):
        existing_sheet_id = self.get_sheet_id_by_title(
            spreadsheet_token,
            title,
            as_user=as_user,
        )
        if existing_sheet_id:
            return existing_sheet_id
        self.batch_update_sheets(
            spreadsheet_token,
            [{"addSheet": {"properties": {"title": title}}}],
            as_user=as_user,
        )
        created_sheet_id = self.get_sheet_id_by_title(
            spreadsheet_token,
            title,
            as_user=as_user,
        )
        if not created_sheet_id:
            raise RuntimeError(f"Created sheet not found for title: {title}")
        return created_sheet_id

    def rename_sheet(self, spreadsheet_token, sheet_id, title, as_user=None):
        self.batch_update_sheets(
            spreadsheet_token,
            [{"updateSheet": {"properties": {"sheetId": sheet_id, "title": title}}}],
            as_user=as_user,
        )

    def read_range(self, spreadsheet_token, value_range, as_user=None):
        encoded_range = requests.utils.quote(value_range, safe="")
        response = requests.get(
            f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{encoded_range}",
            headers=self.build_headers(as_user=as_user),
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to read sheet range: {data}")
        return data.get("data", {}).get("valueRange", {}).get("values", []) or []

    def write_range(self, spreadsheet_token, value_range, values, as_user=None):
        response = requests.put(
            f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values",
            headers=self.build_headers(as_user=as_user),
            json={"valueRange": {"range": value_range, "values": values}},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Failed to write sheet range: {data}")

    def append_rows(self, spreadsheet_token, sheet_id, headers, rows, as_user=None):
        last_col = _column_letter(len(headers))
        existing_values = self.read_range(
            spreadsheet_token,
            f"{sheet_id}!A1:{last_col}50000",
            as_user=as_user,
        )
        if not existing_values or [str(cell or "") for cell in existing_values[0][: len(headers)]] != headers:
            self.write_range(
                spreadsheet_token,
                f"{sheet_id}!A1:{last_col}1",
                [headers],
                as_user=as_user,
            )
            next_row = 2
        else:
            next_row = len(existing_values) + 1

        start = 0
        while start < len(rows):
            chunk = rows[start : start + APPEND_CHUNK_SIZE]
            start_row = next_row + start
            end_row = start_row + len(chunk) - 1
            self.write_range(
                spreadsheet_token,
                f"{sheet_id}!A{start_row}:{last_col}{end_row}",
                chunk,
                as_user=as_user,
            )
            start += APPEND_CHUNK_SIZE


def create_feishu_client():
    return FeishuClient.from_config()
