import json
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from feishu_integration import create_feishu_client


class CallbackHandler(BaseHTTPRequestHandler):
    auth_code = None
    auth_state = None
    error = None

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        CallbackHandler.auth_code = params.get("code", [None])[0]
        CallbackHandler.auth_state = params.get("state", [None])[0]
        CallbackHandler.error = params.get("error", [None])[0]

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            (
                "<html><body><h2>Feishu authorization received.</h2>"
                "<p>You can close this window and return to the terminal.</p>"
                "</body></html>"
            ).encode("utf-8")
        )

    def log_message(self, format, *args):
        return


def main():
    client = create_feishu_client()
    authorize_url, expected_state = client.build_authorization_url()

    parsed = urlparse(client.redirect_uri)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 8787

    server = HTTPServer((host, port), CallbackHandler)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    print("Open the following URL in your browser to authorize Feishu access:")
    print(authorize_url)
    try:
        webbrowser.open(authorize_url)
    except Exception:
        pass

    thread.join(timeout=300)
    server.server_close()

    if CallbackHandler.error:
        raise RuntimeError(f"Authorization failed: {CallbackHandler.error}")
    if not CallbackHandler.auth_code:
        raise RuntimeError("No authorization code received within 300 seconds")
    if CallbackHandler.auth_state != expected_state:
        raise RuntimeError("Authorization state mismatch")

    token_payload = client.exchange_code_for_user_token(CallbackHandler.auth_code)
    print("Feishu user authorization completed successfully.")
    print(
        json.dumps(
            {
                "token_file": client.user_token_file,
                "expires_in": token_payload.get("expires_in"),
                "refresh_expires_in": token_payload.get("refresh_expires_in"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
