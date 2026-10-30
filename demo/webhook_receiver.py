import hashlib
import hmac
import json
import os
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from time import time

deliveries: list[dict] = []
attempts: dict[str, int] = defaultdict(int)
signing_secret = os.environ["WEBHOOK_SIGNING_SECRET"]


class Handler(BaseHTTPRequestHandler):
    def respond(self, status: int, body: object) -> None:
        payload = json.dumps(body, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        if self.path == "/health":
            self.respond(200, {"status": "ok"})
        elif self.path == "/events":
            self.respond(200, deliveries)
        else:
            self.respond(404, {"detail": "Not found"})

    def do_POST(self) -> None:
        if self.path not in {"/success", "/flaky", "/fail"}:
            self.respond(404, {"detail": "Not found"})
            return
        body = self.rfile.read(int(self.headers["Content-Length"]))
        timestamp = self.headers.get("X-Webhook-Timestamp", "")
        signature = self.headers.get("X-Webhook-Signature", "")
        if not timestamp.isdecimal() or abs(time() - int(timestamp)) > 300:
            self.respond(401, {"detail": "Invalid webhook timestamp"})
            return
        expected = hmac.new(
            signing_secret.encode("utf-8"),
            timestamp.encode("ascii") + b"." + body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, "sha256=" + expected):
            self.respond(401, {"detail": "Invalid webhook signature"})
            return
        event = json.loads(body)
        event_id = event["event_id"]
        attempts[event_id] += 1
        status = 200
        if self.path == "/fail" or (self.path == "/flaky" and attempts[event_id] < 3):
            status = 503
        deliveries.append({"path": self.path, "status_code": status, "payload": event})
        self.respond(status, {"received": event_id, "attempt": attempts[event_id]})


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
