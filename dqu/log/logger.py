import datetime
import json
import base64
import hmac
import hashlib
import http.client
from urllib.parse import urlparse

class DQULoggerBase:
    """Base logger interface for DQU loggers."""
    def log(self, record: dict):
        raise NotImplementedError("Subclasses must implement log()")

class AzureLogAnalyticsLogger(DQULoggerBase):
    """
    Logger for sending structured JSON logs to Azure Log Analytics via HTTP Data Collector API.
    """
    def __init__(self, workspace_id: str, shared_key: str, log_type: str):
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.log_type = log_type
        self.endpoint = f"https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
        parsed = urlparse(self.endpoint)
        self.host = parsed.hostname
        self.path = parsed.path + "?" + parsed.query

    def build_signature(self, date: str, content_length: int, content_type: str, resource: str) -> str:
        string_to_hash = f"POST\n{content_length}\n{content_type}\nx-ms-date:{date}\n{resource}"
        bytes_to_hash = string_to_hash.encode("utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = hmac.new(decoded_key, bytes_to_hash, hashlib.sha256).digest()
        encoded_hash = base64.b64encode(encoded_hash).decode("utf-8")
        return f"SharedKey {self.workspace_id}:{encoded_hash}"

    def log(self, record: dict):
        try:
            body = json.dumps([record])
            content_type = "application/json"
            resource = "/api/logs"
            rfc1123date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
            signature = self.build_signature(rfc1123date, len(body), content_type, resource)
            headers = {
                "Content-Type": content_type,
                "Authorization": signature,
                "Log-Type": self.log_type,
                "x-ms-date": rfc1123date,
            }
            conn = http.client.HTTPSConnection(self.host)
            conn.request("POST", self.path, body=body, headers=headers)
            response = conn.getresponse()
            status = response.status
            data = response.read().decode()
            conn.close()
            if not (200 <= status <= 299):
                print(f"Azure Log Analytics: Failed to send log: {status} {data}")
        except Exception as e:
            print(f"AzureLogAnalyticsLogger Exception: {e}")

class GCPLogger(DQULoggerBase):
    """
    Logger for sending structured logs to Google Cloud Logging.
    """
    def __init__(self, log_name: str):
        try:
            from google.cloud import logging
            self.client = logging.Client()
            self.logger = self.client.logger(log_name)
        except ImportError:
            self.logger = None
            print("google-cloud-logging is not installed.")
        except Exception as e:
            self.logger = None
            print(f"GCPLogger initialization error: {e}")

    def log(self, record: dict):
        try:
            if self.logger:
                self.logger.log_struct(record)
            else:
                print("GCPLogger: logger not initialized.")
        except Exception as e:
            print(f"GCPLogger Exception: {e}")

class ConsoleLogger(DQULoggerBase):
    """
    Simple logger that prints logs to the console.
    """
    def log(self, record: dict):
        try:
            print(json.dumps(record, indent=2))
        except Exception as e:
            print(f"ConsoleLogger Exception: {e}")

def log_dqu_results(results, loggers):
    """
    Logs each DQU result (with dqu_score) using the provided logger(s).
    Only logs results with status 'Success'.
    """
    results = json.loads(results)
    for result in results:
        try:
            if isinstance(result, dict):
                if result.get("status") == "Success":
                    total = result.get("dqu_total_count", 0)
                    passed = result.get("dqu_passed_count", 0)
                    result["dqu_score"] = round((passed / total) * 100, 2) if total else 0.0
                else:
                    result["dqu_score"] = 0.0
                for logger in loggers:
                    try:
                        logger.log(result)
                    except Exception as e:
                        print(f"Logger {logger.__class__.__name__} failed: {e}")
        except Exception as e:
            print(f"Error processing result: {e}")
