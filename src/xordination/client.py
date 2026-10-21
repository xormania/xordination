"""Authenticated broker client; local credentials never traverse ambient proxies."""
from http.client import HTTPException
import ipaddress
import json
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from urllib.error import HTTPError, URLError


class BrokerError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # A participant credential is valid only for the explicitly selected
        # broker. Even a same-origin redirect can change the expected endpoint.
        return None


def _opener(endpoint):
    parsed = urlsplit(endpoint)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise ValueError("Endpoint must be an HTTP(S) URL without credentials, query or fragment")
    local = parsed.hostname.lower() == "localhost"
    try:
        local = local or ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        pass
    return build_opener(ProxyHandler({}), NoRedirect()) if local else build_opener(NoRedirect())


def health(endpoint, timeout=5):
    """Read the unauthenticated service health document; no credential is sent."""
    opener = _opener(endpoint)
    try:
        with opener.open(endpoint.rstrip("/") + "/health", timeout=timeout) as response:
            result = json.load(response)
    except (URLError, TimeoutError, OSError, HTTPException):
        raise BrokerError("connection_error", "Broker health is unavailable") from None
    except ValueError:
        result = None
    if not isinstance(result, dict):
        raise BrokerError("invalid_response", "Broker returned an invalid response")
    return result


class Client:
    def __init__(self, endpoint, token, timeout=40):
        opener = _opener(endpoint)
        if not isinstance(token, str) or not token:
            raise ValueError("A participant token is required")
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.opener = opener

    def call(self, operation, **params):
        payload = json.dumps({"operation": operation, "params": params},
                             allow_nan=False).encode("utf-8")
        request = Request(self.endpoint + "/rpc", data=payload, method="POST",
                          headers={"Authorization": "Bearer " + self.token,
                                   "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                result = json.load(response)
        except HTTPError as exc:
            try:
                value = json.load(exc)
                error = value.get("error", {})
            except (ValueError, AttributeError):
                error = {}
            raise BrokerError(error.get("code", "http_error"),
                              error.get("message", "Broker returned HTTP " + str(exc.code))) from None
        except (URLError, TimeoutError) as exc:
            raise BrokerError("connection_error", str(exc.reason) if isinstance(exc, URLError)
                              else "Broker request timed out") from None
        if not isinstance(result, dict) or not isinstance(result.get("ok"), bool):
            raise BrokerError("invalid_response", "Broker returned an invalid response")
        if not result["ok"]:
            error = result.get("error", {})
            raise BrokerError(error.get("code", "broker_error"),
                              error.get("message", "Broker operation failed"))
        return result.get("result")

