# src/integrations/adf_client.py
import time
import requests
from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class AdfRunResult:
    run_id: str
    status: str
    message: Optional[str] = None
    duration_ms: Optional[int] = None

class AdfClient:
    """
    Triggers and monitors ADF pipelines via Azure Management REST API.
    """

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def _get_arm_token(self) -> str:
        # ARM resource for management.azure.com
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "https://management.azure.com/.default"
        }
        r = requests.post(url, data=data, timeout=30)
        r.raise_for_status()
        return r.json()["access_token"]

    def create_run(self, subscription_id: str, resource_group: str, factory_name: str,
                   pipeline_name: str, parameters: Dict[str, Any],
                   api_version: str = "2018-06-01") -> str:
        token = self._get_arm_token()
        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{factory_name}"
            f"/pipelines/{pipeline_name}/createRun?api-version={api_version}"
        )
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {"parameters": parameters}
        r = requests.post(url, headers=headers, json=body, timeout=60)
        r.raise_for_status()
        return r.json()["runId"]  # per CreateRun response :contentReference[oaicite:3]{index=3}

    def get_run(self, subscription_id: str, resource_group: str, factory_name: str,
                run_id: str, api_version: str = "2018-06-01") -> AdfRunResult:
        token = self._get_arm_token()
        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.DataFactory/factories/{factory_name}"
            f"/pipelineruns/{run_id}?api-version={api_version}"
        )
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        j = r.json()
        return AdfRunResult(
            run_id=run_id,
            status=j.get("status"),
            message=j.get("message"),
            duration_ms=j.get("durationInMs"),
        )  # fields documented in Get response :contentReference[oaicite:4]{index=4}

    def wait_for_completion(self, subscription_id: str, resource_group: str, factory_name: str,
                            run_id: str, poll_seconds: int, timeout_minutes: int) -> AdfRunResult:
        deadline = time.time() + timeout_minutes * 60
        while True:
            res = self.get_run(subscription_id, resource_group, factory_name, run_id)
            if res.status in ("Succeeded", "Failed", "Cancelled"):
                return res
            if time.time() > deadline:
                return AdfRunResult(run_id=run_id, status="TimedOut", message="ADF run timed out")
            time.sleep(poll_seconds)
