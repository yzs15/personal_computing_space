from fastapi.testclient import TestClient

from loom_v2.driver.app import create_app as create_driver_app
from loom_v2.observer.app import create_app as create_observer_app


def test_public_message_validation_keeps_domain_error_codes() -> None:
    with TestClient(create_observer_app()) as client:
        for body in ({}, {"text": None}, {"text": 1}):
            response = client.post("/api/v1/messages", json=body)
            assert response.status_code == 422
            assert response.json()["detail"] == "text_required"


def test_driver_message_validation_keeps_identity_error_codes() -> None:
    with TestClient(create_driver_app()) as client:
        response = client.post("/driver/v1/messages", json={"text": "hello"})
        assert response.status_code == 422
        assert response.json()["detail"] == "request_id_required"
