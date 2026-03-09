from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agent_app_dataset.internal_api import create_app


def test_delta_review_route_serves_parallel_surface(tmp_path: Path) -> None:
    repo_ui_dir = Path(__file__).resolve().parents[1] / "src" / "agent_app_dataset" / "ui"
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        ui_dir=repo_ui_dir,
    )
    client = TestClient(app)

    response = client.get("/delta-review")
    assert response.status_code == 200
    assert "Delta Review" in response.text

    script = client.get("/app/delta-review.js")
    assert script.status_code == 200
