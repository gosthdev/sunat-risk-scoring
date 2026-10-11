"""
Tests unitarios para fetch_sagemaker_metrics.py.
Mockea el cliente boto3 para no necesitar credenciales AWS reales.
"""

import json
import os
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from src.models.fetch_sagemaker_metrics import INSTANCE_PRICE_MAP, get_job_cost_info


def _make_describe_response(
    instance_type: str = "ml.m5.xlarge",
    billable_seconds: int = 3600,
    training_seconds: int = 3550,
    status: str = "Completed",
    instance_count: int = 1,
) -> dict:
    """Construye una respuesta ficticia de describe_training_job."""
    t0 = datetime(2026, 10, 8, 10, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 10, 8, 11, 0, 0, tzinfo=UTC)
    return {
        "TrainingJobName": "ssco-run-test",
        "TrainingJobStatus": status,
        "ResourceConfig": {
            "InstanceType": instance_type,
            "InstanceCount": instance_count,
        },
        "BillableTimeInSeconds": billable_seconds,
        "TrainingTimeInSeconds": training_seconds,
        "CreationTime": t0,
        "TrainingEndTime": t1,
    }


@patch("src.models.fetch_sagemaker_metrics.boto3.client")
def test_get_job_cost_info_campos_esperados(mock_boto_client):
    """Verifica que get_job_cost_info retorne todos los campos del contrato C3."""
    mock_sm = MagicMock()
    mock_sm.describe_training_job.return_value = _make_describe_response()
    mock_boto_client.return_value = mock_sm

    result = get_job_cost_info("ssco-run-test", region="us-east-1")

    assert result["run_id"] == "ssco-run-test"
    assert result["status"] == "Completed"
    assert result["sagemaker_instance_type"] == "ml.m5.xlarge"
    assert result["instance_count"] == 1
    assert result["billable_seconds"] == 3600
    assert result["training_seconds"] == 3550
    assert "cost_usd_estimate" in result
    assert "creation_time" in result
    assert "end_time" in result


@patch("src.models.fetch_sagemaker_metrics.boto3.client")
def test_costo_estimado_correcto(mock_boto_client):
    """Verifica que el costo se calcule como billable_seconds / 3600 * precio."""
    mock_sm = MagicMock()
    mock_sm.describe_training_job.return_value = _make_describe_response(
        instance_type="ml.m5.xlarge",
        billable_seconds=1800,  # 30 minutos
    )
    mock_boto_client.return_value = mock_sm

    result = get_job_cost_info("ssco-run-test")

    precio = INSTANCE_PRICE_MAP["ml.m5.xlarge"]  # 0.269 USD/hora
    esperado = round(1800 / 3600 * precio, 4)
    assert result["cost_usd_estimate"] == esperado


@patch("src.models.fetch_sagemaker_metrics.boto3.client")
def test_instancia_desconocida_costo_cero(mock_boto_client):
    """Si la instancia no está en el mapa de precios, el costo debe ser 0.0."""
    mock_sm = MagicMock()
    mock_sm.describe_training_job.return_value = _make_describe_response(
        instance_type="ml.p3.2xlarge",  # no está en INSTANCE_PRICE_MAP
        billable_seconds=600,
    )
    mock_boto_client.return_value = mock_sm

    result = get_job_cost_info("ssco-run-test")

    assert result["cost_usd_estimate"] == 0.0


@patch("src.models.fetch_sagemaker_metrics.boto3.client")
def test_output_json_guarda_archivo(mock_boto_client, tmp_path):
    """Verifica que --output_json guarde el resultado en disco."""
    from src.models.fetch_sagemaker_metrics import main

    mock_sm = MagicMock()
    mock_sm.describe_training_job.return_value = _make_describe_response()
    mock_boto_client.return_value = mock_sm

    output_file = str(tmp_path / "cost.json")

    import sys

    argv_original = sys.argv
    sys.argv = [
        "fetch_sagemaker_metrics",
        "--run_id",
        "ssco-run-test",
        "--output_json",
        output_file,
    ]
    try:
        main()
    finally:
        sys.argv = argv_original

    assert os.path.exists(output_file)
    with open(output_file, encoding="utf-8") as f:
        data = json.load(f)
    assert data["run_id"] == "ssco-run-test"
