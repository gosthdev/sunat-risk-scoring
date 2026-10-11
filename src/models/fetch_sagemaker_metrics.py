"""
Utilidad para obtener métricas de costo y duración de SageMaker Training Jobs.

Consulta la API de SageMaker dado un run_id y retorna los campos de costo
para incluirlos en el run_record.json (Contrato C3).

Uso:
    python -m src.models.fetch_sagemaker_metrics --run_id ssco-run-E1-lr-v2-w1
    python -m src.models.fetch_sagemaker_metrics --run_id ssco-run-E1-lr-v2-w1 --output_json /tmp/cost.json

Precios de referencia on-demand us-east-1:
    ml.m5.xlarge  → $0.269 USD/hora
    ml.m5.2xlarge → $0.538 USD/hora
"""

import argparse
import json
import os

import boto3

# Precios on-demand us-east-1 (USD/hora). Actualizar si cambia la instancia.
INSTANCE_PRICE_MAP: dict[str, float] = {
    "ml.m5.large": 0.134,
    "ml.m5.xlarge": 0.269,
    "ml.m5.2xlarge": 0.538,
    "ml.m5.4xlarge": 1.075,
    "ml.t3.medium": 0.050,
}


def get_job_cost_info(run_id: str, region: str = "us-east-1") -> dict:
    """Consulta SageMaker y retorna campos de costo para el run_record.json.

    Args:
        run_id: TrainingJobName en SageMaker (equivale al run_id del Contrato C3).
        region: Región AWS donde está el Training Job.

    Returns:
        Diccionario con run_id, status, instance_type, billable_seconds y costo estimado.
    """
    client = boto3.client("sagemaker", region_name=region)
    desc = client.describe_training_job(TrainingJobName=run_id)

    instance_type: str = desc["ResourceConfig"]["InstanceType"]
    billable_seconds: int = desc.get("BillableTimeInSeconds", 0)
    price_per_hour: float = INSTANCE_PRICE_MAP.get(instance_type, 0.0)

    return {
        "sagemaker_job_name": run_id,
        "status": desc["TrainingJobStatus"],
        "instancia": instance_type,
        "instance_count": desc["ResourceConfig"]["InstanceCount"],
        "segundos_facturables": float(billable_seconds),
        "training_seconds": desc.get("TrainingTimeInSeconds", 0),
        "costo_estimado_usd": round(billable_seconds / 3600 * price_per_hour, 4),
        "creation_time": str(desc.get("CreationTime", "")),
        "end_time": str(desc.get("TrainingEndTime", "")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Obtiene métricas de costo de SageMaker Training Jobs"
    )
    parser.add_argument(
        "--run_id",
        type=str,
        required=True,
        help="TrainingJobName en SageMaker (= run_id del Contrato C3)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.getenv("AWS_REGION", "us-east-1"),
        help="Región AWS (default: us-east-1 o var. AWS_REGION)",
    )
    parser.add_argument(
        "--output_json",
        type=str,
        default="",
        help="Ruta opcional para guardar el resultado en JSON",
    )
    args = parser.parse_args()

    info = get_job_cost_info(args.run_id, region=args.region)
    print(json.dumps(info, indent=2, ensure_ascii=False))

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(info, f, indent=2, ensure_ascii=False)
        print(f"\n[OK] Guardado en {args.output_json}")


if __name__ == "__main__":
    main()
