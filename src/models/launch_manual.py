"""Script de lanzamiento manual de respaldo para SageMaker Training Jobs (Tarea A8).

Permite a Dev A ejecutar las corridas oficiales (E1..E6) en AWS SageMaker
en caso de contingencia o retraso del orquestador automático de Dev B.

Invariantes clave:
- TrainingJobName == run_id (Garantiza idempotencia y previene ejecuciones duplicadas).
- Métricas capturadas por CloudWatch mediante regexes de C5.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import boto3

# Métrica regex oficial del Contrato C5
SAGEMAKER_METRIC_DEFINITIONS = [
    {"Name": "cv_pr_auc_mean", "Regex": r"cv_pr_auc_mean=([0-9\.]+)"},
    {"Name": "cv_pr_auc_fold", "Regex": r"cv_pr_auc_fold=([0-9\.]+)"},
    {"Name": "train_pr_auc", "Regex": r"train_pr_auc=([0-9\.]+)"},
    {"Name": "n_train", "Regex": r"n_train=([0-9]+)"},
    {"Name": "n_pos_train", "Regex": r"n_pos_train=([0-9]+)"},
    {
        "Name": "n_features_transformadas",
        "Regex": r"n_features_transformadas=([0-9]+)",
    },
    {"Name": "train_seconds", "Regex": r"train_seconds=([0-9\.]+)"},
]

# Imagen ECR oficial de Scikit-Learn 1.3-1 en us-east-1
DEFAULT_SKLEARN_IMAGE = (
    "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:1.3-1-cpu-py3"
)


def lanzar_training_job_manual(
    run_id: str,
    model_name: str,
    variant: str = "V2",
    usa_pesos: str = "si",
    dataset_version: str = "v1",
    instance_type: str = "ml.m5.4xlarge",
    param_grid: dict[str, Any] | None = None,
    max_runtime_seconds: int = 86400,
    volume_size_gb: int = 50,
    role_arn: str | None = None,
    gold_bucket: str | None = None,
    region: str = "us-east-1",
    sagemaker_program: str = "train.py",
    sagemaker_submit_directory: str | None = None,
) -> dict[str, Any]:
    """Crea y despacha un Training Job en SageMaker respetando el contrato C5."""
    if not gold_bucket:
        gold_bucket = os.getenv("GOLD_BUCKET", "sunat-risk-scoring-gold")

    if "perf" in gold_bucket.lower():
        raise ValueError(
            f"El bucket '{gold_bucket}' pertenece a los experimentos de rendimiento "
            "y no puede usarse para entrenar modelos."
        )

    if not role_arn:
        role_arn = os.getenv("SAGEMAKER_ROLE_ARN", "")
        if not role_arn:
            # Intentar resolver vía IAM
            iam = boto3.client("iam", region_name=region)
            try:
                role_resp = iam.get_role(RoleName="sunat-ssco-sagemaker-execution-role")
                role_arn = role_resp["Role"]["Arn"]
            except Exception as err:
                raise ValueError(
                    "Debe especificarse role_arn o configurar SAGEMAKER_ROLE_ARN en el entorno."
                ) from err

    sagemaker_client = boto3.client("sagemaker", region_name=region)

    # REGLA DE IDEMPOTENCIA: El TrainingJobName es exactamente el run_id
    job_name = run_id

    s3_input_uri = f"s3://{gold_bucket}/model_inputs/dataset_version={dataset_version}/"
    s3_output_uri = f"s3://{gold_bucket}/model_artifacts/{run_id}"
    s3_preds_uri = (
        f"s3://{gold_bucket}/model_outputs/predictions/predictions_{run_id}.parquet"
    )
    s3_record_uri = f"s3://{gold_bucket}/model_runs/run_id={run_id}/run_record.json"

    print(f"[INFO] Verificando existencia previa de Training Job: {job_name}")
    try:
        desc = sagemaker_client.describe_training_job(TrainingJobName=job_name)
        status = desc["TrainingJobStatus"]
        print(
            f"[WARN] El Training Job {job_name} ya existe con estado: {status}. Omitiendo lanzamiento."
        )
        return desc
    except sagemaker_client.exceptions.ClientError:
        # No existe, proceder con el lanzamiento
        pass

    print(f"[INFO] Lanzando Training Job {job_name} en {instance_type}...")

    response = sagemaker_client.create_training_job(
        TrainingJobName=job_name,
        AlgorithmSpecification={
            "TrainingImage": DEFAULT_SKLEARN_IMAGE,
            "TrainingInputMode": "File",
            "MetricDefinitions": SAGEMAKER_METRIC_DEFINITIONS,
        },
        RoleArn=role_arn,
        InputDataConfig=[
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": s3_input_uri,
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
            }
        ],
        OutputDataConfig={
            "S3OutputPath": s3_output_uri,
        },
        ResourceConfig={
            "InstanceType": instance_type,
            "InstanceCount": 1,
            "VolumeSizeInGB": volume_size_gb,
        },
        StoppingCondition={
            "MaxRuntimeInSeconds": max_runtime_seconds,
        },
        HyperParameters={
            "sagemaker_program": sagemaker_program,
            "sagemaker_submit_directory": (
                sagemaker_submit_directory
                or f"s3://{gold_bucket}/code/sourcedir.tar.gz"
            ),
            "run_id": run_id,
            "model_name": model_name,
            "variant": variant,
            "usa_pesos": usa_pesos,
            "dataset_version": dataset_version,
            "ruta_salida_predicciones": s3_preds_uri,
            "ruta_salida_run_record": s3_record_uri,
            **({"param_grid_json": json.dumps(param_grid)} if param_grid else {}),
        },
    )

    print(
        f"[SUCCESS] SageMaker Training Job {job_name} despachado exitosamente. ARN: {response['TrainingJobArn']}"
    )
    return response


def lanzar_orquestador_sagemaker(
    run_tag: str,
    experiments: str = "ALL",
    dataset_version: str = "v1",
    instance_type: str = "ml.m5.4xlarge",
    n_jobs: int = 3,
    max_runtime_seconds: int = 432000,
    volume_size_gb: int = 100,
    role_arn: str | None = None,
    gold_bucket: str | None = None,
    region: str = "us-east-1",
) -> dict[str, Any]:
    """Lanza UN Training Job que ejecuta toda la matriz de experimentos en una instancia.

    El job publica cada resultado en S3 apenas termina, así que no depende de que
    quien lo lanzó siga vivo. Repetir el mismo run_tag reanuda sin repetir lo hecho.
    """
    bucket: str = gold_bucket or os.getenv("GOLD_BUCKET") or "sunat-risk-scoring-gold"
    if "perf" in bucket.lower():
        raise ValueError(
            f"El bucket '{bucket}' pertenece a los experimentos de rendimiento "
            "y no puede usarse para entrenar modelos."
        )
    role_arn = role_arn or os.getenv("SAGEMAKER_ROLE_ARN", "")
    if not role_arn:
        role_arn = boto3.client("iam", region_name=region).get_role(
            RoleName="sunat-ssco-sagemaker-execution-role"
        )["Role"]["Arn"]

    sm = boto3.client("sagemaker", region_name=region)
    job_name = f"ssco-all-{run_tag}"[:63]
    try:
        return sm.describe_training_job(TrainingJobName=job_name)
    except sm.exceptions.ClientError:
        pass  # no existe todavía

    return sm.create_training_job(
        TrainingJobName=job_name,
        AlgorithmSpecification={
            "TrainingImage": DEFAULT_SKLEARN_IMAGE,
            "TrainingInputMode": "File",
            "MetricDefinitions": SAGEMAKER_METRIC_DEFINITIONS,
        },
        RoleArn=role_arn,
        InputDataConfig=[
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": f"s3://{bucket}/model_inputs/dataset_version={dataset_version}/",
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
            }
        ],
        OutputDataConfig={"S3OutputPath": f"s3://{bucket}/model_artifacts/{job_name}"},
        ResourceConfig={
            "InstanceType": instance_type,
            "InstanceCount": 1,
            "VolumeSizeInGB": volume_size_gb,
        },
        StoppingCondition={"MaxRuntimeInSeconds": max_runtime_seconds},
        HyperParameters={
            "sagemaker_program": "orchestrator.py",
            "sagemaker_submit_directory": f"s3://{bucket}/code/sourcedir.tar.gz",
            "config": "/opt/ml/code/experiments.yml",
            "target": "runner",
            "experiments": experiments,
            "dataset_version": dataset_version,
            "run_tag": run_tag,
            "gold_bucket": bucket,
            "publicar_s3": "si",
            "reanudar": "si",
            "n_jobs": str(n_jobs),
        },
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Lanzador manual de respaldo en SageMaker (Dev A)"
    )
    parser.add_argument(
        "--run_id",
        type=str,
        required=True,
        help="Identificador de la corrida (e.g. ssco-run-E1-lr-v2-w1)",
    )
    parser.add_argument("--model_name", type=str, choices=["LR", "DT"], required=True)
    parser.add_argument("--variant", type=str, choices=["V1", "V2"], default="V2")
    parser.add_argument("--usa_pesos", type=str, choices=["si", "no"], default="si")
    parser.add_argument("--dataset_version", type=str, default="v1")
    parser.add_argument("--instance_type", type=str, default="ml.m5.4xlarge")
    parser.add_argument("--role_arn", type=str, default="")
    parser.add_argument("--gold_bucket", type=str, default="")
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    lanzar_training_job_manual(
        run_id=cli_args.run_id,
        model_name=cli_args.model_name,
        variant=cli_args.variant,
        usa_pesos=cli_args.usa_pesos,
        dataset_version=cli_args.dataset_version,
        instance_type=cli_args.instance_type,
        role_arn=cli_args.role_arn,
        gold_bucket=cli_args.gold_bucket,
    )
