from prefect import get_client, get_run_logger
from prefect.exceptions import ObjectNotFound
from prefect.client.schemas.actions import WorkPoolCreate
from kubernetes_asyncio import client as k8_client
from kubernetes_asyncio.client import ApiClient


async def create_work_pool(work_pool_name: str):

    logger = get_run_logger()
    try:
        logger.info("Check the client")
        async with get_client() as client:
            try:

                work_pool = await client.read_work_pool(work_pool_name)
                # await start_worker(work_pool_name=work_pool_name)
                logger.info(
                    f"Work pool '{work_pool_name}' already exists with ID: {work_pool.id}")
            except ObjectNotFound:
                worlpoolCreate = WorkPoolCreate(
                    name=work_pool_name,
                    type="kubernetes",
                    base_job_template=await job_base_template())
                work_pool = await client.create_work_pool(
                    work_pool=worlpoolCreate,
                    overwrite=True,
                )
                logger.info(
                    f"Created work pool '{work_pool.name}' with ID: {work_pool.id}")
    except Exception as e:
        logger.error(f"Error on while create work pool: {e}")


async def job_base_template() -> str:
    return {
        "job_configuration": {
                "job_manifest": await create_job_manifest()
            },
        "variables": {
            "type": "object",
            "properties": {
                "image": {
                    "type": "string",
                    "default": "prefecthq/prefect:3-latest"
                },
                "command": {
                    "type": "string",
                    "default": ""
                },
                "api_url": {
                    "type": "string"
                },
                "ttlSecondsAfterFinished": {
                    "type": "integer",
                    "default": 300
                },
                "name": {
                    "type": "string"
                },
                "flow_run_id": {
                    "type": "string"
                },
                "flow_run_name": {
                    "type": "string"
                },
                "deployment_id": {
                    "type": "string"
                },
                "deployment_name": {
                    "type": "string"
                },
                "work_pool_name": {
                    "type": "string"
                },
                "task_run_id": {
                    "type": "string"
                }
            },
            "required": ["image", "api_url"]
        }
    }


async def create_job_manifest() -> str:
    manifest_file = k8_client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=k8_client.V1ObjectMeta(
            name="{{ name }}",
            labels={
                "prefect.io/work-pool": "{{ work_pool_name }}",
                "prefect.io/flow-run-id": "{{ flow_run_id }}",
                "prefect.io/flow-run-name": "{{ flow_run_name }}",
                "prefect.io/deployment-id": "{{ deployment_id }}",
                "prefect.io/deployment-name": "{{ deployment_name }}"
            }

        ),
        spec=k8_client.V1JobSpec(
            completions=1,
            parallelism=1,
            
            template=k8_client.V1JobTemplateSpec(
                metadata=k8_client.V1PodTemplateSpec(
                    metadata=k8_client.V1ObjectMeta(
                        name="{{ name }}",
                        labels={
                            "prefect.io/work-pool": "{{ work_pool_name }}",
                            "prefect.io/flow-run-id": "{{ flow_run_id }}",
                            "prefect.io/flow-run-name": "{{ flow_run_name }}",
                            "prefect.io/deployment-id": "{{ deployment_id }}",
                            "prefect.io/deployment-name": "{{ deployment_name }}"
                        }

                    )
                ),
                spec=k8_client.V1PodSpec(
                    service_account_name="prefect-worker",
                    restart_policy="Never",
                    containers=[
                        k8_client.V1Container(
                            name="prefect-job",
                            image="{{ image }}",
                            env=[
                                k8_client.V1EnvVar(
                                    name="PREFECT_API_URL", value="{{ api_url }}"),
                                k8_client.V1EnvVar(
                                    name="PREFECT_FLOW_RUN_ID", value="{{ flow_run_id }}"),
                                k8_client.V1EnvVar(
                                    name="PREFECT_FLOW_RUN_NAME", value="{{ flow_run_name }}"),
                                k8_client.V1EnvVar(
                                    name="PREFECT_TASK_RUN_ID", value="{{ task_run_id }}"),
                            ],
                            command=[]
                        )
                    ]
                )
            )
        )
    )
    manifest_disct: any = None
    async with ApiClient() as api_client:
        manifest_disct = api_client.sanitize_for_serialization(manifest_file)
    return manifest_disct


import subprocess

async def start_worker(work_pool_name: str):
    try:
        subprocess.run(
            ["prefect", "worker", "start", "-p", work_pool_name],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to start worker: {e}")