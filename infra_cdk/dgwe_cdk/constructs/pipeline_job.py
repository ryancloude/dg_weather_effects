from __future__ import annotations

from dataclasses import dataclass

from aws_cdk import Duration
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_ssm as ssm
from constructs import Construct

from dgwe_cdk.config.jobs import PipelineJobDefinition
from dgwe_cdk.config.settings import PipelineSettings


@dataclass(frozen=True)
class PipelineDataAccess:
    data_bucket_name: str
    data_table_name: str
    athena_results_bucket_name: str | None


class PipelineJob(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        settings: PipelineSettings,
        definition: PipelineJobDefinition,
        repository: ecr.IRepository,
        config_parameters: dict[str, ssm.IStringParameter],
        data_access: PipelineDataAccess,
    ) -> None:
        super().__init__(scope, construct_id)

        self.definition = definition

        self.log_group = logs.LogGroup(
            self,
            "LogGroup",
            log_group_name=f"/{settings.resource_prefix}/ecs/{definition.job_name}",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        self.execution_role = iam.Role(
            self,
            "ExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )
        for parameter in config_parameters.values():
            parameter.grant_read(self.execution_role)

        self.task_role = iam.Role(
            self,
            "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description=f"Task role for {definition.job_name}",
        )
        self._grant_data_access(
            settings=settings,
            data_access=data_access,
            definition=definition,
            role=self.task_role,
        )

        self.task_definition = ecs.FargateTaskDefinition(
            self,
            "TaskDefinition",
            cpu=definition.cpu,
            memory_limit_mib=definition.memory_mib,
            execution_role=self.execution_role,
            task_role=self.task_role,
            family=f"{settings.resource_prefix}-{definition.job_name}",
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.X86_64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )

        secrets = {
            env_key: ecs.Secret.from_ssm_parameter(config_parameters[env_key])
            for env_key in definition.env_keys
        }

        self.container = self.task_definition.add_container(
            "Container",
            container_name=definition.container_name,
            image=ecs.ContainerImage.from_ecr_repository(
                repository,
                tag=settings.image_tag,
            ),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix=definition.job_name,
                log_group=self.log_group,
            ),
            environment={
                "APP_ENV": settings.app_env,
            },
            secrets=secrets,
        )

    def _grant_data_access(
        self,
        *,
        settings: PipelineSettings,
        data_access: PipelineDataAccess,
        definition: PipelineJobDefinition,
        role: iam.Role,
    ) -> None:
        account = settings.aws_region  # placeholder to satisfy linting; replaced below
        del account

        role.add_to_policy(
            iam.PolicyStatement(
                sid="PrimaryDataBucketAccess",
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::{data_access.data_bucket_name}",
                    f"arn:aws:s3:::{data_access.data_bucket_name}/*",
                ],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                sid="ProjectMetadataTableAccess",
                actions=[
                    "dynamodb:BatchGetItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:ConditionCheckItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:UpdateItem",
                ],
                resources=[
                    f"arn:aws:dynamodb:{settings.aws_region}:*:table/{data_access.data_table_name}",
                    f"arn:aws:dynamodb:{settings.aws_region}:*:table/{data_access.data_table_name}/index/*",
                ],
            )
        )

        if definition.needs_athena:
            role.add_to_policy(
                iam.PolicyStatement(
                    sid="AthenaQueryAccess",
                    actions=[
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:GetWorkGroup",
                        "athena:StartQueryExecution",
                        "athena:StopQueryExecution",
                    ],
                    resources=["*"],
                )
            )
            role.add_to_policy(
                iam.PolicyStatement(
                    sid="GlueReadAccess",
                    actions=[
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                    ],
                    resources=["*"],
                )
            )
            if data_access.athena_results_bucket_name:
                role.add_to_policy(
                    iam.PolicyStatement(
                        sid="AthenaResultsBucketAccess",
                        actions=[
                            "s3:AbortMultipartUpload",
                            "s3:GetBucketLocation",
                            "s3:GetObject",
                            "s3:ListBucket",
                            "s3:ListMultipartUploadParts",
                            "s3:PutObject",
                        ],
                        resources=[
                            f"arn:aws:s3:::{data_access.athena_results_bucket_name}",
                            f"arn:aws:s3:::{data_access.athena_results_bucket_name}/*",
                        ],
                    )
                )
