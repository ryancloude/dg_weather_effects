from __future__ import annotations

from typing import Final

from aws_cdk import RemovalPolicy
from aws_cdk import Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_ssm as ssm
from constructs import Construct

from dgwe_cdk.config.jobs import JOB_DEFINITIONS
from dgwe_cdk.config.settings import PipelineSettings


CONFIG_PARAMETER_VALUES: Final[dict[str, str]] = {
    "PDGA_S3_BUCKET": "",
    "PDGA_DDB_TABLE": "",
    "PDGA_DDB_STATUS_END_DATE_GSI": "",
    "AWS_REGION": "",
    "ATHENA_DATABASE": "",
    "ATHENA_WORKGROUP": "",
    "ATHENA_RESULTS_S3_URI": "",
    "ATHENA_SOURCE_SCORED_TABLE": "",
    "ATHENA_REPORTING_BASE_TABLE": "",
    "PRODUCTION_TRAINING_REQUEST_FINGERPRINT": "",
}


class PipelineSharedStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        settings: PipelineSettings,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.settings = settings

        athena_results_bucket_name = settings.athena_results_bucket
        if not athena_results_bucket_name:
            raise ValueError("ATHENA_RESULTS_S3_URI must be a valid s3:// URI.")

        self.athena_results_output_location = f"s3://{athena_results_bucket_name}/query-results/"

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.42.0.0/16"),
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                )
            ],
        )

        self.cluster = ecs.Cluster(
            self,
            "Cluster",
            cluster_name=settings.cluster_name,
            vpc=self.vpc,
            container_insights=True,
        )

        self.bronze_bucket = s3.Bucket(
            self,
            "BronzeBucket",
            bucket_name=settings.pdga_s3_bucket,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
        )

        self.bronze_bucket_policy = s3.BucketPolicy(
            self,
            "BronzeBucketPolicy",
            bucket=self.bronze_bucket,
        )
        self.bronze_bucket_policy.document.add_statements(
            iam.PolicyStatement(
                sid="DenyInsecureTransport",
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=["s3:*"],
                resources=[
                    self.bronze_bucket.bucket_arn,
                    self.bronze_bucket.arn_for_objects("*"),
                ],
                conditions={"Bool": {"aws:SecureTransport": "false"}},
            )
        )

        self.event_index_table = dynamodb.Table(
            self,
            "EventIndexTable",
            table_name=settings.pdga_ddb_table,
            partition_key=dynamodb.Attribute(
                name="pk",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="sk",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
        )
        self.event_index_table.add_global_secondary_index(
            index_name=settings.pdga_ddb_status_end_date_gsi,
            partition_key=dynamodb.Attribute(
                name="status_text",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="end_date",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        self.pipeline_runs_table = dynamodb.Table(
            self,
            "PipelineRunsTable",
            table_name=settings.pipeline_runs_table_name,
            partition_key=dynamodb.Attribute(
                name="run_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
        )

        self.athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name=athena_results_bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
        )

        self.athena_results_bucket_policy = s3.BucketPolicy(
            self,
            "AthenaResultsBucketPolicy",
            bucket=self.athena_results_bucket,
        )
        self.athena_results_bucket_policy.document.add_statements(
            iam.PolicyStatement(
                sid="DenyInsecureTransport",
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=["s3:*"],
                resources=[
                    self.athena_results_bucket.bucket_arn,
                    self.athena_results_bucket.arn_for_objects("*"),
                ],
                conditions={"Bool": {"aws:SecureTransport": "false"}},
            )
        )

        self.analytics_database = glue.CfnDatabase(
            self,
            "AnalyticsDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=settings.athena_database,
                description="PDGA scored fact and dashboard reporting database",
            ),
        )

        self.analytics_workgroup = athena.CfnWorkGroup(
            self,
            "AnalyticsAthenaWorkGroup",
            name=settings.athena_workgroup,
            description="Athena workgroup for PDGA analytics and reporting refreshes",
            recursive_delete_option=False,
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=False,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=107374182400,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=self.athena_results_output_location,
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
        )

        self.config_parameters = self._create_config_parameters(settings)
        self.job_repositories = self._create_job_repositories()

    def _create_config_parameters(
        self,
        settings: PipelineSettings,
    ) -> dict[str, ssm.StringParameter]:
        parameter_values = {
            "PDGA_S3_BUCKET": self.bronze_bucket.bucket_name,
            "PDGA_DDB_TABLE": self.event_index_table.table_name,
            "PDGA_DDB_STATUS_END_DATE_GSI": settings.pdga_ddb_status_end_date_gsi,
            "AWS_REGION": settings.aws_region,
            "ATHENA_DATABASE": settings.athena_database,
            "ATHENA_WORKGROUP": settings.athena_workgroup,
            "ATHENA_RESULTS_S3_URI": self.athena_results_output_location,
            "ATHENA_SOURCE_SCORED_TABLE": settings.athena_source_scored_table,
            "ATHENA_REPORTING_BASE_TABLE": settings.athena_reporting_base_table,
            "PRODUCTION_TRAINING_REQUEST_FINGERPRINT": settings.production_training_request_fingerprint,
        }

        parameters: dict[str, ssm.StringParameter] = {}
        for env_key, value in parameter_values.items():
            parameters[env_key] = ssm.StringParameter(
                self,
                f"{env_key}Parameter",
                parameter_name=settings.parameter_name(env_key),
                string_value=value,
            )
        return parameters

    def _create_job_repositories(self) -> dict[str, ecr.Repository]:
        repositories: dict[str, ecr.Repository] = {}
        for definition in JOB_DEFINITIONS:
            repositories[definition.job_name] = ecr.Repository(
                self,
                f"{definition.state_id}Repository",
                repository_name=definition.ecr_repo_name,
                image_scan_on_push=True,
                removal_policy=RemovalPolicy.RETAIN,
                empty_on_delete=False,
            )
        return repositories
