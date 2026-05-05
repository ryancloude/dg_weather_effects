from __future__ import annotations

from aws_cdk import Duration
from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_logs as logs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

from dgwe_cdk.config.jobs import JOB_DEFINITIONS, PipelineJobDefinition
from dgwe_cdk.config.settings import PipelineSettings
from dgwe_cdk.constructs.pipeline_job import PipelineDataAccess, PipelineJob
from dgwe_cdk.stacks.shared_stack import PipelineSharedStack


class PipelineOrchestrationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        settings: PipelineSettings,
        shared: PipelineSharedStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.settings = settings
        self.shared = shared

        data_access = PipelineDataAccess(
            data_bucket_name=shared.bronze_bucket.bucket_name,
            data_table_name=shared.event_index_table.table_name,
            athena_results_bucket_name=shared.athena_results_bucket.bucket_name,
        )

        self.jobs = {
            definition.job_name: PipelineJob(
                self,
                f"{definition.state_id}Job",
                settings=settings,
                definition=definition,
                repository=shared.job_repositories[definition.job_name],
                config_parameters=shared.config_parameters,
                data_access=data_access,
            )
            for definition in JOB_DEFINITIONS
        }

        self.incremental_state_machine_log_group = logs.LogGroup(
            self,
            "StateMachineLogGroup",
            log_group_name=f"/{settings.resource_prefix}/step-functions/incremental",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        incremental_definition = self._build_pipeline_definition(
            definition_prefix="Incremental",
            pipeline_name=settings.pipeline_name,
            job_names=[
                "ingest_pdga_event_pages",
                "ingest_pdga_live_results",
                "silver_pdga_live_results",
                "ingest_weather_observations",
                "silver_weather_observations",
                "silver_weather_enriched",
                "gold_wind_effects",
                "gold_wind_model_inputs",
                "score_round_wind_model",
                "report_round_weather_impacts",
            ],
            full_refresh=False,
            command_mode="incremental",
        )

        self.state_machine = sfn.StateMachine(
            self,
            "IncrementalPipelineStateMachine",
            state_machine_name=settings.state_machine_name,
            definition_body=sfn.DefinitionBody.from_chainable(incremental_definition),
            timeout=Duration.hours(6),
            logs=sfn.LogOptions(
                destination=self.incremental_state_machine_log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            tracing_enabled=True,
        )

        self.schedule_rule = events.Rule(
            self,
            "IncrementalScheduleRule",
            schedule=events.Schedule.expression(settings.schedule_expression),
        )
        self.schedule_rule.add_target(
            targets.SfnStateMachine(
                self.state_machine,
                input=events.RuleTargetInput.from_object(
                    {
                        "trigger": "eventbridge",
                        "pipeline_mode": "incremental",
                    }
                ),
            )
        )

        self.weekly_retrain_state_machine_log_group = logs.LogGroup(
            self,
            "WeeklyRetrainStateMachineLogGroup",
            log_group_name=f"/{settings.resource_prefix}/step-functions/weekly-retrain",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        weekly_definition = self._build_pipeline_definition(
            definition_prefix="WeeklyRetrain",
            pipeline_name=settings.weekly_retrain_pipeline_name,
            job_names=[
                "train_round_wind_model",
                "score_round_wind_model",
                "report_round_weather_impacts",
            ],
            full_refresh=True,
            command_mode="weekly_retrain",
        )

        self.weekly_retrain_state_machine = sfn.StateMachine(
            self,
            "WeeklyRetrainPipelineStateMachine",
            state_machine_name=settings.weekly_retrain_state_machine_name,
            definition_body=sfn.DefinitionBody.from_chainable(weekly_definition),
            timeout=Duration.hours(8),
            logs=sfn.LogOptions(
                destination=self.weekly_retrain_state_machine_log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            tracing_enabled=True,
        )

        self.weekly_retrain_schedule_rule = events.Rule(
            self,
            "WeeklyRetrainScheduleRule",
            schedule=events.Schedule.expression(settings.weekly_retrain_schedule_expression),
        )
        self.weekly_retrain_schedule_rule.add_target(
            targets.SfnStateMachine(
                self.weekly_retrain_state_machine,
                input=events.RuleTargetInput.from_object(
                    {
                        "trigger": "eventbridge",
                        "pipeline_mode": "weekly_retrain",
                    }
                ),
            )
        )

    def _build_pipeline_definition(
        self,
        *,
        definition_prefix: str,
        pipeline_name: str,
        job_names: list[str],
        full_refresh: bool,
        command_mode: str,
    ) -> sfn.IChainable:
        initialize_context = sfn.Pass(
            self,
            f"{definition_prefix}InitializeContext",
            parameters={
                "run_id.$": "States.UUID()",
                "pipeline_name": pipeline_name,
                "app_env": self.settings.app_env,
                "log_level": self.settings.log_level,
                "execution_ts.$": "$$.Execution.StartTime",
                "trigger_payload.$": "$",
            },
        )

        mark_failed = tasks.DynamoUpdateItem(
            self,
            f"{definition_prefix}MarkRunFailed",
            table=self.shared.pipeline_runs_table,
            key={
                "run_id": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.run_id")
                )
            },
            update_expression=(
                "SET #status = :status, ended_at = :ended_at, "
                "error_name = :error_name, error_cause = :error_cause"
            ),
            expression_attribute_names={
                "#status": "status",
            },
            expression_attribute_values={
                ":status": tasks.DynamoAttributeValue.from_string("FAILED"),
                ":ended_at": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
                ":error_name": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.error_info.Error")
                ),
                ":error_cause": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.error_info.Cause")
                ),
            },
            result_path=sfn.JsonPath.DISCARD,
        )

        fail_state = sfn.Fail(
            self,
            f"{definition_prefix}PipelineFailed",
            cause=f"{pipeline_name} failed",
            error="PipelineFailed",
        )

        failure_chain = mark_failed.next(fail_state)

        initialize_run = tasks.DynamoPutItem(
            self,
            f"{definition_prefix}InitializeRun",
            table=self.shared.pipeline_runs_table,
            item={
                "run_id": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.run_id")
                ),
                "pipeline_name": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.pipeline_name")
                ),
                "app_env": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.app_env")
                ),
                "status": tasks.DynamoAttributeValue.from_string("RUNNING"),
                "started_at": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.execution_ts")
                ),
                "execution_ts": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.execution_ts")
                ),
            },
            result_path=sfn.JsonPath.DISCARD,
        )
        initialize_run.add_catch(failure_chain, result_path="$.error_info")

        chain = initialize_context.next(initialize_run)

        for job_name in job_names:
            definition = self.jobs[job_name].definition
            step = self._ecs_step(
                job_name,
                step_id=f"{definition_prefix}{definition.state_id}",
                command=self._build_command(definition, mode=command_mode),
                full_refresh=full_refresh,
            )
            step.add_catch(failure_chain, result_path="$.error_info")
            chain = chain.next(step)

        mark_succeeded = tasks.DynamoUpdateItem(
            self,
            f"{definition_prefix}MarkRunSucceeded",
            table=self.shared.pipeline_runs_table,
            key={
                "run_id": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.run_id")
                )
            },
            update_expression="SET #status = :status, ended_at = :ended_at",
            expression_attribute_names={
                "#status": "status",
            },
            expression_attribute_values={
                ":status": tasks.DynamoAttributeValue.from_string("SUCCEEDED"),
                ":ended_at": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.State.EnteredTime")
                ),
            },
            result_path=sfn.JsonPath.DISCARD,
        )
        mark_succeeded.add_catch(failure_chain, result_path="$.error_info")

        done = sfn.Succeed(self, f"{definition_prefix}PipelineSucceeded")

        return chain.next(mark_succeeded).next(done)

    def _build_command(
        self,
        definition: PipelineJobDefinition,
        *,
        mode: str,
    ) -> list[str]:
        base = list(definition.default_command)

        if mode == "incremental":
            return base

        if mode == "weekly_retrain":
            if definition.job_name == "train_round_wind_model":
                return [
                    "--force-train",
                    "--set-production-fingerprint",
                    "--production-fingerprint-parameter-name",
                    self.settings.parameter_name("PRODUCTION_TRAINING_REQUEST_FINGERPRINT"),
                    *base,
                ]
            if definition.job_name == "score_round_wind_model":
                return ["--force-events", *base]
            return base

        raise ValueError(f"Unsupported command mode: {mode}")

    def _ecs_step(
        self,
        job_name: str,
        *,
        step_id: str,
        command: list[str],
        full_refresh: bool,
    ) -> tasks.EcsRunTask:
        job = self.jobs[job_name]
        definition: PipelineJobDefinition = job.definition

        return tasks.EcsRunTask(
            self,
            step_id,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=self.shared.cluster,
            task_definition=job.task_definition,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST
            ),
            assign_public_ip=True,
            security_groups=[
                ec2.SecurityGroup(
                    self,
                    f"{step_id}SecurityGroup",
                    vpc=self.shared.vpc,
                    allow_all_outbound=True,
                )
            ],
            subnets=ec2.SubnetSelection(subnets=self.shared.vpc.public_subnets),
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=job.container,
                    command=command,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name="RUN_ID",
                            value=sfn.JsonPath.string_at("$.run_id"),
                        ),
                        tasks.TaskEnvironmentVariable(
                            name="PIPELINE_NAME",
                            value=sfn.JsonPath.string_at("$.pipeline_name"),
                        ),
                        tasks.TaskEnvironmentVariable(
                            name="APP_ENV",
                            value=sfn.JsonPath.string_at("$.app_env"),
                        ),
                        tasks.TaskEnvironmentVariable(
                            name="LOG_LEVEL",
                            value=sfn.JsonPath.string_at("$.log_level"),
                        ),
                        tasks.TaskEnvironmentVariable(
                            name="EXECUTION_TS",
                            value=sfn.JsonPath.string_at("$.execution_ts"),
                        ),
                        tasks.TaskEnvironmentVariable(
                            name="FULL_REFRESH",
                            value="true" if full_refresh else "false",
                        ),
                    ],
                )
            ],
            result_path=sfn.JsonPath.DISCARD,
            timeout=Duration.minutes(definition.timeout_minutes),
        )
