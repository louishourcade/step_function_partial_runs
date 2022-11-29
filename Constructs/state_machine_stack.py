import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as aws_lambda,
    aws_stepfunctions as sf,
    aws_stepfunctions_tasks as tasks,
    aws_glue as aws_glue,
    aws_s3_deployment as s3deploy,
    Duration,
)


class StateMachineStack(cdk.Stack):
    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket that stores Glue scripts and data
        s3_bucket = s3.Bucket(
            self,
            "S3BucketDataStore",
            bucket_name=f"step-function-poc-{self.account}-{self.region}",
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            server_access_logs_prefix="access-logs",
        )
        s3_bucket_name = s3_bucket.bucket_name

        # Upload glue script to S3
        deploy = s3deploy.BucketDeployment(
            self,
            "GlueJobUpload",
            sources=[s3deploy.Source.asset("glue_jobs")],
            destination_bucket=s3_bucket,
            destination_key_prefix="glue-asset/scripts",
        )

        # IAM Policy to access S3 bucket
        S3_access_policy = iam.Policy(
            self,
            "S3AccessPolicy",
            policy_name=f"S3-access-policy-{self.region}",
            statements=[
                iam.PolicyStatement(
                    actions=["s3:*"],
                    resources=[s3_bucket.bucket_arn, s3_bucket.bucket_arn + "/*"],
                    effect=iam.Effect.ALLOW,
                )
            ],
        )
        S3_access_policy.node.add_dependency(s3_bucket)

        # Lambda function that gets data from public API
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name=f"lambda-execution-role-{self.region}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_managed_policy_arn(
                    self,
                    "LambdaAccessPolicy",
                    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
                ),
            ],
        )
        S3_access_policy.attach_to_role(lambda_role)

        data_loader = aws_lambda.Function(
            self,
            "LambdaDataLoader",
            function_name="data-loader",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset("Constructs/lambda/lambda_deploy.zip"),
            handler="velib_data_loader.handler",
            role=lambda_role,
            timeout=Duration.minutes(5),
            environment={
                "S3_BUCKET_NAME": s3_bucket.bucket_name,
            },
        )

        # Glue database
        glue_database = aws_glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                description="Glue database to store metadata tables",
                name="step_function_partial_db",
            ),
        )

        # Glue jobs
        glue_role = iam.Role(
            self,
            "GlueRole",
            role_name=f"glue-execution-role-{self.region}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_managed_policy_arn(
                    self,
                    "AdminAccessPolicy",
                    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
                ),
            ],
        )
        S3_access_policy.attach_to_role(glue_role)

        security_config = aws_glue.CfnSecurityConfiguration(
            self,
            "GlueSecurityConfiguration",
            name="GlueJobSecurityConfiguration",
            encryption_configuration=aws_glue.CfnSecurityConfiguration.EncryptionConfigurationProperty(
                s3_encryptions=[
                    aws_glue.CfnSecurityConfiguration.S3EncryptionProperty(
                        s3_encryption_mode="SSE-S3"
                    )
                ]
            ),
        )

        job_args = {
            "--TempDir": f"s3://{s3_bucket_name}/glue-asset/temporary/",
            "--spark-event-logs-path": (
                f"s3://{s3_bucket_name}/glue-asset/sparkHistoryLogs/"
            ),
            "--enable-job-insights": "true",
            "--enable-spark-ui": "true",
            "--enable-glue-datacatalog": "true",
            "--S3Bucket": s3_bucket_name,
            "--GlueDatabase": glue_database.database_input.name,
        }
        raw_silver = aws_glue.CfnJob(
            self,
            "RawToSilverJob",
            name="velib_raw_to_sliver_cdk",
            role=glue_role.role_arn,
            allocated_capacity=10,
            execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=100
            ),
            glue_version="3.0",
            security_configuration=security_config.name,
            default_arguments=job_args,
            command=aws_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=(
                    f"s3://{s3_bucket_name}/glue-asset/scripts/velib_raw_to_silver.py"
                ),
            ),
        )
        sliver_gold_avg = aws_glue.CfnJob(
            self,
            "SilverGoldAvg",
            name="velib_silver_to_gold_avg_stats_cdk",
            role=glue_role.role_arn,
            allocated_capacity=10,
            execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=100
            ),
            glue_version="3.0",
            security_configuration=security_config.name,
            default_arguments=job_args,
            command=aws_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=(
                    f"s3://{s3_bucket_name}/glue-asset/scripts/velib_silver_to_gold_avg_stats.py"
                ),
            ),
        )
        sliver_gold_availability = aws_glue.CfnJob(
            self,
            "SilverGoldAvailability",
            name="velib_silver_to_gold_avail_percent_cdk",
            role=glue_role.role_arn,
            allocated_capacity=10,
            execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=100
            ),
            glue_version="3.0",
            security_configuration=security_config.name,
            default_arguments=job_args,
            command=aws_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=(
                    f"s3://{s3_bucket_name}/glue-asset/scripts/velib_silver_to_gold_avail_percent.py"
                ),
            ),
        )

        ### State machine
        # Set the lambda invoke task
        lambda_step = tasks.LambdaInvoke(
            self,
            "Lambda - Load Data",
            lambda_function=data_loader,
            result_path=sf.JsonPath.DISCARD,
        )

        # Set the first glue job invoke task
        glue_step1 = tasks.GlueStartJobRun(
            self,
            "Glue StartJobRun - velib_raw_to_silver",
            glue_job_name=raw_silver.name,
            result_path=sf.JsonPath.DISCARD,
        )

        # Set the second glue job invoke task
        glue_step2 = tasks.GlueStartJobRun(
            self,
            "Glue StartJobRun - velib_silver_to_gold_avg_stats",
            glue_job_name=sliver_gold_avg.name,
            result_path=sf.JsonPath.DISCARD,
        )

        # Set the third glue job invoke task
        glue_step3 = tasks.GlueStartJobRun(
            self,
            "Glue StartJobRun - velib_silver_to_gold_avail_percents",
            glue_job_name=sliver_gold_availability.name,
            result_path=sf.JsonPath.DISCARD,
        )

        # Branch1 of parallel execution
        pass_branch_1 = sf.Pass(self, "bypass velib_silver_to_gold_avg_stats")
        choice_branch_1 = (
            sf.Choice(self, "velib_silver_to_gold_avg_stats is runnable?")
            .when(
                sf.Condition.boolean_equals("$.velib_silver_to_gold_avg_stats", True),
                glue_step2,
            )
            .otherwise(pass_branch_1)
        )
        sf_branch_1 = choice_branch_1

        # Branch2 of parallel execution
        pass_branch_2 = sf.Pass(self, "bypass velib_silver_to_gold_avail_percents")
        choice_branch_2 = (
            sf.Choice(self, "velib_silver_to_gold_avail_percents is runnable?")
            .when(
                sf.Condition.boolean_equals(
                    "$.velib_silver_to_gold_avail_percent", True
                ),
                glue_step3,
            )
            .otherwise(pass_branch_2)
        )
        sf_branch_2 = choice_branch_2

        # parallel block
        parallel = sf.Parallel(self, "Parallel").branch(sf_branch_1).branch(sf_branch_2)

        # raw_silver step
        choice_raw_silver = (
            sf.Choice(self, "velib_raw_to_silver is runnable?")
            .when(
                sf.Condition.boolean_equals("$.velib_raw_to_silver", True),
                glue_step1.next(parallel),
            )
            .otherwise(parallel)
        )

        # data gen step
        choice_data_gen = (
            sf.Choice(self, "Load Data is runnable?")
            .when(
                sf.Condition.boolean_equals("$.lambda_load_data", True),
                lambda_step.next(choice_raw_silver),
            )
            .otherwise(choice_raw_silver)
        )

        # choice
        cond1 = sf.Condition.is_present("$.lambda_load_data")
        cond2 = sf.Condition.is_present("$.velib_raw_to_silver")
        cond3 = sf.Condition.is_present("$.velib_silver_to_gold_avg_stats")
        cond4 = sf.Condition.is_present("$.velib_silver_to_gold_avail_percent")

        default_object = {
            "lambda_load_data": True,
            "velib_raw_to_silver": True,
            "velib_silver_to_gold_avg_stats": True,
            "velib_silver_to_gold_avail_percent": True,
        }

        passstate = sf.Pass(
            self,
            "Set Batch default configuration",
            result=sf.Result.from_object(default_object),
            result_path="$",
        ).next(choice_data_gen)
        definition = (
            sf.Choice(self, "Check batchs args")
            .when(sf.Condition.and_(cond1, cond2, cond3, cond4), choice_data_gen)
            .otherwise(passstate)
        )

        state_machine = sf.StateMachine(
            self,
            "StateMachinePOC",
            timeout=Duration.minutes(10),
            definition=definition,
            state_machine_name="velib-demo",
        )

        # Checkov skip
        cfn_bucket = s3_bucket.node.default_child
        cfn_bucket.cfn_options.metadata = {
            "checkov": {
                "skip": [
                    {"id": "CKV_AWS_21", "comment": "S3 bucket without versioning"},
                    {"id": "CKV_AWS_116", "comment": "S3 bucket without versioning"},
                ]
            }
        }
        cfn_lambda = data_loader.node.default_child
        cfn_lambda.cfn_options.metadata = {
            "checkov": {
                "skip": [
                    {
                        "id": "CKV_AWS_173",
                        "comment": "Lambda creates its own CMK by default",
                    },
                    {"id": "CKV_AWS_116", "comment": "Lambda with no DLQ"},
                ]
            }
        }
        glue_database.cfn_options.metadata = {
            "checkov": {
                "skip": [
                    {
                        "id": "CKV_SECRET_2",
                        "comment": "No need to ensure check for secrets in this resource",
                    }
                ]
            }
        }
        security_config.cfn_options.metadata = {
            "checkov": {
                "skip": [
                    {
                        "id": "CKV_AWS_99",
                        "comment": "No need to encrypt cloudwatch logs and job bookmarks",
                    }
                ]
            }
        }
