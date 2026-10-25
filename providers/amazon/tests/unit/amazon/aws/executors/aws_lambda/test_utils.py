# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import datetime
from unittest import mock

from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    CONFIG_GROUP_NAME,
    INVALID_CREDENTIALS_EXCEPTIONS,
    AllLambdaConfigKeys,
    InvokeLambdaKwargsConfigKeys,
    LambdaQueuedTask,
)


class TestAwsLambdaExecutorUtilsConstants:
    def test_config_group_name(self):
        assert CONFIG_GROUP_NAME == "aws_lambda_executor"

    def test_invalid_credentials_exceptions(self):
        assert "ExpiredTokenException" in INVALID_CREDENTIALS_EXCEPTIONS
        assert "InvalidClientTokenId" in INVALID_CREDENTIALS_EXCEPTIONS
        assert "UnrecognizedClientException" in INVALID_CREDENTIALS_EXCEPTIONS
        assert len(INVALID_CREDENTIALS_EXCEPTIONS) == 3


class TestLambdaConfigKeys:
    def test_invoke_lambda_kwargs_config_keys(self):
        assert InvokeLambdaKwargsConfigKeys.FUNCTION_NAME == "function_name"
        assert InvokeLambdaKwargsConfigKeys.QUALIFIER == "function_qualifier"
        assert set(InvokeLambdaKwargsConfigKeys()) == {
            "function_name",
            "function_qualifier",
        }

    def test_all_lambda_config_keys(self):
        assert AllLambdaConfigKeys.AWS_CONN_ID == "conn_id"
        assert AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP == "check_health_on_startup"
        assert AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS == "max_invoke_attempts"
        assert AllLambdaConfigKeys.REGION_NAME == "region_name"
        assert AllLambdaConfigKeys.QUEUE_URL == "queue_url"
        assert AllLambdaConfigKeys.DLQ_URL == "dead_letter_queue_url"
        assert AllLambdaConfigKeys.END_WAIT_TIMEOUT == "end_wait_timeout"
        assert AllLambdaConfigKeys.FUNCTION_NAME == "function_name"
        assert AllLambdaConfigKeys.QUALIFIER == "function_qualifier"

        expected_keys = {
            "conn_id",
            "check_health_on_startup",
            "max_invoke_attempts",
            "region_name",
            "queue_url",
            "dead_letter_queue_url",
            "end_wait_timeout",
            "function_name",
            "function_qualifier",
        }
        assert set(AllLambdaConfigKeys()) == expected_keys


class TestLambdaQueuedTask:
    def test_lambda_queued_task_creation(self):
        key = mock.MagicMock()
        command = ["airflow", "tasks", "run", "dag_id", "task_id"]
        queue = "lambda-queue"
        executor_config = {"memory": 512}
        attempt_number = 2
        next_attempt_time = datetime.datetime.now(datetime.timezone.utc)

        queued_task = LambdaQueuedTask(
            key=key,
            command=command,
            queue=queue,
            executor_config=executor_config,
            attempt_number=attempt_number,
            next_attempt_time=next_attempt_time,
        )

        assert queued_task.key is key
        assert queued_task.command == command
        assert queued_task.queue == queue
        assert queued_task.executor_config == executor_config
        assert queued_task.attempt_number == attempt_number
        assert queued_task.next_attempt_time is next_attempt_time

    def test_lambda_queued_task_equality(self):
        key = mock.MagicMock()
        now = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        task1 = LambdaQueuedTask(
            key=key,
            command=["cmd"],
            queue="q",
            executor_config={},
            attempt_number=1,
            next_attempt_time=now,
        )
        task2 = LambdaQueuedTask(
            key=key,
            command=["cmd"],
            queue="q",
            executor_config={},
            attempt_number=1,
            next_attempt_time=now,
        )
        task3 = LambdaQueuedTask(
            key=key,
            command=["different_cmd"],
            queue="q",
            executor_config={},
            attempt_number=1,
            next_attempt_time=now,
        )

        assert task1 == task2
        assert task1 != task3
