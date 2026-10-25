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

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.amazon.aws.executors.aws_lambda.docker import app


class TestLambdaApp:
    def test_get_queue_url_primary(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/primary"
        )
        monkeypatch.setenv("QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/fallback")
        assert app.get_queue_url() == "https://sqs.us-east-1.amazonaws.com/123/primary"

    def test_get_queue_url_fallback(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL", raising=False)
        monkeypatch.setenv("QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/fallback")
        assert app.get_queue_url() == "https://sqs.us-east-1.amazonaws.com/123/fallback"

    def test_get_queue_url_missing_raises(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL", raising=False)
        monkeypatch.delenv("QUEUE_URL", raising=False)
        with pytest.raises(RuntimeError, match="No Queue URL detected"):
            app.get_queue_url()

    @patch("boto3.client")
    def test_get_sqs_client(self, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        result = app.get_sqs_client()
        mock_boto_client.assert_called_once_with("sqs")
        assert result == mock_client

    @patch.object(app, "get_sqs_client")
    @patch.object(app, "get_queue_url")
    @patch("subprocess.run")
    def test_run_and_report_success(self, mock_subprocess_run, mock_get_queue, mock_get_sqs):
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["airflow", "tasks", "run"],
            returncode=0,
            stdout=b"Task ran successfully",
        )
        mock_get_queue.return_value = "https://sqs.us-east-1.amazonaws.com/123/queue"
        mock_sqs = MagicMock()
        mock_get_sqs.return_value = mock_sqs

        app.run_and_report(["airflow", "tasks", "run"], "dag.task.run_id")

        mock_subprocess_run.assert_called_once_with(
            ["airflow", "tasks", "run"],
            check=False,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        expected_body = json.dumps({"task_key": "dag.task.run_id", "return_code": 0})
        mock_sqs.send_message.assert_called_once_with(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/123/queue",
            MessageBody=expected_body,
        )

    @patch.object(app, "get_sqs_client")
    @patch.object(app, "get_queue_url")
    @patch("subprocess.run")
    def test_run_and_report_subprocess_exception(self, mock_subprocess_run, mock_get_queue, mock_get_sqs):
        mock_subprocess_run.side_effect = OSError("Execution failed")
        mock_get_queue.return_value = "https://sqs.us-east-1.amazonaws.com/123/queue"
        mock_sqs = MagicMock()
        mock_get_sqs.return_value = mock_sqs

        app.run_and_report("airflow tasks run string_cmd", "dag.task.run_id")

        expected_body = json.dumps({"task_key": "dag.task.run_id", "return_code": 1})
        mock_sqs.send_message.assert_called_once_with(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/123/queue",
            MessageBody=expected_body,
        )

    @patch.object(app, "get_sqs_client")
    @patch.object(app, "get_queue_url")
    @patch("subprocess.run")
    def test_run_and_report_sqs_failure_swallowed(self, mock_subprocess_run, mock_get_queue, mock_get_sqs):
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["airflow"],
            returncode=0,
            stdout=b"ok",
        )
        mock_get_queue.return_value = "https://sqs.us-east-1.amazonaws.com/123/queue"
        mock_sqs = MagicMock()
        mock_sqs.send_message.side_effect = RuntimeError("SQS unavailable")
        mock_get_sqs.return_value = mock_sqs

        # Should log exception but not re-raise
        app.run_and_report(["airflow"], "key")
        mock_sqs.send_message.assert_called_once()

    @patch("boto3.resource")
    def test_fetch_dags_from_s3(self, mock_boto_resource, monkeypatch, tmp_path):
        mock_s3 = MagicMock()
        mock_boto_resource.return_value = mock_s3
        mock_bucket = MagicMock()
        mock_s3.Bucket.return_value = mock_bucket

        obj_dir = MagicMock(key="dags/subdir/")
        obj_dag1 = MagicMock(key="dags/example_dag.py")
        obj_dag2 = MagicMock(key="dags/subdir/sub_dag.py")
        mock_bucket.objects.filter.return_value = [obj_dir, obj_dag1, obj_dag2]

        app.fetch_dags_from_s3("s3://my-test-bucket/dags/")

        mock_s3.Bucket.assert_called_once_with("my-test-bucket")
        mock_bucket.objects.filter.assert_called_once_with(Prefix="dags/")

        # Verify only files were downloaded, directory skipped
        assert mock_bucket.download_file.call_count == 2

    @patch.object(app, "run_and_report")
    @patch.object(app, "fetch_dags_from_s3")
    def test_lambda_handler_without_s3(self, mock_fetch_dags, mock_run_and_report, monkeypatch):
        monkeypatch.setattr(app, "S3_URI", None)
        event = {
            "command": ["airflow", "tasks", "run"],
            "task_key": "task_1",
            "executor_config": {"memory": 512},
        }
        app.lambda_handler(event, None)

        mock_fetch_dags.assert_not_called()
        mock_run_and_report.assert_called_once_with(["airflow", "tasks", "run"], "task_1")

    @patch.object(app, "run_and_report")
    @patch.object(app, "fetch_dags_from_s3")
    def test_lambda_handler_with_s3(self, mock_fetch_dags, mock_run_and_report, monkeypatch):
        monkeypatch.setattr(app, "S3_URI", "s3://my-bucket/dags/")
        event = {
            "command": "airflow tasks run",
            "task_key": "task_2",
        }
        app.lambda_handler(event, None)

        mock_fetch_dags.assert_called_once_with("s3://my-bucket/dags/")
        mock_run_and_report.assert_called_once_with("airflow tasks run", "task_2")
