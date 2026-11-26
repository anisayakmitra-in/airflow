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

from unittest import mock

import pytest
from celery.backends.base import BaseKeyValueStoreBackend, DatabaseBackend

from airflow.providers.celery.executors.celery_executor_utils import (
    BulkStateFetcher,
    ExceptionWithTraceback,
    _celery_task_logs_to_stdout_override,
    fetch_celery_task_state,
)
from tests_common.test_utils.config import conf_vars


class TestExceptionWithTraceback:
    def test_initialization_and_properties(self):
        original_exc = ValueError("Invalid celery task payload")
        tb_str = "Traceback (most recent call last):\n  File 'test.py', line 1"
        wrapped = ExceptionWithTraceback(original_exc, tb_str)

        assert wrapped.exception is original_exc
        assert wrapped.traceback == tb_str


class TestFetchCeleryTaskState:
    def test_fetch_success_state(self):
        mock_result = mock.MagicMock()
        mock_result.task_id = "celery-task-12345"
        mock_result.state = "SUCCESS"
        mock_result.info = {"retries": 0, "result": "done"}

        task_id, state, info = fetch_celery_task_state(mock_result)

        assert task_id == "celery-task-12345"
        assert state == "SUCCESS"
        assert info == {"retries": 0, "result": "done"}

    def test_fetch_failure_state_catches_exception(self):
        mock_result = mock.MagicMock()
        mock_result.task_id = "celery-task-failed"
        type(mock_result).state = mock.PropertyMock(side_effect=RuntimeError("Connection lost to broker"))

        task_id, state_or_exception, info = fetch_celery_task_state(mock_result)

        assert task_id == "celery-task-failed"
        assert isinstance(state_or_exception, ExceptionWithTraceback)
        assert isinstance(state_or_exception.exception, RuntimeError)
        assert info is None


class TestCeleryTaskLogsToStdoutOverride:
    def test_returns_true_when_configured_true(self):
        with conf_vars({("celery", "task_logs_to_stdout"): "True"}):
            assert _celery_task_logs_to_stdout_override() is True

    def test_returns_false_when_configured_false(self):
        with conf_vars({("celery", "task_logs_to_stdout"): "False"}):
            assert _celery_task_logs_to_stdout_override() is False

    def test_returns_none_when_unset_or_none(self):
        with conf_vars({("celery", "task_logs_to_stdout"): "none"}):
            assert _celery_task_logs_to_stdout_override() is None


class TestBulkStateFetcher:
    def test_tasks_list_to_task_ids(self):
        mock_app = mock.MagicMock()
        fetcher = BulkStateFetcher(sync_parallelism=2, celery_app=mock_app)

        t1 = mock.MagicMock(task_id="task-1")
        t2 = mock.MagicMock(task_id="task-2")
        t3 = mock.MagicMock(task_id="task-1")

        result = fetcher._tasks_list_to_task_ids([t1, t2, t3])
        assert result == {"task-1", "task-2"}

    def test_routes_to_kv_backend(self):
        mock_app = mock.MagicMock()
        mock_app.backend = mock.MagicMock(spec=BaseKeyValueStoreBackend)
        fetcher = BulkStateFetcher(sync_parallelism=2, celery_app=mock_app)

        with mock.patch.object(fetcher, "_get_many_from_kv_backend", return_value={"t1": "SUCCESS"}):
            results = fetcher.get_many([mock.MagicMock(task_id="t1")])
            assert results == {"t1": "SUCCESS"}

    def test_routes_to_db_backend(self):
        mock_app = mock.MagicMock()
        mock_app.backend = mock.MagicMock(spec=DatabaseBackend)
        fetcher = BulkStateFetcher(sync_parallelism=2, celery_app=mock_app)

        with mock.patch.object(fetcher, "_get_many_from_db_backend", return_value={"t2": "SUCCESS"}):
            results = fetcher.get_many([mock.MagicMock(task_id="t2")])
            assert results == {"t2": "SUCCESS"}

    def test_routes_to_multiprocessing_by_default(self):
        mock_app = mock.MagicMock()
        mock_app.backend = mock.MagicMock()  # not BaseKeyValueStoreBackend or DatabaseBackend
        fetcher = BulkStateFetcher(sync_parallelism=2, celery_app=mock_app)

        with mock.patch.object(fetcher, "_get_many_using_multiprocessing", return_value={"t3": "PENDING"}):
            results = fetcher.get_many([mock.MagicMock(task_id="t3")])
            assert results == {"t3": "PENDING"}
