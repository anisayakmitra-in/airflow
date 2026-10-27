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
from celery import states as celery_states
from celery.backends.base import BaseKeyValueStoreBackend
from celery.backends.database import DatabaseBackend

from airflow.providers.celery.executors import celery_executor_utils
from airflow.providers.celery.executors.celery_executor_utils import (
    CELERY_FETCH_ERR_MSG_HEADER,
    BulkStateFetcher,
    ExceptionWithTraceback,
    _celery_task_logs_to_stdout_override,
    fetch_celery_task_state,
    get_celery_configuration,
)
from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG
from airflow.providers.common.compat.sdk import conf

from tests_common.test_utils.config import conf_vars


class TestCeleryExecutorUtils:
    @pytest.mark.parametrize(
        ("conf_val", "expected"),
        [
            ("true", True),
            ("True", True),
            ("1", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("none", None),
            ("None", None),
            ("", None),
        ],
    )
    def test_celery_task_logs_to_stdout_override(self, conf_val, expected):
        with conf_vars({("celery", "task_logs_to_stdout"): conf_val}):
            assert _celery_task_logs_to_stdout_override() == expected

    def test_get_celery_configuration_default(self):
        # Clear cache to ensure clean test
        get_celery_configuration.cache_clear()
        try:
            with conf_vars({("celery", "celery_config_options"): None}):
                config = get_celery_configuration()
                assert config == DEFAULT_CELERY_CONFIG
        finally:
            get_celery_configuration.cache_clear()

    def test_get_celery_configuration_custom(self):
        get_celery_configuration.cache_clear()
        custom_dict = {"broker_url": "redis://custom:6379/0"}
        try:
            with (
                conf_vars({("celery", "celery_config_options"): "test_module.custom_config"}),
                mock.patch.object(conf, "has_option", return_value=True),
                mock.patch.object(conf, "getimport", return_value=custom_dict),
            ):
                config = get_celery_configuration()
                assert config == custom_dict
        finally:
            get_celery_configuration.cache_clear()

    def test_create_celery_app_global_conf(self):
        team_conf = mock.MagicMock()
        team_conf.get.side_effect = lambda section, key, fallback=None: {
            ("celery", "CELERY_APP_NAME"): "airflow_celery",
            ("celery", "celery_config_options"): None,
        }.get((section, key), fallback)
        del team_conf.team_name

        with (
            mock.patch(
                "airflow.providers.celery.executors.default_celery.get_default_celery_config",
                return_value={"broker_url": "memory://"},
            ),
            mock.patch("airflow.providers.celery.executors.celery_executor_utils.Celery") as mock_celery_cls,
        ):
            mock_app = mock.MagicMock()
            mock_celery_cls.return_value = mock_app

            app = celery_executor_utils.create_celery_app(team_conf)
            mock_celery_cls.assert_called_once_with(
                "airflow_celery", config_source={"broker_url": "memory://"}
            )
            assert app == mock_app

    def test_create_celery_app_with_team_name(self):
        team_conf = mock.MagicMock()
        team_conf.team_name = "analytics"
        team_conf.get.side_effect = lambda section, key, fallback=None: {
            ("celery", "CELERY_APP_NAME"): "airflow_celery",
            ("celery", "celery_config_options"): None,
        }.get((section, key), fallback)

        with (
            mock.patch(
                "airflow.providers.celery.executors.default_celery.get_default_celery_config",
                return_value={"broker_url": "memory://"},
            ),
            mock.patch("airflow.providers.celery.executors.celery_executor_utils.Celery") as mock_celery_cls,
        ):
            mock_app = mock.MagicMock()
            mock_celery_cls.return_value = mock_app

            app = celery_executor_utils.create_celery_app(team_conf)
            mock_celery_cls.assert_called_once_with(
                "airflow_celery_analytics", config_source={"broker_url": "memory://"}
            )
            assert app == mock_app

    def test_exception_with_traceback(self):
        exc = ValueError("something failed")
        tb = "Traceback (most recent call last):\n..."
        wrapper = ExceptionWithTraceback(exc, tb)
        assert wrapper.exception is exc
        assert wrapper.traceback == tb

    def test_fetch_celery_task_state_success(self):
        mock_async_result = mock.MagicMock()
        mock_async_result.task_id = "task_uuid_123"
        mock_async_result.state = celery_states.SUCCESS
        mock_async_result.info = {"result": 42}

        task_id, state, info = fetch_celery_task_state(mock_async_result)
        assert task_id == "task_uuid_123"
        assert state == celery_states.SUCCESS
        assert info == {"result": 42}

    def test_fetch_celery_task_state_failure(self):
        mock_async_result = mock.MagicMock()
        mock_async_result.task_id = "task_uuid_fail"
        type(mock_async_result).state = mock.PropertyMock(side_effect=RuntimeError("Connection lost"))

        task_id, state_or_exc, info = fetch_celery_task_state(mock_async_result)
        assert task_id == "task_uuid_fail"
        assert isinstance(state_or_exc, ExceptionWithTraceback)
        assert isinstance(state_or_exc.exception, RuntimeError)
        assert info is None

    @mock.patch("gc.unfreeze")
    def test_on_celery_worker_ready(self, mock_unfreeze):
        celery_executor_utils.on_celery_worker_ready()
        mock_unfreeze.assert_called_once()


class TestBulkStateFetcher:
    def test_tasks_list_to_task_ids(self):
        fetcher = BulkStateFetcher(sync_parallelism=2)
        res1 = mock.MagicMock(task_id="id1")
        res2 = mock.MagicMock(task_id="id2")
        res3 = mock.MagicMock(task_id="id1")  # duplicate

        assert fetcher._tasks_list_to_task_ids([res1, res2, res3]) == {"id1", "id2"}

    def test_prepare_state_and_info_by_task_dict(self):
        task_ids = {"task_1", "task_2", "task_missing"}
        task_results = {
            "task_1": {"status": celery_states.SUCCESS, "info": "ok"},
            "task_2": {"status": celery_states.FAILURE, "info": "err"},
        }

        result = BulkStateFetcher._prepare_state_and_info_by_task_dict(task_ids, task_results)

        assert result["task_1"] == (celery_states.SUCCESS, "ok")
        assert result["task_2"] == (celery_states.FAILURE, "err")
        assert result["task_missing"] == (celery_states.PENDING, None)

    def test_get_many_kv_backend_routing(self):
        mock_backend = mock.MagicMock(spec=BaseKeyValueStoreBackend)
        mock_backend.get_key_for_task.side_effect = lambda k: f"task_{k}".encode()
        mock_backend.mget.return_value = [b'{"task_id": "t1", "status": "SUCCESS"}']
        mock_backend.decode_result.return_value = {"task_id": "t1", "status": celery_states.SUCCESS}

        mock_celery_app = mock.MagicMock()
        mock_celery_app.backend = mock_backend

        fetcher = BulkStateFetcher(sync_parallelism=2, celery_app=mock_celery_app)
        async_result = mock.MagicMock(task_id="t1")

        results = fetcher.get_many([async_result])
        assert "t1" in results
        assert results["t1"] == (celery_states.SUCCESS, None)
        mock_backend.mget.assert_called_once()

    def test_get_many_db_backend_routing(self):
        mock_backend = mock.MagicMock(spec=DatabaseBackend)
        mock_task = mock.MagicMock()
        mock_task.to_dict.return_value = {"task_id": "db_t1", "status": celery_states.SUCCESS}
        mock_backend.meta_from_decoded.return_value = {
            "task_id": "db_t1",
            "status": celery_states.SUCCESS,
            "info": "done",
        }

        mock_celery_app = mock.MagicMock()
        mock_celery_app.backend = mock_backend

        fetcher = BulkStateFetcher(sync_parallelism=2, celery_app=mock_celery_app)
        with mock.patch.object(fetcher, "_query_task_cls_from_db_backend", return_value=[mock_task]):
            async_result = mock.MagicMock(task_id="db_t1")
            results = fetcher.get_many([async_result])

            assert results["db_t1"] == (celery_states.SUCCESS, "done")

    def test_get_many_using_multiprocessing(self, caplog):
        mock_celery_app = mock.MagicMock()
        mock_celery_app.backend = mock.MagicMock()  # Generic backend (not KV or DB)

        fetcher = BulkStateFetcher(sync_parallelism=2, celery_app=mock_celery_app)
        res1 = mock.MagicMock(task_id="t1")
        res2 = mock.MagicMock(task_id="t2")

        exc = ExceptionWithTraceback(RuntimeError("fetch error"), "Traceback line")
        simulated_results = [
            ("t1", celery_states.SUCCESS, "info_ok"),
            ("t2", exc, None),
        ]

        with mock.patch(
            "airflow.providers.celery.executors.celery_executor_utils.ProcessPoolExecutor"
        ) as mock_pool_cls:
            mock_pool = mock.MagicMock()
            mock_pool.__enter__.return_value = mock_pool
            mock_pool.map.return_value = simulated_results
            mock_pool_cls.return_value = mock_pool

            results = fetcher.get_many([res1, res2])

            assert results["t1"] == (celery_states.SUCCESS, "info_ok")
            assert "t2" not in results
            assert CELERY_FETCH_ERR_MSG_HEADER in caplog.text
