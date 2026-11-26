#
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

import inspect

import pytest

from airflow.providers.amazon.aws.sensors import emr
from airflow.sensors.base import BaseSensorOperator


class TestEmrSensorsModuleExports:
    """Tests that verify module-level exports and class structures in emr.py."""

    @pytest.mark.parametrize(
        "class_name",
        [
            "EmrBaseSensor",
            "EmrServerlessJobSensor",
            "EmrServerlessApplicationSensor",
            "EmrContainerSensor",
            "EmrNotebookExecutionSensor",
            "EmrJobFlowSensor",
            "EmrStepSensor",
        ],
    )
    def test_emr_sensor_classes_exist(self, class_name):
        assert hasattr(emr, class_name)
        cls = getattr(emr, class_name)
        assert inspect.isclass(cls)
        assert issubclass(cls, BaseSensorOperator)

    def test_template_fields_defined_on_sensors(self):
        sensor_classes = [
            emr.EmrServerlessJobSensor,
            emr.EmrServerlessApplicationSensor,
            emr.EmrContainerSensor,
            emr.EmrNotebookExecutionSensor,
            emr.EmrJobFlowSensor,
            emr.EmrStepSensor,
        ]
        for cls in sensor_classes:
            assert hasattr(cls, "template_fields")
            assert isinstance(cls.template_fields, (tuple, list, set))
            assert len(cls.template_fields) > 0

    def test_emr_base_sensor_inheritance(self):
        assert issubclass(emr.EmrStepSensor, emr.EmrBaseSensor)
        assert issubclass(emr.EmrJobFlowSensor, emr.EmrBaseSensor)
        assert issubclass(emr.EmrBaseSensor, BaseSensorOperator)
