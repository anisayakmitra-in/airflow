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

from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.operators import emr


class TestEmrOperatorsModuleExports:
    """Tests that verify module-level exports and class structures in operators/emr.py."""

    @pytest.mark.parametrize(
        "class_name",
        [
            "EmrAddStepsOperator",
            "EmrStartNotebookExecutionOperator",
            "EmrStopNotebookExecutionOperator",
            "EmrEksCreateClusterOperator",
            "EmrContainerOperator",
            "EmrCreateJobFlowOperator",
            "EmrModifyClusterOperator",
            "EmrTerminateJobFlowOperator",
            "EmrServerlessCreateApplicationOperator",
            "EmrServerlessStartJobOperator",
            "EmrServerlessStopApplicationOperator",
            "EmrServerlessDeleteApplicationOperator",
            "EmrServerlessStartSessionOperator",
            "EmrServerlessGetSessionEndpointOperator",
            "EmrServerlessStopSessionOperator",
        ],
    )
    def test_emr_operator_classes_exist(self, class_name):
        assert hasattr(emr, class_name)
        cls = getattr(emr, class_name)
        assert inspect.isclass(cls)
        assert issubclass(cls, BaseOperator)

    def test_template_fields_defined_on_operators(self):
        operator_classes = [
            emr.EmrAddStepsOperator,
            emr.EmrStartNotebookExecutionOperator,
            emr.EmrStopNotebookExecutionOperator,
            emr.EmrEksCreateClusterOperator,
            emr.EmrContainerOperator,
            emr.EmrCreateJobFlowOperator,
            emr.EmrModifyClusterOperator,
            emr.EmrTerminateJobFlowOperator,
            emr.EmrServerlessCreateApplicationOperator,
            emr.EmrServerlessStartJobOperator,
            emr.EmrServerlessStopApplicationOperator,
            emr.EmrServerlessDeleteApplicationOperator,
            emr.EmrServerlessStartSessionOperator,
            emr.EmrServerlessGetSessionEndpointOperator,
            emr.EmrServerlessStopSessionOperator,
        ]
        for cls in operator_classes:
            assert hasattr(cls, "template_fields")
            assert isinstance(cls.template_fields, (tuple, list, set))
            assert len(cls.template_fields) > 0
