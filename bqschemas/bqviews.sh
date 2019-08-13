#!/bin/bash -eu
#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

bq mk --use_legacy_sql=false --description "DedupDataGenView" \
--view 'SELECT a.RunName, a.RunTimestamp, a.EventTime, a.GenId, a.Number FROM `karthiproject1.karthidemo1.DedupDataGen` a
JOIN (SELECT DISTINCT RunName, GenId FROM `karthiproject1.karthidemo1.DedupDataGen`) b
ON a.RunName = b.RunName AND a.GenId = b.GenId
ORDER BY a.RunName, a.GenId' karthidemo1.DedupDataGenView

bq mk --use_legacy_sql=false --description "DedupResultsView" \
--view 'SELECT a.RunName, a.RunTimestamp, a.EventTime, a.GenId, a.DfUniqueId, a.DedupType, a.Number FROM `karthiproject1.karthidemo1.DedupResults` a
JOIN (SELECT DISTINCT RunName, DfUniqueId FROM `karthiproject1.karthidemo1.DedupResults`) b
ON a.RunName = b.RunName AND a.DfUniqueId = b.DfUniqueId
ORDER BY a.RunName, a.GenId' karthidemo1.DedupResultsView