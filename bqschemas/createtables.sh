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

# bq mk -t karthidemo1.DedupWithDistinctOrig ./dedupinfo.json
# bq mk -t karthidemo1.DedupWithDistinctResults ./dedupinfo.json
# bq mk -t karthidemo1.DedupWithStateOrig ./dedupinfo.json
# bq mk -t karthidemo1.DedupWithStateResults ./dedupinfo.json

# bq rm -f -t karthidemo1.DedupDataGen
# bq rm -f -t karthidemo1.DedupResults

# bq rm -f -t karthidemo1.DedupDataGenView
# bq rm -f -t karthidemo1.DedupResultsView

bq mk -t --schema ./dedupgenschema.json --time_partitioning_field RunTimestamp karthidemo1.DedupDataGen

bq mk -t --schema ./dedupresultsschema.json --time_partitioning_field RunTimestamp karthidemo1.DedupResults