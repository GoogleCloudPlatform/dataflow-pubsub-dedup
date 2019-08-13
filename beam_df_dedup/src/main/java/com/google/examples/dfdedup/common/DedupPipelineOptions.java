// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.examples.dfdedup.common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface DedupPipelineOptions extends DataflowPipelineOptions {
    /**
     * BQ Dataset
     */
    @Description("BQ Dataset")
    @Default.String("")
    String getBQDataset();

    void setBQDataset(String value);

    /**
     * PubSub Subscription (dedup)
     */
    @Description("PubSub Subscription (dedup)")
    @Default.String("")
    String getPubSubSubscriptionForDedup();

    void setPubSubSubscriptionForDedup(String value);

    /**
     * Session gap window in minutes (for state based distinct with GC)
     */
    @Description("Session gap window in minutes (for state based distinct with GC)")
    @Default.Integer(20)
    Integer getSessionGapWindowInMinutes();

    void setSessionGapWindowInMinutes(Integer value);

} // interface