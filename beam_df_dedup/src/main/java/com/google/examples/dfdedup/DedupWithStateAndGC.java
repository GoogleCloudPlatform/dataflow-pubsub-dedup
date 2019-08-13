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

package com.google.examples.dfdedup;

import com.google.examples.dfdedup.common.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedupWithStateAndGC {
    private static final Logger LOG = LoggerFactory.getLogger(DedupWithStateAndGC.class);

    public static void main(String[] args) {
        DedupPipelineOptions dfOptions =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(DedupPipelineOptions.class);

        GcpOptions gcpOptions = dfOptions.as(GcpOptions.class);
        String projectId = gcpOptions.getProject();

        Pipeline p = Pipeline.create(dfOptions);

        // read from PubSub
        PCollection<Message> psReadForDedup =
                p.apply("ReadFromPubSubForStateBasedDedup", new ReadFromPubSub(projectId,
                        dfOptions.getPubSubSubscriptionForDedup()));

        PCollection<Message> dedupedMsgs =
                psReadForDedup.apply("PubSubReadForStateBasedDedupWithGC",
                        new StateBasedDistinctWithTimer(dfOptions.getSessionGapWindowInMinutes()))
                        .apply(new AssignDfUniqueIds());

        dedupedMsgs.apply("DedupWithStateAndGCResults",
                new WriteMessagesToBQ(projectId,
                        dfOptions.getBQDataset(),
                        "DedupResults",
                        "StateAndGC")
        );

        LOG.info("Kicking off pipeline...");

        p.run();
    }
}
