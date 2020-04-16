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
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedupWithDistinct {
    private static final Logger LOG = LoggerFactory.getLogger(DedupWithDistinct.class);

    private static PCollection<Message> dedupWithDistinct(PCollection<Message> stream) {
        // [START FIXED_WINDOW_DISTINCT]
        PCollection<Message> windowedStream =
                stream.apply("WindowForDistinct",
                        Window.<Message>into(FixedWindows.of(Duration.standardSeconds(10)))
                                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                .withAllowedLateness(Duration.ZERO)
                                .accumulatingFiredPanes()
                );

        PCollection<Message> dedupedStream =
                windowedStream.apply(Distinct.withRepresentativeValueFn(
                        (Message m) -> (m.getLogicalId()))
                        .withRepresentativeType(TypeDescriptor.of(String.class)));
        // [END FIXED_WINDOW_DISTINCT]
        return dedupedStream;
    }

    private static PCollection<Message> dedupWithDistinctGlobal(PCollection<Message> stream) {
        // [START GLOBAL_WINDOW_DISTINCT]
        PCollection<Message> windowedStream =
                stream.apply("WindowForDistinct",
                        Window.<Message>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                .withAllowedLateness(Duration.ZERO)
                                .accumulatingFiredPanes()
                );

        PCollection<Message> dedupedStream =
                windowedStream.apply(Distinct.withRepresentativeValueFn(
                        (Message m) -> (m.getLogicalId()))
                        .withRepresentativeType(TypeDescriptor.of(String.class)));
        // [END GLOBAL_WINDOW_DISTINCT]
        return dedupedStream;
    }

    public static void main(String[] args) {
        DedupPipelineOptions dfOptions =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(DedupPipelineOptions.class);

        GcpOptions gcpOptions = dfOptions.as(GcpOptions.class);
        String projectId = gcpOptions.getProject();

        Pipeline p = Pipeline.create(dfOptions);

        // read from PubSub
        PCollection<Message> psRead =
                p.apply(new ReadFromPubSub(projectId,
                        dfOptions.getPubSubSubscriptionForDedup()));

        PCollection<Message> dedupedMsgs =
                dedupWithDistinct(psRead)
//                        .apply("Rewindow", Window.<Message>into(new GlobalWindows()))
                        .apply("AssignUniqueIds", new AssignDfUniqueIds());

        dedupedMsgs.apply(
                new WriteMessagesToBQ(projectId,
                        dfOptions.getBQDataset(),
                        "DedupResults",
                        "Distinct")
        );

        LOG.info("Kicking off pipeline...");

        p.run();
    }
}
