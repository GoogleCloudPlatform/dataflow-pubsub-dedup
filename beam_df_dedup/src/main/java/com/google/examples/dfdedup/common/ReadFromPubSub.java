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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class ReadFromPubSub extends PTransform<PBegin, PCollection<Message>> {
    private final String eventTimeName = "event_time";
    private final String projectId;
    private final String subscription;
    private final String logicalIdName;
    private final Boolean dedup;

    public ReadFromPubSub(String projectId,
                          String subscription,
                          String logicalIdName,
                          Boolean dedup) {
        this.projectId = projectId;
        this.subscription = subscription;
        this.logicalIdName = logicalIdName;
        this.dedup = dedup;
    }

    public ReadFromPubSub(String projectId,
                          String subscription) {
        this.projectId = projectId;
        this.subscription = subscription;
        this.logicalIdName = "";
        this.dedup = false;
    }

    @Override
    public PCollection<Message> expand(PBegin begin) {
        PubsubIO.Read<String> psReadJsonMessages = PubsubIO.readStrings();
        PCollection<String> readFromPS;

        if(dedup) {
            String subName =
                    "projects/" + projectId + "/subscriptions/" + subscription;

            // [START PUBSUBIO_WITHIDATTRIBUTE]
            readFromPS = begin.apply(psReadJsonMessages
                            .fromSubscription(subName)
                            .withTimestampAttribute(eventTimeName)
                            // include withIdAttribute call for deduplication
                            .withIdAttribute(logicalIdName))
                    .setCoder(StringUtf8Coder.of());
            // [END PUBSUBIO_WITHIDATTRIBUTE]
        } else {
            String subName =
                    "projects/" + projectId + "/subscriptions/" + subscription;

            // no deduplication (withIdAttribute call is absent)
            readFromPS = begin.apply(psReadJsonMessages
                            .fromSubscription(subName)
                            .withTimestampAttribute(eventTimeName))
                    .setCoder(StringUtf8Coder.of());
        }

        PCollection<Message> msgs =
                readFromPS.apply(ParDo.of(new DoFn<String, Message>() {
                    Gson gson;

                    @StartBundle
                    public void startBundle() {
                        gson = new GsonBuilder().create();
                    }

                    @ProcessElement
                    public void ProcessElement(ProcessContext c) {
                        Message m = Message.fromJson(gson, c.element());
                        c.output(m);
                    }
                }));

        return msgs;
    }
}
