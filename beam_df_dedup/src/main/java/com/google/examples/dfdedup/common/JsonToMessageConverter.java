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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class JsonToMessageConverter extends PTransform<PCollection<String>, PCollection<Message>> {
    @Override
    public PCollection<Message> expand(PCollection<String> jsonMessage) {
        PCollection<Message> outMsgs =
                jsonMessage.apply(ParDo.of(new DoFn<String, Message>() {
                    Gson gson;

                    @StartBundle
                    public void startBundle() {
                        gson = new GsonBuilder().create();
                    }

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Message m = Message.fromJson(gson, c.element());
                        c.output(m);
                    }
                }));

        return outMsgs;
    }
}