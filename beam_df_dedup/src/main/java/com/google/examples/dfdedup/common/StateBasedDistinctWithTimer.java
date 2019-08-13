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

import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import static com.google.common.base.MoreObjects.firstNonNull;

/*
  Dedup with Beam's internal state (with garbage collection)
 */
public class StateBasedDistinctWithTimer extends PTransform<PCollection<Message>, PCollection<Message>> {

    private final Integer stateSessionGapInMinutes;

    public StateBasedDistinctWithTimer(Integer stateSessionGapInMinutes) {
        this.stateSessionGapInMinutes = stateSessionGapInMinutes;
    }

    @Override
    public PCollection<Message> expand(PCollection<Message> msgs) {
        PCollection<Message> distinctMsgs =
                msgs.apply("MapMessagesToKV",
                        MapElements.into(
                                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(Message.class)))
                                .via((Message s) -> KV.of(s.getLogicalId(), s)))
                        .apply("RemoveDuplicatesWithStateKeeperWithTimerFn",
                                ParDo.of(new StateKeeperWithTimerFn(stateSessionGapInMinutes)))
                        .apply("MapBackToMessages", MapElements.into(TypeDescriptor.of(Message.class))
                                .via((KV<String, Message> kv) -> (kv.getValue())));

        return distinctMsgs;
    }

    private static class StateKeeperWithTimerFn extends DoFn<KV<String, Message>, KV<String, Message>> {

        private final Integer stateSessionGapInMinutes;

        @StateId("keyEncountered")
        private final StateSpec<ValueState<Boolean>> keyEncountered =
                StateSpecs.value(BooleanCoder.of());

        @TimerId("resetStateTimer")
        private final TimerSpec resetStateTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

        public StateKeeperWithTimerFn(Integer stateSessionGapInMinutes) {
            this.stateSessionGapInMinutes = stateSessionGapInMinutes;
        }

        @ProcessElement
        public void processElement(ProcessContext processContext,
                                   @TimerId("resetStateTimer") Timer resetStateTimer,
                                   @StateId("keyEncountered") ValueState<Boolean> keyEncounteredState) {

            resetStateTimer.offset(Duration.standardMinutes(stateSessionGapInMinutes)).setRelative();

            boolean keyEncountered = firstNonNull(keyEncounteredState.read(), false);

            if (!keyEncountered) {
                processContext.output(processContext.element());
                keyEncounteredState.write(true);
            }
        }

        @OnTimer("resetStateTimer")
        public void onResetStateTimer(OnTimerContext context,
                                      @StateId("keyEncountered") ValueState<Boolean> keyEncounteredState) {
            // relinquish state storage
            keyEncounteredState.clear();
        }
    }
}