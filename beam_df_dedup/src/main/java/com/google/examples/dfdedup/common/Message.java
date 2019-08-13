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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;


@DefaultCoder(AvroCoder.class)
public class Message {
    protected String LogicalId;

    public String getLogicalId() {
        return LogicalId;
    }

    public void setLogicalId(String logicalId) {
        this.LogicalId = logicalId;
    }

    protected String RunName;

    public String getRunName() {
        return RunName;
    }

    public void setRunName(String runName) {
        this.RunName = runName;
    }

    protected String RunTimestamp;

    public String getRunTimestamp() {
        return RunTimestamp;
    }

    public void setRunTimestamp(String runTimestamp) {
        this.RunTimestamp = runTimestamp;
    }

    public Instant getRunTimestampAsInstant() {
        return getTimestampAsInstant(this.RunTimestamp);
    }

    protected String EventTime;

    public String getEventTime() {
        return EventTime;
    }

    public void setEventTime(String eventTime) {
        this.EventTime = eventTime;
    }

    public Instant getEventTimeAsInstant() {
        return getTimestampAsInstant(this.EventTime);
    }
    public org.joda.time.Instant getEventTimeAsJodaInstant() {
        return getTimestampAsJodaTimeInstant(this.EventTime);
    }

    protected Integer Val;

    public Integer getVal() {
        return Val;
    }

    public void setVal(Integer val) {
        this.Val = val;
    }

    protected Integer GenId;

    public Integer getGenId() {
        return GenId;
    }

    public void setGenId(Integer genId) {
        this.GenId = genId;
    }

    protected Integer DfUniqueId = 0;

    public Integer getDfUniqueId() {
        return DfUniqueId;
    }

    public void setDfUniqueId(Integer dfUniqueId) {
        this.DfUniqueId = dfUniqueId;
    }

    private Instant getTimestampAsInstant(String timestampStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        TemporalAccessor temporalAccessor = formatter.parse(timestampStr);
        LocalDateTime localDateTime = LocalDateTime.from(temporalAccessor);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        return Instant.from(zonedDateTime);
    }

    private org.joda.time.Instant getTimestampAsJodaTimeInstant(String timestampStr) {
        Instant i = getTimestampAsInstant(timestampStr);
        return new org.joda.time.Instant(i.toEpochMilli());
    }

    public String toJson(Gson gson) {
        return gson.toJson(this);
    }

    public static Message fromJson(Gson gson, String rawJson) {
        return gson.fromJson(rawJson, Message.class);
    }
}
