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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class WriteMessagesToBQ extends PTransform<PCollection<Message>, WriteResult> {

    private String bqTableSpec;
    private String dedupType;

    public WriteMessagesToBQ(String projectId,
                             String bqDataset,
                             String destTable,
                             String dedupType) {
        StringBuilder sbTableSpec = new StringBuilder();

        sbTableSpec
                .append(projectId).append(":")
                .append(bqDataset).append(".")
                .append(destTable);

        this.bqTableSpec = sbTableSpec.toString();
        this.dedupType = dedupType;
    }

    @Override
    public WriteResult expand(PCollection<Message> input) {


        BigQueryIO.Write.Method writeMethod = BigQueryIO.Write.Method.STREAMING_INSERTS;

        return input.apply(BigQueryIO.<Message>write()
                .to(this.bqTableSpec)
                .withSchema(new TableSchema().setFields(
                        ImmutableList.of(
                                new TableFieldSchema().setName("RunName").setType("STRING"),
                                new TableFieldSchema().setName("RunTimestamp").setType("TIMESTAMP"),
                                new TableFieldSchema().setName("EventTime").setType("TIMESTAMP"),
                                new TableFieldSchema().setName("GenId").setType("INTEGER"),
                                new TableFieldSchema().setName("DfUniqueId").setType("INTEGER"),
                                new TableFieldSchema().setName("DedupType").setType("STRING"),
                                new TableFieldSchema().setName("Number").setType("INTEGER"))))
                .withFormatFunction(msg -> new TableRow()
                        .set("RunName", msg.getRunName())
                        .set("RunTimestamp", msg.getRunTimestampAsInstant().toEpochMilli() / 1000)
                        .set("EventTime", msg.getEventTimeAsInstant().toEpochMilli() / 1000)
                        .set("GenId", msg.getGenId())
                        .set("DfUniqueId", msg.getDfUniqueId())
                        .set("DedupType", this.dedupType)
                        .set("Number", msg.getVal()))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withTimePartitioning(new TimePartitioning().setField("RunTimestamp").setType("TIMESTAMP"))
                .withMethod(writeMethod)
        );
    }
}
