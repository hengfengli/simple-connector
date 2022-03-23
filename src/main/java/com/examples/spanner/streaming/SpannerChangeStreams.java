// Copyright 2019 Google Inc.
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

package com.examples.spanner.streaming;

import com.google.cloud.Timestamp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;


public class SpannerChangeStreams {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreams.class);

  private static final String SPANNER_HOST = "https://wrenchworks-loadtest.googleapis.com";
  private static final String PROJECT_ID = "cloud-spanner-backups-loadtest";
  private static final String INSTANCE_ID = "change-stream-load-test-3";
  private static final String DATABASE_ID = "load-test-change-stream-enable";
  private static final String CHANGE_STREAM_NAME = "changeStreamAll";
  private static final String METADATA_DATABASE = "change-stream-meta-hengfeng";
  private static final String METADATA_INSTANCE = "change-stream-load-test-3";
  private static final String GCS_PATH_PREFIX = "gs://hengfeng-cdc-loadtest/change-stream-records/connector";
  // private static final String TOPIC = "projects/cloud-spanner-backups-loadtest/topics/connector-test-hengfeng";

  public interface MySpannerOptions extends StreamingOptions {
  }

  public static void main(String[] args) throws IOException {
    MySpannerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MySpannerOptions.class);

    final Pipeline pipeline = Pipeline.create(options);

    pipeline.getOptions().as(MySpannerOptions.class).setStreaming(true);

    final SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withHost(StaticValueProvider.of(SPANNER_HOST))
        .withProjectId(PROJECT_ID)
        .withInstanceId(INSTANCE_ID)
        .withDatabaseId(DATABASE_ID);
    final Timestamp now = Timestamp.now();
    final Timestamp startTime = Timestamp.ofTimeSecondsAndNanos(now.getSeconds(), now.getNanos());
    // final Timestamp endTime = Timestamp.ofTimeSecondsAndNanos(startTime.getSeconds() + 5*60, startTime.getNanos());

    pipeline
        .apply(SpannerIO
            .readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(CHANGE_STREAM_NAME)
            .withMetadataDatabase(METADATA_DATABASE)
            .withMetadataInstance(METADATA_INSTANCE)
            .withInclusiveStartAt(startTime)
            // .withInclusiveEndAt(endTime)
        )
        .apply(
            "Transform records to strings",
            MapElements.into(TypeDescriptor.of(String.class))
                .via(
                    record -> {
                      // Timestamp currentTime = Timestamp.now();
                      // LOG.info("Received: {}", record);
                      // return String.format("%s,%s,%s,%s,%d", record.getPartitionToken(), record.getCommitTimestamp(), currentTime.toString(), record.getServerTransactionId(), record.getNumberOfRecordsInTransaction());

                      // Timestamp currentTime = Timestamp.now();
                      // return String.format("%s,%s,%s,%d,%s", record.getPartitionToken(), record.getCommitTimestamp(), record.getServerTransactionId(), record.getNumberOfRecordsInTransaction(), currentTime.toString());
                      
                      return String.format("%s,%s,%s,%d", record.getPartitionToken(), record.getCommitTimestamp(), record.getServerTransactionId(), record.getNumberOfRecordsInTransaction());
                    }))
        
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply("Write records to GCS", 
          new WriteOneFilePerWindow(GCS_PATH_PREFIX , 5));

        // .apply(PubsubIO.writeStrings().to(TOPIC));

        // .apply(TextIO
        //   .write()
        //   .to(GCS_PATH_PREFIX)
        //   .withSuffix(".txt")
        //   .withWindowedWrites()
        //   .withNumShards(5)
        // );

    pipeline.run().waitUntilFinish();
  }
}