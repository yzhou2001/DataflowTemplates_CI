/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.KafkaToBigQuery.MessageToTableRow;
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link KafkaToBigQueryTest} class. */
public class KafkaToBigQueryTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String RESOURCES_DIR = "JavascriptTextTransformerTest/";

  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "transform.js").getPath();

  /** Tests the {@link KafkaToBigQueryTest} pipeline end-to-end. */
  @Test
  public void testKafkaToBigQueryE2E() throws Exception {
    // Test input
    final String key = "{\"id\": \"1001\"}";
    final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94}";
    final KV<String, String> message = KV.of(key, payload);

    final Instant timestamp =
        new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

    final FailsafeElementCoder<KV<String, String>, String> coder =
        FailsafeElementCoder.of(
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    // Parameters
    ValueProvider<String> transformPath = pipeline.newProvider(TRANSFORM_FILE_PATH);
    ValueProvider<String> transformFunction = pipeline.newProvider("transform");

    KafkaToBigQuery.Options options =
        PipelineOptionsFactory.create().as(KafkaToBigQuery.Options.class);

    options.setJavascriptTextTransformGcsPath(transformPath);
    options.setJavascriptTextTransformFunctionName(transformFunction);

    // Build pipeline
    PCollectionTuple transformOut =
        pipeline
            .apply(
                "CreateInput",
                Create.of(message)
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("ConvertMessageToTableRow", new MessageToTableRow(options));

    // Assert
    PAssert.that(transformOut.get(KafkaToBigQuery.UDF_DEADLETTER_OUT)).empty();
    PAssert.that(transformOut.get(KafkaToBigQuery.TRANSFORM_DEADLETTER_OUT)).empty();
    PAssert.that(transformOut.get(KafkaToBigQuery.TRANSFORM_OUT))
        .satisfies(
            collection -> {
              TableRow result = collection.iterator().next();
              assertThat(result.get("ticker"), is(equalTo("GOOGL")));
              assertThat(result.get("price"), is(equalTo(1006.94)));
              return null;
            });

    // Execute pipeline
    pipeline.run();
  }
}
