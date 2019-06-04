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

package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Test for {@link SpannerRecordConverter}.
 */
public class SpannerRecordConverterTest {

  private DdlToAvroSchemaConverter converter =
      new DdlToAvroSchemaConverter("booleans", "booleans");

  @Test
  public void simple() {
    Ddl ddl = Ddl.builder()
        .createTable("users")
        .column("id").int64().notNull().endColumn()
        .column("email").string().size(15).notNull().endColumn()
        .column("name").string().max().endColumn()
        .primaryKey().asc("id").end()
        .endTable()
        .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct = Struct.newBuilder()
        .set("id").to(1L)
        .set("email").to("abc@google.com")
        .set("name").to("John Doe")
        .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("email"), equalTo("abc@google.com"));
    assertThat(avroRecord.get("name"), equalTo("John Doe"));
  }

  @Test
  public void nulls() {
    Ddl ddl = Ddl.builder()
        .createTable("users")
        .column("id").int64().notNull().endColumn()
        .column("age").int64().endColumn()
        .column("name").string().max().endColumn()
        .column("bytes").bytes().max().endColumn()
        .column("date").date().endColumn()
        .column("ts").timestamp().endColumn()
        .primaryKey().asc("id").end()
        .endTable()
        .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct = Struct.newBuilder()
        .set("id").to(1L)
        .set("age").to((Long) null)
        .set("name").to((String) null)
        .set("bytes").to((ByteArray) null)
        .set("date").to((Date) null)
        .set("ts").to((Timestamp) null)
        .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("age"), is((Long) null));
    assertThat(avroRecord.get("name"), is((String) null));
    assertThat(avroRecord.get("bytes"), is((ByteArray) null));
    assertThat(avroRecord.get("date"), is((String) null));
    assertThat(avroRecord.get("ts"), is((String) null));
  }

  @Test
  public void dateTimestamp() {
    Ddl ddl = Ddl.builder()
        .createTable("users")
        .column("id").int64().notNull().endColumn()
        .column("date").date().endColumn()
        .column("ts").timestamp().endColumn()
        .primaryKey().asc("id").end()
        .endTable()
        .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct = Struct.newBuilder()
        .set("id").to(1L)
        .set("date").to(Date.fromYearMonthDay(2018, 2, 2))
        .set("ts").to(Timestamp.ofTimeMicroseconds(10))
        .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("date"), equalTo("2018-02-02"));
    assertThat(avroRecord.get("ts"), equalTo("1970-01-01T00:00:00.000010000Z"));
  }

  @Test
  public void arrays() {
    Ddl ddl = Ddl.builder()
        .createTable("users")
        .column("id").int64().notNull().endColumn()
        .column("ints").type(Type.array(Type.int64())).endColumn()
        .column("strings").type(Type.array(Type.string())).max().endColumn()
        .column("ts").type(Type.array(Type.timestamp())).endColumn()
        .column("date").type(Type.array(Type.date())).endColumn()
        .primaryKey().asc("id").end()
        .endTable()
        .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct = Struct.newBuilder()
        .set("id").to(1L)
        .set("ints").toInt64Array(Lists.newArrayList(1L, null, 2L))
        .set("strings").toStringArray(Lists.newArrayList(null, null, "one"))
        .set("ts").toTimestampArray(Lists.newArrayList(null, null,
            Timestamp.ofTimeMicroseconds(10L)))
        .set("date").toDateArray(Lists.newArrayList(null, null,
            Date.fromYearMonthDay(2018, 2, 2)))
        .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("ints"), equalTo(Arrays.asList(1L, null, 2L)));
    assertThat(avroRecord.get("strings"), equalTo(Arrays.asList(null, null, "one")));
    assertThat(avroRecord.get("date"), equalTo(Arrays.asList(null, null, "2018-02-02")));
    assertThat(avroRecord.get("ts"),
        equalTo(Arrays.asList(null, null, "1970-01-01T00:00:00.000010000Z")));
  }

}
