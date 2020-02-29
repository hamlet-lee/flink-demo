/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playgrounds.ops.clickcount;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.playgrounds.ops.clickcount.functions.BackpressureMap;
import org.apache.flink.playgrounds.ops.clickcount.functions.ClickEventStatisticsCollector;
import org.apache.flink.playgrounds.ops.clickcount.functions.CountingAggregator;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEvent;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventDeserializationSchema;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventStatistics;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventStatisticsSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.joda.time.format.DateTimeFormat;
import org.nn.flink.streaming.connectors.kudu.KuduMapper;
import org.nn.flink.streaming.connectors.kudu.KuduSink;
import org.nn.flink.streaming.connectors.kudu.example.JsonKeyTableSerializationSchema;
import org.nn.flink.streaming.connectors.kudu.example.JsonKuduTableRowConverter;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A simple streaming job reading {@link ClickEvent}s from Kafka, counting events per 15 seconds and
 * writing the resulting {@link ClickEventStatistics} back to Kafka.
 *
 * <p> It can be run with or without checkpointing and with event time or processing time semantics.
 * </p>
 *
 * <p>The Job can be configured via the command line:</p>
 * * "--checkpointing": enables checkpointing
 * * "--event-time": set the StreamTimeCharacteristic to EventTime
 * * "--backpressure": insert an operator that causes periodic backpressure
 * * "--input-topic": the name of the Kafka Topic to consume {@link ClickEvent}s from
 * * "--output-topic": the name of the Kafka Topic to produce {@link ClickEventStatistics} to
 * * "--bootstrap.servers": comma-separated list of Kafka brokers
 *
 */
public class SimpleSinkKudu {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		configureEnvironment(params, env);

		String inputTopic = params.get("input-topic", "input");
		String brokers = params.get("bootstrap.servers", "localhost:9092");
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-sink-kudu");



		DataStream<JSONObject> clicks =
				env.addSource(new FlinkKafkaConsumer<>(inputTopic, new MyDeserializationSchema(), kafkaProps))
			.name("Input Source")
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.of(200, TimeUnit.MILLISECONDS)) {
				@Override
				public long extractTimestamp(final JSONObject element) {
					long ts = parseTS(element.get("timestamp").toString());
					return ts;
				}
			});


		Properties properties = new Properties();
		properties.setProperty("timeoutMillis", "50000");
		properties.setProperty("batchSize", "1000");
		System.out.println(params.get("masterAddress"));
		KuduSink<JSONObject> kudu = new KuduSink<JSONObject>(
				params.get("masterAddress"),
				new MyTableSerializationSchema("input2"),
				KuduMapper.Mode.UPSERT,
				new MyJsonKuduTableRowConverter(), false, properties);

		clicks
				.map(s -> s)
				.addSink(kudu);
		env.execute("simple-sink-kudu");
	}

	public static org.joda.time.format.DateTimeFormatter fmt = DateTimeFormat.forPattern("dd-MM-yyyy hh:mm:ss:SSS").withLocale( Locale.CHINA );

	private static long parseTS(String timestamp) {
		return fmt.parseMillis(timestamp);
	}

	private static void configureEnvironment(
			final ParameterTool params,
			final StreamExecutionEnvironment env) {

//		boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
//		boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);

//		if (checkpointingEnabled) {
			env.enableCheckpointing(1000);
//		}

//		if (eventTimeSemantics) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		}

		//disabling Operator chaining to make it easier to follow the Job in the WebUI
		env.disableOperatorChaining();
	}
}
