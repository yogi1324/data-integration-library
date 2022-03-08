// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.
package com.linkedin.cdi.factory.producer;

import com.linkedin.kafka.clients.producer.LiKafkaProducerImpl;
import org.apache.avro.generic.IndexedRecord;
import org.apache.gobblin.configuration.State;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


public class KafkaLoggingReporter implements EventReporter<IndexedRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLoggingReporter.class);
  private Producer producer;
  private State state;
  private String topicName;

  public KafkaLoggingReporter(State state) {
    this.state = state;
    this.topicName = MSTAGE_KAFKA_LOGGER_PROPERTIES.getTopicName(state);
    MSTAGE_KAFKA_LOGGER_PROPERTIES.fillKafkaConfig(state);
  }

  private Producer getProducer() {
    if (producer != null) {
      return producer;
    }
    return new LiKafkaProducerImpl<String, IndexedRecord>(this.state.getProperties());
  }

  @Override
  public void send(IndexedRecord event) {

    if (producer == null) {
      producer = getProducer();
      LOG.info("Kafka producer for Logging is initialized");
    }
    producer.send(new ProducerRecord(topicName, event));
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }
}
