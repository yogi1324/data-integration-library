// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.
package com.linkedin.cdi.factory;

import com.linkedin.cdi.event.EventHelper;
import com.linkedin.cdi.factory.producer.EventReporter;
import com.linkedin.cdi.factory.producer.EventReporterFactory;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import static com.linkedin.cdi.configuration.PropertyCollection.*;


/**
 * A Log wrapper class to send log events to Stdout and Kafka
 */
public class LogWrapper {

  private Logger LOG;
  private State state;
  private boolean enableKafkaLogging;
  private EventReporter producer;

  public LogWrapper(State state, Class<?> clazz) {
    this.enableKafkaLogging = MSTAGE_KAFKA_LOGGING_ENABLED.get(state);
    this.state = state;
    LOG = LoggerFactory.getLogger(clazz);
    if (enableKafkaLogging) {
      producer = EventReporterFactory.getLoggingReporter(state);
    }
  }

  // DEBUG
  public void debug(String message) {
    LOG.debug(message);
    if (enableKafkaLogging && producer != null) {
      send(message, Level.DEBUG.toString());
    }
  }

  public void debug(String message, Object arg1) {
    LOG.debug(message, arg1);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1).getMessage();
      send(message, Level.DEBUG.toString());
    }
  }

  public void debug(String message, Object arg1, Object arg2) {
    LOG.debug(message, arg1, arg2);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1, arg2).getMessage();
      send(message, Level.DEBUG.toString());
    }
  }

  public void debug(String message, Object... args) {
    LOG.debug(message, args);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, args).getMessage();
      send(message, Level.DEBUG.toString());
    }
  }

  public void debug(String message, Throwable throwable) {
    LOG.debug(message, throwable);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, throwable).getMessage();
      send(message, Level.DEBUG.toString());
    }
  }

  public void info(String message) {
    LOG.info(message);
    if (enableKafkaLogging && producer != null) {
      send(message, Level.INFO.toString());
    }
  }

  public void info(String message, Object arg1) {
    LOG.info(message, arg1);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1).getMessage();
      send(message, Level.INFO.toString());
    }
  }

  public void info(String message, Object arg1, Object arg2) {
    LOG.info(message, arg1, arg2);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1, arg2).getMessage();
      send(message, Level.INFO.toString());
    }
  }

  public void info(String message, Object... args) {
    LOG.info(message, args);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, args).getMessage();
      send(message, Level.INFO.toString());
    }
  }

  public void info(String message, Throwable throwable) {
    LOG.info(message, throwable);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, throwable).getMessage();
      send(message, Level.INFO.toString());
    }
  }

  // WARN
  public void warn(String message) {
    LOG.warn(message);
    if (enableKafkaLogging && producer != null) {
      send(message, Level.WARN.toString());
    }
  }

  public void warn(String message, Object arg1) {
    LOG.warn(message, arg1);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1).getMessage();
      send(message, Level.WARN.toString());
    }
  }

  public void warn(String message, Object arg1, Object arg2) {
    LOG.warn(message, arg1, arg2);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1, arg2).getMessage();
      send(message, Level.WARN.toString());
    }
  }

  public void warn(String message, Object... args) {
    LOG.warn(message, args);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, args).getMessage();
      send(message, Level.WARN.toString());
    }
  }

  public void warn(String message, Throwable throwable) {
    LOG.warn(message, throwable);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, throwable).getMessage();
      send(message, Level.WARN.toString());
    }
  }

  // ERROR
  public void error(String message) {
    LOG.error(message);
    if (enableKafkaLogging && producer != null) {
      send(message, Level.ERROR.toString());
    }
  }

  public void error(String message, Object arg1) {
    LOG.error(message, arg1);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1).getMessage();
      send(message, Level.ERROR.toString());
    }
  }

  public void error(String message, Object arg1, Object arg2) {
    LOG.error(message, arg1, arg2);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, arg1, arg2).getMessage();
      send(message, Level.ERROR.toString());
    }
  }

  public void error(String message, Object... args) {
    LOG.error(message, args);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, args).getMessage();
      send(message, Level.ERROR.toString());
    }
  }

  public void error(String message, Throwable throwable) {
    LOG.error(message, throwable);
    if (enableKafkaLogging && producer != null) {
      message = MessageFormatter.format(message, throwable).getMessage();
      send(message, Level.ERROR.toString());
    }
  }

  private void send(String message, String level) {
    producer.send(EventHelper.createCdiLoggingEvent(state, level, message));
  }

  public void close() {
    if (producer != null) {
      producer.close();
    }
  }

  private enum Level {
    DEBUG("DEBUG"), INFO("INFO"), WARN("WARN"), ERROR("ERROR");
    private final String level;

    Level(String level) {
      this.level = level;
    }

    @Override
    public String toString() {
      return level;
    }
  }
}
