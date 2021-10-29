package io.confluent.se.connect.azure.queuestorage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureStorageQueueSourceConfig extends AbstractConfig {

  private static final Logger LOG = LoggerFactory.getLogger(AzureStorageQueueSourceConfig.class);

  public static final String TOPIC_CONFIG = "topic";
  public static final String STORAGE_QUEUE_CONFIG = "storage.queue";
  public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
  public static final String EMPTY_SLEEP_MILLIS_CONFIG = "empty.sleep.millis";
  public static final String STORAGE_CONNECTION_STRING_CONFIG = "storage.connection.string";

  public static final int DEFAULT_TASK_BATCH_SIZE = 2000;
  public static final int DEFAULT_EMPTY_SLEEP_MILLIS = 3000;

  public static final ConfigDef CONFIG_DEF = config();

  private static ConfigDef config() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to")
             .define(STORAGE_CONNECTION_STRING_CONFIG, Type.STRING, Importance.HIGH, "Authentication credentials for Azure Storage Queues")
             .define(STORAGE_QUEUE_CONFIG, Type.STRING, Importance.HIGH, "Azure Storage Queue from which to consume")
             .define(EMPTY_SLEEP_MILLIS_CONFIG, Type.INT, DEFAULT_EMPTY_SLEEP_MILLIS, Importance.LOW,"Milliseconds to sleep if the Azure queue is empty")
             .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW, "The maximum number of records the Source task can read from file one time");
    return configDef;
  }

  public AzureStorageQueueSourceConfig(Map<?, ?> originals) {
    super(config(), originals);
  }

  public String getTopic() {
    return getString(TOPIC_CONFIG);
  }

  public String getStorageQueue() {
    return getString(STORAGE_QUEUE_CONFIG);
  }

  public int getTaskBatchSize() {
    return getInt(TASK_BATCH_SIZE_CONFIG);
  }

  public int getEmptySleepMillis() {
    return getInt(EMPTY_SLEEP_MILLIS_CONFIG);
  }

  public String getStorageConnectionString() {
    return getString(STORAGE_CONNECTION_STRING_CONFIG);
  }
}

