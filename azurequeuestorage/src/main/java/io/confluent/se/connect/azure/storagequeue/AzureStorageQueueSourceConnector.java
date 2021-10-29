package io.confluent.se.connect.azure.queuestorage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureStorageQueueSourceConnector extends SourceConnector {

    private String topic;
    private int batchSize;
    private int emptySleepMillis;
    private String storageQueue;
    private String storageConnectionString;
    private AzureStorageQueueSourceConfig parsedConfig;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
      parsedConfig = new AzureStorageQueueSourceConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
      return AzureStorageQueueSourceTask.class;
    }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> taskConfig = new HashMap<>(parsedConfig.originalsStrings());
      configs.add(taskConfig);
    }
    return configs;
  }
/*
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> config = new HashMap<>();
    config.put("topic", topic);
    config.put("task.batch.size", String.valueOf(batchSize));
    config.put("empty.sleep.millis", String.valueOf(emptySleepMillis));
    config.put("storage.connection.string", storageConnectionString);
    config.put("storage.queue", storageQueue);
    configs.add(config);
    return configs;
  }
*/

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
      return AzureStorageQueueSourceConfig.CONFIG_DEF;
    }
}
