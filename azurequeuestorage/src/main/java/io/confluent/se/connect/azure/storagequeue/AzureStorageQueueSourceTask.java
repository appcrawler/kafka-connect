package io.confluent.se.connect.azure.queuestorage;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureStorageQueueSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(AzureStorageQueueSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private CloudStorageAccount storageAccount;
    private CloudQueueClient queueClient;
    private CloudQueue queue;
    private String storageConnectionString;
    private String storageQueue;
    private int offset = 0;
    private String topic = null;
    private int batchSize;
    private int emptySleepMillis;

    private Long streamOffset;

    public AzureStorageQueueSourceTask() {
    }

    @Override
    public String version() {
        return new AzureStorageQueueSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
      log.info("Starting ServiceBus source task");
      AzureStorageQueueSourceConfig config =  new AzureStorageQueueSourceConfig(props);

      topic = config.getTopic();
      batchSize = config.getTaskBatchSize();
      emptySleepMillis = config.getEmptySleepMillis();
      storageConnectionString = config.getStorageConnectionString();
      storageQueue = config.getStorageQueue();
      try {
        storageAccount = CloudStorageAccount.parse(storageConnectionString);
        queueClient = storageAccount.createCloudQueueClient();
        queue = queueClient.getQueueReference(storageQueue);
        queue.createIfNotExists();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        try {
            ArrayList<SourceRecord> records = null;

            CloudQueueMessage retrievedMessage = null;
            while (true) {
              try {
                retrievedMessage = queue.retrieveMessage();
                if (retrievedMessage == null) {
                  Thread.sleep(emptySleepMillis);
                }
                else {
                  System.out.println("Retrieved messaged " + retrievedMessage.getMessageContentAsString().toUpperCase() + " in upper case");
                  if (records == null)
                    records = new ArrayList<>();
                  records.add(new SourceRecord(offsetKey("foo"), offsetValue(1L), topic, null,null, null, VALUE_SCHEMA, retrievedMessage.getMessageContentAsString(), System.currentTimeMillis()));
                  queue.deleteMessage(retrievedMessage);
                }
                if (records.size() >= batchSize) {
                  return records;
                }
              }
              catch (Exception e) {
                e.printStackTrace();
              }
            } 
        }
        catch (Exception e) {
          return null;
        }
    }

    private Map<String, String> offsetKey(String filename) {
      return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
      return Collections.singletonMap(POSITION_FIELD, pos);
    }

    @Override
    public void stop() {
      log.info("Stopping");
    }
}
