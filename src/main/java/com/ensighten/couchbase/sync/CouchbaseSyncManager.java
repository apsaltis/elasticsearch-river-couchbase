package com.ensighten.couchbase.sync;

import com.couchbase.client.TapClient;
import com.ensighten.couchbase.sync.event.CouchbaseEvent;
import com.ensighten.couchbase.sync.event.CouchbaseEventFactory;
import com.ensighten.couchbase.sync.event.CouchbaseEventProducer;
import com.ensighten.couchbase.sync.event.handlers.SolrBatchImportEventHandler;
import com.ensighten.couchbase.sync.event.handlers.SolrEventImportHandler;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapStream;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class CouchbaseSyncManager {
  private static final Config CONFIG = ConfigFactory
      .parseFile(new File("conf/application.json"));
  private static final Logger LOGGER = LoggerFactory
      .getLogger(CouchbaseSyncManager.class);
  private static final int DISRUPTOR_BUFFER_SIZE = 1024;
  private static final Executor EXECUTOR = Executors.newCachedThreadPool();
  private static TapClient tapClient;
  private static Disruptor<CouchbaseEvent> disruptor;
  private static String tapName =
      "couchbase-sync-" + UUID.randomUUID().toString();

  private CouchbaseSyncManager() {
  }

  @SuppressWarnings("unchecked")
  //java -Dlogback.configurationFile=conf/logback.xml -cp couchbase-sync-all.jar
  // com.ensighten.couchbase.sync.CouchbaseSyncManager
  public static void main(final String[] args) throws IOException,
      ConfigurationException {

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("interrupt received, killing server…");
        shutdownGracefully();
      }
    });

    createCouchbaseClient();
    if (isFirstLaunch()) {
      LOGGER.debug("This is our first launch");
      //then we need to do a full dump and then setup the tap to continue.
      performInitialSync();
      performFutureSync(-1);
    } else {
      LOGGER.debug("We have been running before.");
      //get the last time from it and setup the tap.
      long lastReadTime = getLastReadTime();
      performFutureSync(lastReadTime);
    }

  }

  /**
   * This will do the initial synch from Couchbase assuming the the system it is sending data to has not received any
   * data from Couchbase before. This uses an event handler that does not do any checking to see if the data already
   * exists.
   * @throws IOException - This may get thrown if there are problems getting to Couchbase
   * @throws ConfigurationException - This may get called if the Couchbase configuration is not valid.
   */
  private static void performInitialSync() throws IOException,ConfigurationException {

    LOGGER.info("Starting to perform initial sync");
    //tapDump is basically that -- a dump of all data currently in CB.
    TapStream tapStream = tapClient.tapDump(tapName);
    SolrBatchImportEventHandler couchbaseToSolrEventHandler = new SolrBatchImportEventHandler();
    startupBatchDisruptor(couchbaseToSolrEventHandler);
    RingBuffer<CouchbaseEvent> ringBuffer = disruptor.getRingBuffer();

    CouchbaseEventProducer producer = new CouchbaseEventProducer(ringBuffer);
    //While there is data keep reading and producing
    while (!tapStream.isCompleted()) {
      while (tapClient.hasMoreMessages()) {
        ResponseMessage responseMessage;
        responseMessage = tapClient.getNextMessage();
        if (null != responseMessage) {
          String key = responseMessage.getKey();
          String value = new String(responseMessage.getValue());
          producer.onData(new String[] {key, value}, responseMessage.getOpcode());

        }
      }
    }
    LOGGER.info("Finished initial sync");
    shutdownBatchDisruptor();
    //Since this event handler is batch based, we need to let it know we are done and it could flush the remaining data.
    couchbaseToSolrEventHandler.flush();

  }

  /**
   * This will start a sync for data in Couchbase that changes from the start time forward.
   * @param startTime -- The time from which to be notified of new versions.
   * @throws IOException - This may get thrown if there are problems getting to Couchbase
   * @throws ConfigurationException - This may get called if the Couchbase configuration is not valid.
   */
  private static void performFutureSync(final long startTime) throws IOException,ConfigurationException {

    LOGGER.info("Starting to perform future sync");
    startupDisruptor();
    TapStream tapStream = tapClient
        .tapBackfill(tapName, startTime, 0, TimeUnit.MILLISECONDS);
    RingBuffer<CouchbaseEvent> ringBuffer = disruptor.getRingBuffer();

    CouchbaseEventProducer producer = new CouchbaseEventProducer(
        ringBuffer);
    while (!tapStream.isCompleted()) {
      while (tapClient.hasMoreMessages()) {
        ResponseMessage responseMessage;
        responseMessage = tapClient.getNextMessage();
        if (null != responseMessage) {
          String key = responseMessage.getKey();
          String value = new String(responseMessage.getValue());
          producer.onData(new String[] {key, value},
              responseMessage.getOpcode());
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  /**
   * This will startup the Disruptor queue with the Batch handler.
   */
  private static void startupBatchDisruptor(final SolrBatchImportEventHandler couchbaseToSolrBatchEventHandler) {
    LOGGER.debug("Starting Disruptor");
    // The factory for the event
    CouchbaseEventFactory factory = new CouchbaseEventFactory();

    disruptor = new Disruptor(factory,
        DISRUPTOR_BUFFER_SIZE,
        EXECUTOR,
        ProducerType.SINGLE, // Single producer
        new BlockingWaitStrategy()
    );

    // Connect the handler, down the road each of these handlers may come
    // from some Guice configuration so there would be a colleciton of them.
    // For today we just need one.
    disruptor.handleEventsWith(couchbaseToSolrBatchEventHandler);

    // Start the Disruptor, starts all threads running
    disruptor.start();
    LOGGER.debug("Disruptor started");
  }

  /**
   * This will shutdown the disruptor queue.
   */
  private static void shutdownBatchDisruptor() {
    disruptor.shutdown();
  }

  @SuppressWarnings("unchecked")
  /**
   * This will start the disruptor with a handler that is suitable for single event syncing. Basically for ongoing
   * sync but not for backfill.
   */
  private static void startupDisruptor() {
    LOGGER.debug("Starting Disruptor");
    // The factory for the event
    CouchbaseEventFactory factory = new CouchbaseEventFactory();

    disruptor = new Disruptor(factory,
        DISRUPTOR_BUFFER_SIZE,
        EXECUTOR,
        ProducerType.SINGLE, // Single producer
        new BlockingWaitStrategy()
    );

    // Connect the handler, down the road each of these handlers may come
    // from some Guice configuration so there would be a colleciton of them.
    // For today we just need one.
    disruptor.handleEventsWith(new SolrEventImportHandler());

    // Start the Disruptor, starts all threads running
    disruptor.start();
    LOGGER.debug("Disruptor started");
  }

  /**
   * Gracefully shut everything down as when the app is going down.
   */
  private static void shutdownGracefully() {
    //record out file time for the last access to the stream.
    try {
      disruptor.shutdown();
      setLastReadTime();
      closeCouchbaseClient();
    } catch (Exception ex) {
      //not much we can do, what if our logging is already shutdown or not up
      // when this happens?
      ex.printStackTrace();
    }

  }

  /**
   * Used to determine if the app has been launched before. This does not check the validity of the .state file,
   * just that it exists.
   * @return true - if this is the first launch of the app.
   *         false - if there is a couchbase-sync.state file.
   */
  private static boolean isFirstLaunch() {
    if (new File("couchbase-sync.state").exists()) {
      return false;
    }
    return true;
  }

  /**
   * Get the timestamp from when we shut down, this will be used to start a sync from a given time.
   * @return -- the time in ms of the time we shutdown from the state file. If this was the first launch it will
   * return -1
   * @throws IOException - If it has trouble reading the .state file.
   */
  private static long getLastReadTime() throws IOException {
    if (isFirstLaunch()) {
      return -1;
    } else {
      List<String> line = FileUtils.readLines(new File("couchbase-sync.state"));
      return Long.parseLong(line.get(0).split("\t")[1]);
    }
  }

  /**
   * This will write out the current time to the .state file and create it if it does not exist.
   * @throws IOException -- This will be thrown if there are problems creating or writing the file.
   */
  private static void setLastReadTime() throws IOException {
    final File stateFile = new File("couchbase-sync.state");
    if (!stateFile.exists()) {
      if (!stateFile.createNewFile()) {
        LOGGER.error("Failed to create .state file!");
      }
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(stateFile))) {
      writer.write(tapName + "\t" + DateTime.now(DateTimeZone.UTC).getMillis());
      writer.newLine();
    }

  }

  /**
   * Create the Couchbase client.
   */
  private static void createCouchbaseClient() {
    LOGGER.debug("Creating Couchbase Client");
    String bucket = CONFIG.getString("couchbase.bucket");
    String[] hosts = CONFIG.getString("couchbase.host").split(";");
    String port = CONFIG.getString("couchbase.port");
    String base = CONFIG.getString("couchbase.base");

    List<URI> uris = new ArrayList<>();
    for (final String host : hosts) {
      uris.add(URI.create("http://" + host + ":" + port + "/" + base));
    }
    try {
      tapClient = new TapClient(uris, bucket, "");
    } catch (Exception ex) {
      LOGGER.error("Caught exception trying to connect to Couchbase", ex);
    }
  }

  /**
   * We are done with Couchbase.
   */
  private static void closeCouchbaseClient() {
    try {
      if (null != tapClient) {
        LOGGER.debug("Shutting down Couchbase Client");
        tapClient.shutdown();
      }
    } catch (Exception ex) {
      LOGGER.error("Caught exception trying to shutdown Couchbase client",
          ex);
    }
  }

}
