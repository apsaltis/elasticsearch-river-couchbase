package com.ensighten.couchbase.sync.event.handlers;

import com.ensighten.couchbase.sync.event.CouchbaseEvent;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.EventHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.spy.memcached.tapmessage.TapOpcode;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * In the future there may be more of these, each particular to a different
 * destination.
 */
public class SolrBatchImportEventHandler implements EventHandler<CouchbaseEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrBatchImportEventHandler.class);
  private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();
  private static final int DOC_BATCH_SIZE = 1024;
  private static CloudSolrServer cloudSolrServer;
  private Collection<SolrInputDocument> solrInputDocuments = new ArrayList<>();

  static {
    try {
      Config config = ConfigFactory
          .parseFile(new File("conf/application.json"));
      cloudSolrServer = new CloudSolrServer(
          config.getString("solr.zkhost"));
      cloudSolrServer.setDefaultCollection(
          config.getString("solr.collection"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void onEvent(final CouchbaseEvent couchbaseEvent, final long sequence,
      final boolean endOfBatch) throws Exception {

    if (TapOpcode.MUTATION == couchbaseEvent.getCouchbaseEventState()) {
      LOGGER.debug("key: " + couchbaseEvent.getKey() + " value: " + couchbaseEvent.getValue());
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", UUID.randomUUID().toString());
      final String cbID = couchbaseEvent.getKey();
      doc.addField("couchbase_id", cbID);
      if (cbID.endsWith("_uid")) {
        doc.addField("cb_type", "user_profile");
      }
      try {
        Map<String, Object> couchbaseValueMap = OBJECTMAPPER.readValue(couchbaseEvent.getValue(), Map.class);
        for (final Map.Entry<String, Object> entry : couchbaseValueMap.entrySet()) {
          doc.addField(entry.getKey(), entry.getValue());
        }
        solrInputDocuments.add(doc);
        if (DOC_BATCH_SIZE == solrInputDocuments.size()) {
          LOGGER.info("Sending " + solrInputDocuments.size() + " to Solr");
          cloudSolrServer.add(solrInputDocuments);
          cloudSolrServer.commit();
          solrInputDocuments.clear();
        }
      } catch (JsonMappingException jsonMappingException) {
        LOGGER.info("Caught invalid JSON exception, may be midstream of Couchbase Update. Couchbase key is: " +
            couchbaseEvent.getKey());
      } catch (Exception exception) {
        LOGGER.error("Caught exception while trying to process Coucbase document for key: " + couchbaseEvent.getKey());
      }
    }
  }

  /**
   * This needs to be called when we are done processing messages.
   */
  public final void flush() {

    try {
      if (0 < solrInputDocuments.size()) {
        LOGGER.info(
            "Flushing " + solrInputDocuments.size() + " to Solr");
        cloudSolrServer.add(solrInputDocuments);
        cloudSolrServer.commit();
        solrInputDocuments.clear();
      }
    } catch (SolrServerException | IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

}
