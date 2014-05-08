package com.ensighten.couchbase.sync.event.handlers;

import com.ensighten.couchbase.sync.event.CouchbaseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.EventHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.spy.memcached.tapmessage.TapOpcode;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.UUID;

/**
 * In the future there may be more of these, each particular to a different
 * destination.
 */
public class SolrEventImportHandler implements EventHandler<CouchbaseEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrEventImportHandler.class);
  private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();
  private static CloudSolrServer cloudSolrServer;

  static {
    try {
      Config config = ConfigFactory
          .parseFile(new File("conf/application.json"));
      cloudSolrServer = new CloudSolrServer(
          config.getString("solr.zkhost"));
      cloudSolrServer
          .setDefaultCollection(config.getString("solr.collection"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void onEvent(final CouchbaseEvent couchbaseEvent, final long l,
      final boolean b) throws Exception {

    LOGGER.debug(
        "key: " + couchbaseEvent.getKey() + " value: " + couchbaseEvent
            .getValue()
    );
    if (TapOpcode.DELETE == couchbaseEvent.getCouchbaseEventState()) {
      LOGGER.info("Deleting couchbase data with key:" + couchbaseEvent.getKey());
      cloudSolrServer.deleteByQuery("couchbase_id:" + couchbaseEvent.getKey());
    } else if (TapOpcode.MUTATION == couchbaseEvent.getCouchbaseEventState()) {
      //we need to check and see if the document is already there
      QueryResponse queryResponse = cloudSolrServer.query(new SolrQuery("couchbase_id:" + couchbaseEvent.getKey()));
      SolrDocumentList solrDocumentList = queryResponse.getResults();
      if (1 == solrDocumentList.getNumFound()) {
        LOGGER.info("Updating couchbase data for key:" + couchbaseEvent.getKey());
        //found the document let's do the update.
        SolrDocument solrDocument = solrDocumentList.get(0);
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        solrInputDocument.addField("id", solrDocument.getFieldValue("id"));
        solrInputDocument.addField("couchbase_id",solrDocument.getFieldValue("couchbase_id"));
        if (solrDocument.containsKey("cb_type")) {
          solrInputDocument.addField("cb_type",solrDocument.getFieldValue("cb_type"));
        }
        Map<String, Object> couchbaseValueMap = OBJECTMAPPER.readValue(couchbaseEvent.getValue(), Map.class);
        for (final Map.Entry<String, Object> entry : couchbaseValueMap.entrySet()) {
          solrInputDocument.addField(entry.getKey(), entry.getValue());
        }
        cloudSolrServer.add(solrInputDocument);
      } else {
        LOGGER.info("Adding new document with couchbase data for key:" + couchbaseEvent.getKey());
        //New CB doc, so add it.
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", UUID.randomUUID().toString());
        final String cbID = couchbaseEvent.getKey();
        doc.addField("couchbase_id", cbID);
        if (cbID.endsWith("_uid")) {
          doc.addField("cb_type", "user_profile");
        }
        Map<String, Object> couchbaseValueMap = OBJECTMAPPER.readValue(couchbaseEvent.getValue(), Map.class);
        for (final Map.Entry<String, Object> entry : couchbaseValueMap.entrySet()) {
          doc.addField(entry.getKey(), entry.getValue());
        }
        cloudSolrServer.add(doc);
      }

    }
    cloudSolrServer.commit();

  }

}
