package com.ensighten.couchbase.sync.event;

import com.lmax.disruptor.EventFactory;

/**
 * This is responsible for generating the CouchbaseEvents needed for the
 * Disruptor.
 */
public class CouchbaseEventFactory implements EventFactory {
  @Override
  public final Object newInstance() {
    return new CouchbaseEvent();
  }
}
