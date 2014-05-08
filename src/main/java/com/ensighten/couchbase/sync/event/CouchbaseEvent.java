package com.ensighten.couchbase.sync.event;

import net.spy.memcached.tapmessage.TapOpcode;

/**
 * Created by andrewpsaltis on 4/28/14.
 */
public final class CouchbaseEvent {

  /**
   * The Couchbase document key.
   */
  private String internalKey;
  /**
   * The value of the Couchbase document.
   */
  private String internalValue;
  /**
   * The type of Couchbase event.
   */
  private TapOpcode internalCouchbaseEventState;

  /**
   * Returns the Couchbase key this event relates to.
   *
   * @return String
   */
  public String getKey() {
    return internalKey;
  }

  /**
   * Set the key for this Couchbase event.
   *
   * @param key - The key
   */
  public void setKey(final String key) {
    this.internalKey = key;
  }

  /**
   * This returns the value of the Couchbase document.
   *
   * @return String
   */
  public String getValue() {
    return internalValue;
  }

  /**
   * Set the value of this event to be the Couchbase document value.
   *
   * @param value - The value for the document
   */
  public void setValue(final String value) {
    this.internalValue = value;
  }

  /**
   * Returns the TapOpcode for this event.
   *
   * @return TapOpcode
   */
  public TapOpcode getCouchbaseEventState() {
    return internalCouchbaseEventState;
  }

  /**
   * Set the state of the this Couchbase Event.
   *
   * @param couchbaseEventState - The state from the operation
   */
  public void setCouchbaseEventState(final TapOpcode couchbaseEventState) {
    this.internalCouchbaseEventState = couchbaseEventState;
  }
}
