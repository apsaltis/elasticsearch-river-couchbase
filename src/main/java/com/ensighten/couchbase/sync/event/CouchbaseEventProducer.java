package com.ensighten.couchbase.sync.event;

import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import net.spy.memcached.tapmessage.TapOpcode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This handles producing events onto the Disruptor Queue
 */
public final class CouchbaseEventProducer {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(CouchbaseEventProducer.class);
  private static final EventTranslatorTwoArg<CouchbaseEvent, String[],
      TapOpcode> TRANSLATOR = new EventTranslatorTwoArg<CouchbaseEvent, String[], TapOpcode>() {
    public void translateTo(final CouchbaseEvent event, final long sequence,
        final String[] couchbaseMessage, final TapOpcode tapOpcode) {
      event.setKey(couchbaseMessage[0]);
      event.setValue(couchbaseMessage[1]);
      event.setCouchbaseEventState(tapOpcode);
    }
  };

  private final RingBuffer<CouchbaseEvent> ringBuffer;

  public CouchbaseEventProducer(final RingBuffer<CouchbaseEvent>
      passedRingBuffer) {
    this.ringBuffer = passedRingBuffer;
  }

  /**
   * This will publish the event onto the queue
   *
   * @param couchbaseMessage -- the couchbase message with the key being in [0] and the value in [1]
   * @param tapOpcode        - The TapOpcode of the Couchbase event.
   */
  public void onData(final String[] couchbaseMessage,
      final TapOpcode tapOpcode) {
    LOGGER.debug("onData");
    ringBuffer.publishEvent(TRANSLATOR, couchbaseMessage, tapOpcode);
  }
}
