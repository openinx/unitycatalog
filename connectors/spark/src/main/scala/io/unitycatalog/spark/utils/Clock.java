package io.unitycatalog.spark.utils;

import java.time.Duration;
import java.time.Instant;

public interface Clock {
  /**
   * @return the current time of the clock.
   */
  Instant now();

  /**
   * Advances the current time of this clock by the specified duration. After this call,
   * {@link #now()} should return a time equal to the previous time plus the given {@code duration}.
   */
  void advance(Duration duration);

  static Clock systemClock() {
    return SystemClock.SINGLETON;
  }

  static Clock manualClock(Instant now) {
    return new ManualClock(now);
  }

  class SystemClock implements Clock {
    private static final SystemClock SINGLETON = new SystemClock();

    @Override
    public Instant now() {
      return Instant.now();
    }

    @Override
    public void advance(Duration duration) {
      throw new UnsupportedOperationException("Cannot advance system clock.");
    }
  }

  class ManualClock implements Clock {
    private volatile Instant now;

    ManualClock(Instant now) {
      this.now = now;
    }

    @Override
    public synchronized Instant now() {
      return now;
    }

    @Override
    public synchronized void advance(Duration duration) {
      now = now.plus(duration);
    }
  }
}
