/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spring.stream.binder.pubsub;

import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.util.Assert;

/**
 * Health indicator for the Pub/Sub stream binder, reported under the {@code binders} health
 * contributor.
 *
 * <p>Reports {@code DOWN} while any subscriber created by the binder has permanently failed (for
 * example, after a non-retryable streaming pull error), since such a subscriber no longer receives
 * messages until its binding is restarted.
 *
 * @since 8.2
 */
public class PubSubBinderHealthIndicator extends AbstractHealthIndicator {

  private final PubSubMessageChannelBinder binder;

  public PubSubBinderHealthIndicator(PubSubMessageChannelBinder binder) {
    Assert.notNull(binder, "The binder can't be null.");
    this.binder = binder;
  }

  @Override
  protected void doHealthCheck(Health.Builder builder) {
    boolean anySubscriberFailed = false;

    for (PubSubInboundChannelAdapter adapter : this.binder.getSubscriberAdapters()) {
      if (adapter.isSubscriberFailed()) {
        anySubscriberFailed = true;
        Throwable failure = adapter.getSubscriberFailure();
        builder.withDetail(
            adapter.getSubscriptionName(),
            failure != null ? failure.toString() : "subscriber failed");
      }
    }

    if (anySubscriberFailed) {
      builder.down();
    } else {
      builder.up();
    }
  }
}
