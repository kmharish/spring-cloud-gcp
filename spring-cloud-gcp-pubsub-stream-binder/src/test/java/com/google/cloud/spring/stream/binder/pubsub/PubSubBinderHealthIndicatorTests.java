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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

/** Tests for {@link PubSubBinderHealthIndicator}. */
@ExtendWith(MockitoExtension.class)
class PubSubBinderHealthIndicatorTests {

  @Mock private PubSubMessageChannelBinder binder;

  private PubSubBinderHealthIndicator healthIndicator;

  @BeforeEach
  void setUp() {
    this.healthIndicator = new PubSubBinderHealthIndicator(this.binder);
  }

  @Test
  void nullBinderIsRejected() {
    assertThatThrownBy(() -> new PubSubBinderHealthIndicator(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void upWhenNoAdapters() {
    when(this.binder.getSubscriberAdapters()).thenReturn(List.of());

    Health health = this.healthIndicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void upWhenAllSubscribersAlive() {
    PubSubInboundChannelAdapter adapter = mock(PubSubInboundChannelAdapter.class);
    when(adapter.isSubscriberFailed()).thenReturn(false);
    when(this.binder.getSubscriberAdapters()).thenReturn(List.of(adapter));

    Health health = this.healthIndicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void downWhenAnySubscriberFailed() {
    PubSubInboundChannelAdapter alive = mock(PubSubInboundChannelAdapter.class);
    when(alive.isSubscriberFailed()).thenReturn(false);

    PubSubInboundChannelAdapter dead = mock(PubSubInboundChannelAdapter.class);
    when(dead.isSubscriberFailed()).thenReturn(true);
    when(dead.getSubscriptionName()).thenReturn("dead-subscription");
    when(dead.getSubscriberFailure()).thenReturn(new IllegalStateException("streaming pull died"));

    when(this.binder.getSubscriberAdapters()).thenReturn(List.of(alive, dead));

    Health health = this.healthIndicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails())
        .containsEntry(
            "dead-subscription", "java.lang.IllegalStateException: streaming pull died");
  }
}
