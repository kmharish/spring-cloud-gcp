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

package com.google.cloud.spring.pubsub.integration.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberOperations;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;

/**
 * Tests that a permanently FAILED Pub/Sub {@link Subscriber} is surfaced by {@link
 * PubSubInboundChannelAdapter} rather than silently ignored.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(OutputCaptureExtension.class)
class SubscriberFailureSupervisionTests {

  private final TestUtils.TestApplicationContext context = TestUtils.createTestApplicationContext();

  @Mock private PubSubSubscriberOperations subscriberOperations;
  @Mock private MessageChannel outputChannel;

  @SuppressWarnings("unchecked")
  private PubSubInboundChannelAdapter startedAdapterFor(Subscriber subscriber) {
    when(this.subscriberOperations.subscribeAndConvert(
            anyString(), any(Consumer.class), any(Class.class)))
        .thenReturn(subscriber);

    PubSubInboundChannelAdapter adapter =
        new PubSubInboundChannelAdapter(this.subscriberOperations, "testSubscription");
    adapter.setOutputChannel(this.outputChannel);
    adapter.setBeanFactory(this.context);
    adapter.start();
    return adapter;
  }

  private ApiService.Listener captureListener(Subscriber subscriber) {
    ArgumentCaptor<ApiService.Listener> captor = ArgumentCaptor.forClass(ApiService.Listener.class);
    verify(subscriber).addListener(captor.capture(), any(Executor.class));
    return captor.getValue();
  }

  @Test
  void failureListenerIsRegisteredEvenWithoutHealthTracking() {
    Subscriber subscriber = mock(Subscriber.class);
    PubSubInboundChannelAdapter adapter = startedAdapterFor(subscriber);

    assertThat(captureListener(subscriber)).isNotNull();
    assertThat(adapter.isSubscriberFailed()).isFalse();
    assertThat(adapter.getSubscriberFailure()).isNull();
  }

  @Test
  void failedSubscriberStopsAdapterAndRecordsFailure(CapturedOutput output) {
    Subscriber subscriber = mock(Subscriber.class);
    PubSubInboundChannelAdapter adapter = startedAdapterFor(subscriber);
    assertThat(adapter.isRunning()).isTrue();

    IllegalStateException cause = new IllegalStateException("streaming pull died");

    // Drive the ApiService FAILED transition, exactly as StreamingSubscriberConnection does on a
    // non-retryable stream error ("terminated streaming with exception").
    captureListener(subscriber).failed(ApiService.State.RUNNING, cause);

    assertThat(adapter.isRunning()).isFalse();
    assertThat(adapter.isSubscriberFailed()).isTrue();
    assertThat(adapter.getSubscriberFailure()).isSameAs(cause);
    assertThat(output.getOut() + output.getErr())
        .contains("Pub/Sub subscriber for subscription 'testSubscription' failed");
  }

  @Test
  void restartingTheAdapterClearsThePreviousFailure() {
    Subscriber subscriber = mock(Subscriber.class);
    PubSubInboundChannelAdapter adapter = startedAdapterFor(subscriber);

    captureListener(subscriber).failed(ApiService.State.RUNNING, new IllegalStateException("boom"));
    assertThat(adapter.isRunning()).isFalse();

    adapter.start();

    assertThat(adapter.isRunning()).isTrue();
    assertThat(adapter.isSubscriberFailed()).isFalse();
    assertThat(adapter.getSubscriberFailure()).isNull();
  }

  @Test
  void failureAfterManualStopDoesNotBreakTheStoppedAdapter() {
    Subscriber subscriber = mock(Subscriber.class);
    PubSubInboundChannelAdapter adapter = startedAdapterFor(subscriber);
    ApiService.Listener listener = captureListener(subscriber);

    adapter.stop();
    listener.failed(ApiService.State.STOPPING, new IllegalStateException("boom"));

    assertThat(adapter.isRunning()).isFalse();
    assertThat(adapter.isSubscriberFailed()).isTrue();
  }
}
