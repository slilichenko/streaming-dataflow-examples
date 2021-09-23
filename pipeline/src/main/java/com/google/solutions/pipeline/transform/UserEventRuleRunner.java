/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.solutions.pipeline.transform;

import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.Finding.Level;
import com.google.solutions.pipeline.model.UserEventFinding;
import com.google.solutions.pipeline.model.UserProfile;
import com.google.solutions.pipeline.rule.user.UserEventContext;
import com.google.solutions.pipeline.rule.user.UserEventRule;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class UserEventRuleRunner extends
    DoFn<KV<String, Pair<Event, Set<UserEventRule>>>, UserEventFinding> {

  private static final long serialVersionUID = 1L;

  @StateId("profile")
  private final StateSpec<ValueState<UserProfile>> userProfileStateSpec = StateSpecs.value();

  @StateId("hosts-accessed")
  private final StateSpec<ValueState<Map<String, Instant>>> hostsAccessedStateSpec = StateSpecs
      .value();

  @StateId("suspicious-activity")
  private final StateSpec<BagState<UserEventFinding>> suspiciousActivitySpec = StateSpecs.bag();

  @TimerId("emit-suspicious-activity")
  private final TimerSpec emitSuspiciousActivitySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @TimerId("expiry")
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final PCollectionView<Set<UserEventRule>> rulesSideInput;

  public UserEventRuleRunner(
      PCollectionView<Set<UserEventRule>> rulesSideInput) {
    this.rulesSideInput = rulesSideInput;
  }

  @ProcessElement
  public void process(
      @Element KV<String, Pair<Event, Set<UserEventRule>>> eventAndRules,
      @StateId("profile") ValueState<UserProfile> userProfileState,
      @StateId("suspicious-activity") BagState<UserEventFinding> suspiciousActivityState,
      @StateId("hosts-accessed") ValueState<Map<String, Instant>> hostsAccessedState,
      @TimerId("emit-suspicious-activity") Timer emitSuspiciousActivityTimer,
      @TimerId("expiry") Timer expiryTimer,
      ProcessContext context) {
    String userId = eventAndRules.getKey();

    boolean updateProfile = false;
    var userProfile = userProfileState.read();
    if (userProfile == null) {
      userProfile = readFromPersistentStore(userId);
      updateProfile = true;
    }

    // Update last accessed time for host.
    Event event = eventAndRules.getValue().getLeft();
    String destinationIp = event.getDestinationIp();
    Instant requestTime = event.getRequestTime();

    Map<String, Instant> hosts = hostsAccessedState.read();
    if (hosts == null) {
      hosts = new HashMap<>();
    }
    Instant lastAccessTime = hosts.get(destinationIp);
    boolean newlyAccessedHost = lastAccessTime == null;
    if (newlyAccessedHost || lastAccessTime.isBefore(requestTime)) {
      Map<String, Instant> updatedHosts = new HashMap<>(hosts);
      updatedHosts.put(destinationIp, requestTime);
      hostsAccessedState.write(updatedHosts);

      //--- Set the expiry timer to run once per day to remove older hosts from the map
      expiryTimer.align(Duration.standardDays(1)).setRelative();
    }

    var rules = eventAndRules.getValue().getRight();

    UserEventContext userEventContext = new UserEventContext(userProfile, event, newlyAccessedHost);
    for (var rule : rules) {
      var findings = rule.analyze(userEventContext);
      for (var finding : findings) {
        UserEventFinding userEventFinding = UserEventFinding
            .create(event.getRequestTime(), userId, finding, event.getSourceIp());
        if (finding.getLevel() == Level.critical) {
          context.output(userEventFinding);
          continue;
        }

        // Set the timer in case it's the first event.
        if (!suspiciousActivityState.read().iterator().hasNext()) {
          emitSuspiciousActivityTimer.offset(Duration.standardSeconds(120)).setRelative();
        }

        suspiciousActivityState.add(userEventFinding);
      }
    }

    // Update user profile in case it changed.
    if (updateProfile) {
      userProfileState.write(userProfile);
    }
  }

  @OnTimer("emit-suspicious-activity")
  public void onSuspiciousActivityDeadline(
      @StateId("suspicious-activity") BagState<UserEventFinding> suspiciousActivityState,
      OutputReceiver<UserEventFinding> out) {
    // TODO: add de-duping
    var suspiciousActivities = suspiciousActivityState.read();
    suspiciousActivities.forEach(activity -> out.output(activity));

    suspiciousActivityState.clear();
  }

  @OnTimer("expiry")
  public void onExpiry(
      OnTimerContext context,
      @StateId("hosts-accessed") ValueState<Map<String, Instant>> hostLastAccessTime,
      @TimerId("expiry") Timer expiryTimer) {
    // Iterate over the collection and remove stale entries
    Instant cutoffTime = Instant.now().minus(Duration.standardDays(30));
    Map<String, Instant> newMap = new HashMap<>();
    hostLastAccessTime.read().entrySet().forEach(entry -> {
      if (entry.getValue().isAfter(cutoffTime)) {
        newMap.put(entry.getKey(), entry.getValue());
      }
    });
    hostLastAccessTime.write(newMap);

    //--- Schedule the next run
    expiryTimer.align(Duration.standardDays(1)).setRelative();
  }

  private UserProfile readFromPersistentStore(String userId) {
    // TODO: add retrieving the profile from persistent store.
    return UserProfile.builder().setStatus(UserProfile.Status.regular).build();
  }

}
