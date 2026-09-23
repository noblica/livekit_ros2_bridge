// Copyright (c) 2025-present Polymath Robotics, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "room_connection/remote_track_router.hpp"

namespace livekit_ros2_bridge
{
namespace
{

// SDK publications and tracks cannot be built without a live room, so every handle here is null.

using Record = RemotePublicationMirror::Record;

constexpr char kOperatorTrack[] = "lkros.audio.operator";

Record makeRecord(
  const std::string & participant_identity,
  const std::string & track_sid,
  const std::string & track_name = kOperatorTrack,
  bool subscribed = false)
{
  return Record{
    RoomConnection::RemoteTrackSnapshotEntry{
      participant_identity, track_sid, track_name, livekit::TrackKind::KIND_AUDIO, subscribed},
    nullptr};
}

RemoteTrackEventInput withPublication(const Record & record)
{
  RemoteTrackEventInput input;
  input.participant_identity = record.entry.participant_identity;
  input.publication = record;
  return input;
}

// The SDK v1.6.0 TrackPublished shape: a participant but a null publication.
RemoteTrackEventInput withoutPublication(const std::string & participant_identity)
{
  RemoteTrackEventInput input;
  input.participant_identity = participant_identity;
  return input;
}

std::function<std::vector<Record>()> hydrated(std::vector<Record> records)
{
  return [records]() { return records; };
}

std::function<std::vector<Record>()> noHydration()
{
  return []() -> std::vector<Record> {
    ADD_FAILURE() << "participant publications read for an event that carries its publication";
    return {};
  };
}

std::optional<RoomConnection::RemoteTrackSnapshotEntry> findEntry(
  const RemoteTrackRouter & router, const std::string & track_sid)
{
  for (const auto & entry : router.snapshot()) {
    if (entry.track_sid == track_sid) {
      return entry;
    }
  }
  return std::nullopt;
}

TEST(RemoteTrackRouterTest, PublishedWithPublicationMergesAndForwardsWhileConnected)
{
  RemoteTrackRouter router;

  const auto events = router.trackPublished(
    withPublication(makeRecord("operator-a", "TR_a")), noHydration(), livekit::ConnectionState::Connected);

  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events.front().participant_identity, "operator-a");
  EXPECT_EQ(events.front().track_sid, "TR_a");
  EXPECT_EQ(events.front().track_name, kOperatorTrack);
  EXPECT_EQ(events.front().track_kind, livekit::TrackKind::KIND_AUDIO);
  EXPECT_EQ(events.front().track, nullptr);
  EXPECT_TRUE(findEntry(router, "TR_a").has_value());
}

TEST(RemoteTrackRouterTest, PublishedWithoutParticipantIsIgnored)
{
  RemoteTrackRouter router;
  RemoteTrackEventInput input;
  input.publication = makeRecord("", "TR_a");

  EXPECT_TRUE(router.trackPublished(input, noHydration(), livekit::ConnectionState::Connected).empty());
  EXPECT_TRUE(router.snapshot().empty());
}

TEST(RemoteTrackRouterTest, NullPublicationRecoversUnseenEntriesFromHydratedRecords)
{
  RemoteTrackRouter router;
  router.beginSeed();
  router.completeSeed({makeRecord("operator-a", "TR_a")});

  const auto events = router.trackPublished(
    withoutPublication("operator-a"),
    hydrated({makeRecord("operator-a", "TR_a"), makeRecord("operator-a", "TR_b", "second")}),
    livekit::ConnectionState::Connected);

  // TR_a was seeded at connect, so only the publication the event actually announced is forwarded.
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events.front().participant_identity, "operator-a");
  EXPECT_EQ(events.front().track_sid, "TR_b");
  EXPECT_EQ(events.front().track_name, "second");
  EXPECT_EQ(router.snapshot().size(), 2U);
}

TEST(RemoteTrackRouterTest, PublicationWithEmptySidFallsBackToHydratedRecords)
{
  RemoteTrackRouter router;

  const auto events = router.trackPublished(
    withPublication(makeRecord("operator-a", "")),
    hydrated({makeRecord("operator-a", "TR_a")}),
    livekit::ConnectionState::Connected);

  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events.front().track_sid, "TR_a");
}

TEST(RemoteTrackRouterTest, PublishedWhileNotConnectedMergesWithoutForwarding)
{
  for (const auto state : {livekit::ConnectionState::Reconnecting, livekit::ConnectionState::Disconnected}) {
    RemoteTrackRouter router;

    EXPECT_TRUE(router.trackPublished(withPublication(makeRecord("operator-a", "TR_a")), noHydration(), state).empty());
    EXPECT_TRUE(
      router.trackPublished(withoutPublication("operator-b"), hydrated({makeRecord("operator-b", "TR_b")}), state)
        .empty());

    EXPECT_TRUE(findEntry(router, "TR_a").has_value());
    EXPECT_TRUE(findEntry(router, "TR_b").has_value());
  }
}

TEST(RemoteTrackRouterTest, FullRestartReannouncesIntoSnapshotWithoutForwarding)
{
  RemoteTrackRouter router;
  const auto operator_track = makeRecord("operator-a", "TR_a");
  (void)router.trackPublished(withPublication(operator_track), noHydration(), livekit::ConnectionState::Connected);
  (void)router.trackSubscribed(withPublication(operator_track), livekit::ConnectionState::Connected);

  // A full restart unpublishes the track, then re-announces it while still Reconnecting.
  const auto unpublished =
    router.trackUnpublished(withPublication(operator_track), livekit::ConnectionState::Reconnecting);
  ASSERT_TRUE(unpublished.has_value());
  EXPECT_EQ(unpublished->track_sid, "TR_a");
  EXPECT_FALSE(findEntry(router, "TR_a").has_value());

  EXPECT_TRUE(router
                .trackPublished(
                  withoutPublication("operator-a"), hydrated({operator_track}), livekit::ConnectionState::Reconnecting)
                .empty());

  // The Connected snapshot is what re-subscribes: it must report the track, unsubscribed.
  const auto entry = findEntry(router, "TR_a");
  ASSERT_TRUE(entry.has_value());
  EXPECT_FALSE(entry->subscribed);

  // A later null-publication event for the same participant must not re-emit it as new.
  EXPECT_TRUE(
    router
      .trackPublished(withoutPublication("operator-a"), hydrated({operator_track}), livekit::ConnectionState::Connected)
      .empty());
}

TEST(RemoteTrackRouterTest, TrackEventsForwardInEveryStateButDisconnected)
{
  RemoteTrackRouter router;
  const auto record = makeRecord("operator-a", "TR_a");

  EXPECT_TRUE(router.trackSubscribed(withPublication(record), livekit::ConnectionState::Connected).has_value());
  EXPECT_TRUE(router.trackUnsubscribed(withPublication(record), livekit::ConnectionState::Reconnecting).has_value());
  EXPECT_TRUE(router.trackUnpublished(withPublication(record), livekit::ConnectionState::Reconnecting).has_value());

  EXPECT_FALSE(router.trackSubscribed(withPublication(record), livekit::ConnectionState::Disconnected).has_value());
  EXPECT_FALSE(router.trackUnsubscribed(withPublication(record), livekit::ConnectionState::Disconnected).has_value());
  EXPECT_FALSE(router.trackUnpublished(withPublication(record), livekit::ConnectionState::Disconnected).has_value());
}

TEST(RemoteTrackRouterTest, SubscriptionEventsUpdateMirrorInEveryState)
{
  RemoteTrackRouter router;
  const auto record = makeRecord("operator-a", "TR_a");

  // A subscribe for a publication the mirror has not seen yet adds it, already subscribed.
  EXPECT_FALSE(router.trackSubscribed(withPublication(record), livekit::ConnectionState::Disconnected).has_value());
  auto entry = findEntry(router, "TR_a");
  ASSERT_TRUE(entry.has_value());
  EXPECT_TRUE(entry->subscribed);

  EXPECT_FALSE(router.trackUnsubscribed(withPublication(record), livekit::ConnectionState::Disconnected).has_value());
  entry = findEntry(router, "TR_a");
  ASSERT_TRUE(entry.has_value());
  EXPECT_FALSE(entry->subscribed);

  EXPECT_FALSE(router.trackUnpublished(withPublication(record), livekit::ConnectionState::Disconnected).has_value());
  EXPECT_FALSE(findEntry(router, "TR_a").has_value());
}

TEST(RemoteTrackRouterTest, SubscribedTakesIdentityFromParticipant)
{
  RemoteTrackRouter router;
  auto input = withPublication(makeRecord("", "TR_a"));
  input.participant_identity = "operator-a";

  (void)router.trackSubscribed(input, livekit::ConnectionState::Connected);

  const auto entry = findEntry(router, "TR_a");
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->participant_identity, "operator-a");
}

TEST(RemoteTrackRouterTest, SubscribedWithoutParticipantForwardsButDoesNotMerge)
{
  RemoteTrackRouter router;
  RemoteTrackEventInput input;
  input.publication = makeRecord("", "TR_a");

  const auto forwarded = router.trackSubscribed(input, livekit::ConnectionState::Connected);

  ASSERT_TRUE(forwarded.has_value());
  EXPECT_EQ(forwarded->participant_identity, "");
  EXPECT_EQ(forwarded->track_sid, "TR_a");
  EXPECT_TRUE(router.snapshot().empty());
}

TEST(RemoteTrackRouterTest, TranslationFallsBackToTrackSidWithoutPublication)
{
  RemoteTrackRouter router;
  RemoteTrackEventInput input;
  input.participant_identity = "operator-a";
  input.track = RemoteTrackEventInput::MediaTrack{nullptr, "TR_media", livekit::TrackKind::KIND_AUDIO};

  const auto forwarded = router.trackUnsubscribed(input, livekit::ConnectionState::Connected);

  ASSERT_TRUE(forwarded.has_value());
  EXPECT_EQ(forwarded->participant_identity, "operator-a");
  EXPECT_EQ(forwarded->track_sid, "TR_media");
  EXPECT_EQ(forwarded->track_kind, livekit::TrackKind::KIND_AUDIO);
  EXPECT_EQ(forwarded->track_name, "");

  // The publication's sid wins whenever it has one.
  input.publication = makeRecord("operator-a", "TR_publication");
  const auto with_publication = router.trackUnsubscribed(input, livekit::ConnectionState::Connected);
  ASSERT_TRUE(with_publication.has_value());
  EXPECT_EQ(with_publication->track_sid, "TR_publication");
  EXPECT_EQ(with_publication->track_name, kOperatorTrack);
}

TEST(RemoteTrackRouterTest, ParticipantDisconnectErasesInEveryStateAndForwardsOnlyWhileConnected)
{
  RemoteTrackRouter router;
  const auto seed_both = [&router]() {
    router.beginSeed();
    router.completeSeed(
      {makeRecord("operator-a", "TR_a"), makeRecord("operator-a", "TR_b"), makeRecord("operator-b", "TR_c")});
  };

  seed_both();
  EXPECT_FALSE(router.participantDisconnected("operator-a", livekit::ConnectionState::Reconnecting));
  EXPECT_FALSE(findEntry(router, "TR_a").has_value());
  EXPECT_FALSE(findEntry(router, "TR_b").has_value());
  EXPECT_TRUE(findEntry(router, "TR_c").has_value());

  seed_both();
  EXPECT_FALSE(router.participantDisconnected("operator-a", livekit::ConnectionState::Disconnected));
  EXPECT_EQ(router.snapshot().size(), 1U);

  seed_both();
  EXPECT_TRUE(router.participantDisconnected("operator-a", livekit::ConnectionState::Connected));
  EXPECT_EQ(router.snapshot().size(), 1U);

  EXPECT_FALSE(router.participantDisconnected("", livekit::ConnectionState::Connected));
}

TEST(RemoteTrackRouterTest, SubscribeUnknownSidIsUnavailable)
{
  const RemoteTrackRouter router;

  const auto plan = router.planSetSubscribed("TR_missing", true);

  EXPECT_EQ(plan.action, SubscriptionAction::kUnavailable);
  EXPECT_EQ(plan.publication, nullptr);
}

TEST(RemoteTrackRouterTest, SubscribeAlreadySubscribedNeedsNoRequest)
{
  RemoteTrackRouter router;
  (void)router.trackSubscribed(withPublication(makeRecord("operator-a", "TR_a")), livekit::ConnectionState::Connected);

  // The fast path SdkRoomConnection::subscribeRemoteTrack returns true on, without an SDK call.
  const auto plan = router.planSetSubscribed("TR_a", true);

  EXPECT_EQ(plan.action, SubscriptionAction::kNone);
  EXPECT_EQ(plan.publication, nullptr);
}

TEST(RemoteTrackRouterTest, SubscribeWithoutHandleIsUnavailable)
{
  RemoteTrackRouter router;
  (void)router.trackPublished(
    withPublication(makeRecord("operator-a", "TR_a")), noHydration(), livekit::ConnectionState::Connected);

  EXPECT_EQ(router.planSetSubscribed("TR_a", true).action, SubscriptionAction::kUnavailable);
}

TEST(RemoteTrackRouterTest, UnsubscribeNeedsNoRequestUnlessSubscribed)
{
  RemoteTrackRouter router;
  (void)router.trackPublished(
    withPublication(makeRecord("operator-a", "TR_a")), noHydration(), livekit::ConnectionState::Connected);

  EXPECT_EQ(router.planSetSubscribed("TR_a", false).action, SubscriptionAction::kNone);
  EXPECT_EQ(router.planSetSubscribed("TR_missing", false).action, SubscriptionAction::kUnavailable);

  router.markSubscribed("TR_a", true);
  const auto entry = findEntry(router, "TR_a");
  ASSERT_TRUE(entry.has_value());
  EXPECT_TRUE(entry->subscribed);
  EXPECT_EQ(router.planSetSubscribed("TR_a", true).action, SubscriptionAction::kNone);
}

TEST(RemoteTrackRouterTest, SeedKeepsPublicationDeliveredBeforeIt)
{
  RemoteTrackRouter router;
  router.beginSeed();

  // Delivered after connect() returned but before activateRoom() completed the seed.
  (void)router.trackPublished(
    withoutPublication("operator-b"),
    hydrated({makeRecord("operator-b", "TR_b")}),
    livekit::ConnectionState::Disconnected);
  router.completeSeed({makeRecord("operator-a", "TR_a")});

  EXPECT_TRUE(findEntry(router, "TR_a").has_value());
  EXPECT_TRUE(findEntry(router, "TR_b").has_value());
}

TEST(RemoteTrackRouterTest, SeedDoesNotResurrectPublicationUnpublishedBeforeIt)
{
  RemoteTrackRouter router;
  router.beginSeed();

  // The seed read raced ahead of this unpublish's delivery, so it still lists TR_a.
  (void)router.trackUnpublished(
    withPublication(makeRecord("operator-a", "TR_a")), livekit::ConnectionState::Disconnected);
  router.completeSeed({makeRecord("operator-a", "TR_a"), makeRecord("operator-a", "TR_b")});

  EXPECT_FALSE(findEntry(router, "TR_a").has_value());
  EXPECT_TRUE(findEntry(router, "TR_b").has_value());
}

TEST(RemoteTrackRouterTest, SeedDoesNotResurrectParticipantThatLeftBeforeIt)
{
  RemoteTrackRouter router;
  router.beginSeed();

  EXPECT_FALSE(router.participantDisconnected("operator-a", livekit::ConnectionState::Disconnected));
  router.completeSeed({makeRecord("operator-a", "TR_a"), makeRecord("operator-b", "TR_b")});

  EXPECT_FALSE(findEntry(router, "TR_a").has_value());
  EXPECT_TRUE(findEntry(router, "TR_b").has_value());
}

TEST(RemoteTrackRouterTest, SeedKeepsSubscriptionRecordedBeforeIt)
{
  RemoteTrackRouter router;
  router.beginSeed();

  (void)router.trackPublished(
    withPublication(makeRecord("operator-a", "TR_a")), noHydration(), livekit::ConnectionState::Connected);
  router.markSubscribed("TR_a", true);
  // The seed read the SDK's flag before the subscribe request completed.
  router.completeSeed({makeRecord("operator-a", "TR_a", kOperatorTrack, false)});

  const auto entry = findEntry(router, "TR_a");
  ASSERT_TRUE(entry.has_value());
  EXPECT_TRUE(entry->subscribed);
}

TEST(RemoteTrackRouterTest, ChangesAfterSeedApplyOnce)
{
  RemoteTrackRouter router;
  router.beginSeed();
  router.completeSeed({makeRecord("operator-a", "TR_a")});

  (void)router.trackUnpublished(withPublication(makeRecord("operator-a", "TR_a")), livekit::ConnectionState::Connected);
  EXPECT_TRUE(router.snapshot().empty());

  // A later seed must not replay changes logged before the previous one completed.
  router.beginSeed();
  router.completeSeed({makeRecord("operator-a", "TR_a")});
  EXPECT_TRUE(findEntry(router, "TR_a").has_value());
}

TEST(RemoteTrackRouterTest, ClearDropsMirrorAndPendingSeed)
{
  RemoteTrackRouter router;
  router.beginSeed();
  (void)router.trackUnpublished(withPublication(makeRecord("operator-a", "TR_a")), livekit::ConnectionState::Connected);
  router.clear();
  EXPECT_TRUE(router.snapshot().empty());

  // The unpublish logged before clear() is gone, so it cannot hide the next room's seed.
  router.beginSeed();
  router.completeSeed({makeRecord("operator-a", "TR_a")});
  EXPECT_TRUE(findEntry(router, "TR_a").has_value());
}

}  // namespace
}  // namespace livekit_ros2_bridge
