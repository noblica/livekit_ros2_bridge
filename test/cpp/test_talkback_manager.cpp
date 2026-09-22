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

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "audio/talkback_manager.hpp"
#include "fake_room_connection.hpp"
#include "gtest/gtest.h"
#include "livekit/participant.h"
#include "livekit/remote_participant.h"
#include "livekit/track.h"
#include "protocol/constants.hpp"
#include "ros_test_support.hpp"

namespace livekit_ros2_bridge::audio
{
namespace
{

// Tests assert external behavior only — track events in, subscribe calls and
// reader lifecycle out — never pipeline internals. The reader threads bind
// against a real GStreamer pipeline only when frames arrive; these tests drive
// events, so no audio device is needed. The manager is a plain object: tests
// call its handlers directly, the way Runtime's callback wiring does.

constexpr char kOperatorTrackName[] = "lkros.audio.operator";
constexpr char kTestSinkFragment[] = "fakesink sync=false";

livekit::ParticipantDisconnectedEvent makeDisconnectedEvent(const std::string & identity)
{
  static livekit::RemoteParticipant participant(
    livekit::FfiHandle{},
    "fake-participant-sid",
    "fake-participant-name",
    identity,
    "",
    std::unordered_map<std::string, std::string>{},
    livekit::ParticipantKind::Standard,
    livekit::DisconnectReason::Unknown);
  livekit::ParticipantDisconnectedEvent event;
  event.participant = &participant;
  return event;
}

class TalkbackManagerTest : public ::testing::Test
{
protected:
  static void SetUpTestSuite()
  {
    static test_support::ScopedRclcppInit rclcpp_init;
  }
};

TEST_F(TalkbackManagerTest, SubscribesToOperatorTrackOnPublish)
{
  FakeRoomConnection connection;
  TalkbackManager manager(connection, kTestSinkFragment);

  manager.onRemoteTrackPublished(
    RemoteTrackEvent{"participant-1", "PA_track1", kOperatorTrackName, livekit::TrackKind::KIND_AUDIO, nullptr});

  ASSERT_EQ(connection.state->subscribe_remote_track_calls.size(), 1U);
  EXPECT_EQ(connection.state->subscribe_remote_track_calls.front().first, "participant-1");
  EXPECT_EQ(connection.state->subscribe_remote_track_calls.front().second, "PA_track1");
}

TEST_F(TalkbackManagerTest, IgnoresNonOperatorTrackPublishes)
{
  FakeRoomConnection connection;
  TalkbackManager manager(connection, kTestSinkFragment);

  manager.onRemoteTrackPublished(
    RemoteTrackEvent{
      "participant-1", "PA_track1", "lkros.audio.other.cab_mic", livekit::TrackKind::KIND_AUDIO, nullptr});
  manager.onRemoteTrackPublished(
    RemoteTrackEvent{"participant-1", "PA_track2", "some_video_feed", livekit::TrackKind::KIND_VIDEO, nullptr});

  EXPECT_TRUE(connection.state->subscribe_remote_track_calls.empty());
}

TEST_F(TalkbackManagerTest, SubscribeFailureIsLoggedNotThrown)
{
  FakeRoomConnection connection;
  connection.setRemoteTrackSubscribable("participant-1", "PA_track1", false);
  TalkbackManager manager(connection, kTestSinkFragment);

  EXPECT_NO_THROW(manager.onRemoteTrackPublished(
    RemoteTrackEvent{"participant-1", "PA_track1", kOperatorTrackName, livekit::TrackKind::KIND_AUDIO, nullptr}));
  EXPECT_EQ(connection.state->subscribe_remote_track_calls.size(), 1U);
}

TEST_F(TalkbackManagerTest, CleanupPathsStopReaders)
{
  FakeRoomConnection connection;
  {
    TalkbackManager manager(connection, kTestSinkFragment);
    auto track = connection.makeSyntheticRemoteTrack();
    // No frames arrive in this test, so no reader thread outlives the manager.
    manager.onRemoteTrackSubscribed(
      RemoteTrackEvent{"participant-1", track->sid(), kOperatorTrackName, livekit::TrackKind::KIND_AUDIO, track});

    // Reader registered for the operator track; unpublish tears it down.
    manager.onRemoteTrackUnpublished(
      RemoteTrackEvent{"participant-1", track->sid(), kOperatorTrackName, livekit::TrackKind::KIND_AUDIO, nullptr});

    // All cleanup paths accept events after the corresponding reader is gone.
    manager.onRemoteTrackUnsubscribed(
      RemoteTrackEvent{"participant-1", track->sid(), kOperatorTrackName, livekit::TrackKind::KIND_AUDIO, track});
    connection.emitParticipantDisconnected("participant-1");
  }
  SUCCEED();
}

TEST_F(TalkbackManagerTest, ParticipantDisconnectIsSafeWithoutReaders)
{
  FakeRoomConnection connection;
  TalkbackManager manager(connection, kTestSinkFragment);

  EXPECT_NO_THROW(connection.emitParticipantDisconnected("participant-1"));
  EXPECT_NO_THROW(manager.onParticipantDisconnected(makeDisconnectedEvent("participant-1")));
}

TEST_F(TalkbackManagerTest, GenerationChangeResubscribesFromSnapshot)
{
  FakeRoomConnection connection;
  TalkbackManager manager(connection, kTestSinkFragment);

  connection.setRemoteTrackSnapshot(
    {RoomConnection::RemoteTrackSnapshotEntry{
       "participant-1", "PA_track1", kOperatorTrackName, livekit::TrackKind::KIND_AUDIO, false},
     RoomConnection::RemoteTrackSnapshotEntry{
       "participant-2", "PA_track2", "some_video_feed", livekit::TrackKind::KIND_VIDEO, false}});

  manager.onRoomGenerationChanged();

  ASSERT_EQ(connection.state->subscribe_remote_track_calls.size(), 1U);
  EXPECT_EQ(connection.state->subscribe_remote_track_calls.front().first, "participant-1");
  EXPECT_EQ(connection.state->subscribe_remote_track_calls.front().second, "PA_track1");
}

TEST_F(TalkbackManagerTest, GenerationChangeSkipsAlreadySubscribedOperatorTrack)
{
  FakeRoomConnection connection;
  TalkbackManager manager(connection, kTestSinkFragment);

  connection.setRemoteTrackSnapshot({RoomConnection::RemoteTrackSnapshotEntry{
    "participant-1", "PA_track1", kOperatorTrackName, livekit::TrackKind::KIND_AUDIO, true}});

  manager.onRoomGenerationChanged();

  EXPECT_TRUE(connection.state->subscribe_remote_track_calls.empty());
}

TEST_F(TalkbackManagerTest, ActiveGateFollowsLifetime)
{
  FakeRoomConnection connection;
  auto manager = std::make_unique<TalkbackManager>(connection, kTestSinkFragment);

  EXPECT_TRUE(manager->isActive());
  manager->onRoomGenerationChanged();
  manager.reset();
}

TEST_F(TalkbackManagerTest, NonOperatorSubscribedTrackIsIgnored)
{
  FakeRoomConnection connection;
  TalkbackManager manager(connection, kTestSinkFragment);

  auto track = connection.makeSyntheticRemoteTrack();
  manager.onRemoteTrackSubscribed(
    RemoteTrackEvent{
      "participant-1", track->sid(), "lkros.audio.other.cab_mic", livekit::TrackKind::KIND_AUDIO, track});

  // No reader work observable; the manager simply does not track foreign names.
  manager.onRemoteTrackUnpublished(
    RemoteTrackEvent{
      "participant-1", track->sid(), "lkros.audio.other.cab_mic", livekit::TrackKind::KIND_AUDIO, nullptr});
}

}  // namespace
}  // namespace livekit_ros2_bridge::audio
