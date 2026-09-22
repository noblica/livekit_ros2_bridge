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

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "room_connection/remote_publication_mirror.hpp"

namespace livekit_ros2_bridge
{
namespace
{

// The mirror is deliberately free of SDK calls: every test passes null publication handles so it can
// run without a live room. SdkRoomConnection wires the real handles in.

RoomConnection::RemoteTrackSnapshotEntry makeEntry(
  const std::string & participant_identity,
  const std::string & track_sid,
  const std::string & track_name,
  livekit::TrackKind track_kind = livekit::TrackKind::KIND_AUDIO)
{
  RoomConnection::RemoteTrackSnapshotEntry entry;
  entry.participant_identity = participant_identity;
  entry.track_sid = track_sid;
  entry.track_name = track_name;
  entry.track_kind = track_kind;
  entry.subscribed = false;
  return entry;
}

TEST(RemotePublicationMirrorTest, MergeReturnsOnlyNewEntries)
{
  RemotePublicationMirror mirror;
  const std::vector<RoomConnection::RemoteTrackSnapshotEntry> candidates{
    makeEntry("operator-a", "TR_a", "lkros.audio.operator"), makeEntry("operator-b", "TR_b", "lkros.audio.operator")};

  const auto first_added = mirror.merge(candidates);
  ASSERT_EQ(first_added.size(), 2U);

  const auto second_added = mirror.merge(candidates);
  EXPECT_TRUE(second_added.empty());

  const std::vector<RoomConnection::RemoteTrackSnapshotEntry> mixed{
    makeEntry("operator-a", "TR_a", "lkros.audio.operator"), makeEntry("operator-b", "TR_c", "lkros.audio.operator")};
  const auto third_added = mirror.merge(mixed);
  ASSERT_EQ(third_added.size(), 1U);
  EXPECT_EQ(third_added.front().track_sid, "TR_c");
  EXPECT_EQ(mirror.size(), 3U);
}

TEST(RemotePublicationMirrorTest, MergeRecordListReturnsOnlyNewEntries)
{
  RemotePublicationMirror mirror;
  const std::vector<RemotePublicationMirror::Record> candidates{
    RemotePublicationMirror::Record{makeEntry("operator-a", "TR_a", "lkros.audio.operator"), nullptr},
    RemotePublicationMirror::Record{makeEntry("operator-b", "TR_b", "lkros.audio.operator"), nullptr}};

  const auto first_added = mirror.merge(candidates);
  EXPECT_EQ(first_added.size(), 2U);

  const auto second_added = mirror.merge(candidates);
  EXPECT_TRUE(second_added.empty());
}

TEST(RemotePublicationMirrorTest, MergeUpdatesExistingFields)
{
  RemotePublicationMirror mirror;
  const auto first_added = mirror.merge(makeEntry("operator-a", "TR_a", "old-name", livekit::TrackKind::KIND_AUDIO));
  ASSERT_EQ(first_added.size(), 1U);

  const auto second_added = mirror.merge(makeEntry("operator-a", "TR_a", "new-name", livekit::TrackKind::KIND_VIDEO));
  EXPECT_TRUE(second_added.empty());

  const auto record = mirror.find("TR_a");
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ(record->entry.track_name, "new-name");
  EXPECT_EQ(record->entry.track_kind, livekit::TrackKind::KIND_VIDEO);

  const auto snapshot = mirror.snapshot();
  ASSERT_EQ(snapshot.size(), 1U);
  EXPECT_EQ(snapshot.front().track_name, "new-name");
  EXPECT_EQ(snapshot.front().track_kind, livekit::TrackKind::KIND_VIDEO);
}

TEST(RemotePublicationMirrorTest, EraseAndEraseIdentity)
{
  RemotePublicationMirror mirror;
  (void)mirror.merge(
    std::vector<RoomConnection::RemoteTrackSnapshotEntry>{
      makeEntry("operator-a", "TR_a", "a"),
      makeEntry("operator-a", "TR_b", "b"),
      makeEntry("operator-b", "TR_c", "c")});

  mirror.erase("TR_a");
  EXPECT_FALSE(mirror.contains("TR_a"));
  EXPECT_TRUE(mirror.contains("TR_b"));

  mirror.eraseIdentity("operator-a");
  EXPECT_FALSE(mirror.contains("TR_b"));
  EXPECT_TRUE(mirror.contains("TR_c"));
  EXPECT_EQ(mirror.size(), 1U);
}

TEST(RemotePublicationMirrorTest, SetSubscribedAndClear)
{
  RemotePublicationMirror mirror;
  (void)mirror.merge(makeEntry("operator-a", "TR_a", "a"));

  ASSERT_EQ(mirror.snapshot().size(), 1U);
  EXPECT_FALSE(mirror.snapshot().front().subscribed);
  EXPECT_FALSE(mirror.find("TR_a")->handle);

  mirror.setSubscribed("TR_a", true);
  const auto record = mirror.find("TR_a");
  ASSERT_TRUE(record.has_value());
  EXPECT_TRUE(record->entry.subscribed);

  // An unknown sid is a silent no-op.
  mirror.setSubscribed("TR_missing", true);

  mirror.clear();
  EXPECT_EQ(mirror.size(), 0U);
  EXPECT_TRUE(mirror.snapshot().empty());
}

}  // namespace
}  // namespace livekit_ros2_bridge
