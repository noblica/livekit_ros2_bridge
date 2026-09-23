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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "livekit/room.h"
#include "protocol/constants.hpp"
#include "room_connection.hpp"

namespace livekit_ros2_bridge
{
namespace
{

// The detached-thread body of sendByteStream (construct/write/close against the LiveKit SDK) needs a
// live room and is exercised by integration, like the rest of SdkRoomConnection. These tests cover
// the synchronous guards that run before any thread is spawned, on a connection that was never
// started: payload validation and the local-participant precondition. The latter is the exact
// synchronous throw SubscriptionLeaseManager::dispatchEchoOnce catches to report "none".

std::shared_ptr<const std::vector<std::uint8_t>> makeCdr()
{
  return std::make_shared<const std::vector<std::uint8_t>>(std::vector<std::uint8_t>{0x01, 0x02, 0x03});
}

TEST(RoomConnectionSendByteStreamTest, NullPayloadThrowsInvalidArgument)
{
  const auto connection = createRoomConnection();
  EXPECT_THROW(
    connection->sendByteStream(protocol::kEchoOnceTopic, "/map", protocol::kCdrContentType, nullptr, "participant-1"),
    std::invalid_argument);
}

TEST(RoomConnectionSendByteStreamTest, UnavailableLocalParticipantThrowsRuntimeError)
{
  // Never started, so there is no local participant; the send must throw synchronously rather than
  // spawn a sender thread.
  const auto connection = createRoomConnection();
  EXPECT_THROW(
    connection->sendByteStream(protocol::kEchoOnceTopic, "/map", protocol::kCdrContentType, makeCdr(), "participant-1"),
    std::runtime_error);
}

// snapshotRemotePublications() relies on unregisterTextStreamHandler() destroying the handler under
// Room::lock_. The SDK does not promise that, so this fails if an SDK bump changes it.
struct LockProbeResult
{
  bool inside_unregister = false;
  bool destroyed_inside_unregister = false;
  bool room_lock_held = false;
  std::atomic<bool> contender_finished{false};
  std::thread contender;
};

// On destruction, checks that a second thread's locked Room call stays blocked.
class LockHeldProbe
{
public:
  LockHeldProbe(livekit::Room & room, LockProbeResult & result)
  : room_(room)
  , result_(result)
  , owner_(std::this_thread::get_id())
  {}

  LockHeldProbe(const LockHeldProbe &) = delete;
  LockHeldProbe & operator=(const LockHeldProbe &) = delete;

  ~LockHeldProbe()
  {
    if (!result_.inside_unregister || std::this_thread::get_id() != owner_) {
      return;
    }
    result_.destroyed_inside_unregister = true;

    auto & room = room_;
    auto & contender_finished = result_.contender_finished;
    result_.contender = std::thread([&room, &contender_finished]() {
      (void)room.roomInfo();  // Takes Room::lock_.
      contender_finished.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    result_.room_lock_held = !contender_finished.load();
  }

private:
  livekit::Room & room_;
  LockProbeResult & result_;
  std::thread::id owner_;
};

TEST(RoomConnectionSdkContractTest, UnregisteringATextStreamHandlerDestroysItUnderTheRoomLock)
{
  // Declared before the room so the contender can be joined before the room goes away.
  LockProbeResult result;
  livekit::Room room;

  auto probe = std::make_shared<LockHeldProbe>(room, result);
  room.registerTextStreamHandler(
    "lkros.test.lock_probe",
    [probe = std::move(probe)](std::shared_ptr<livekit::TextStreamReader>, const std::string &) { (void)probe; });

  result.inside_unregister = true;
  room.unregisterTextStreamHandler("lkros.test.lock_probe");
  result.inside_unregister = false;

  if (result.contender.joinable()) {
    result.contender.join();
  }

  EXPECT_TRUE(result.destroyed_inside_unregister);
  EXPECT_TRUE(result.room_lock_held);
  EXPECT_TRUE(result.contender_finished.load());
}

}  // namespace
}  // namespace livekit_ros2_bridge
