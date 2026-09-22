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
#include <string>
#include <thread>
#include <vector>

#include "audio/talkback_sink.hpp"
#include "gtest/gtest.h"
#include "ros_test_support.hpp"
#include "utils/gstreamer_resources.hpp"

namespace livekit_ros2_bridge::audio
{
namespace
{

// Tests assert external behavior only: which reader claimed the sink, and that
// lifecycle paths leave the sink rebindable. Pipeline internals (appsrc caps,
// buffer timestamps) are POC-verified properties covered by the real-path
// integration, not this suite.

class TalkbackSinkTest : public ::testing::Test
{
protected:
  static void SetUpTestSuite()
  {
    utils::ensureGStreamerInitialized();
    static test_support::ScopedRclcppInit rclcpp_init;
  }
};

std::vector<std::int16_t> makeSamples(std::size_t count)
{
  return std::vector<std::int16_t>(count, 0);
}

// The fragment names a real sink element so pipeline start succeeds in CI.
constexpr char kTestSinkFragment[] = "fakesink sync=false";

TEST_F(TalkbackSinkTest, FirstReaderClaimsAndBindReportsOwnership)
{
  TalkbackSink sink(kTestSinkFragment);

  EXPECT_TRUE(sink.bind(1, 48000, 1));
  EXPECT_FALSE(sink.bind(2, 48000, 1));

  sink.unbind(1);
  EXPECT_TRUE(sink.bind(2, 48000, 1));
}

TEST_F(TalkbackSinkTest, PushFromNonOwnerIsDroppedWithoutEffect)
{
  TalkbackSink sink(kTestSinkFragment);

  EXPECT_TRUE(sink.bind(1, 48000, 1));

  // A second live operator track's frames are dropped, never played.
  EXPECT_NO_THROW(sink.push(2, makeSamples(480).data(), 480));
  EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));

  sink.unbind(1);
}

TEST_F(TalkbackSinkTest, UnbindReleasesClaimForNextOperator)
{
  TalkbackSink sink(kTestSinkFragment);

  EXPECT_TRUE(sink.bind(1, 48000, 1));
  sink.push(1, makeSamples(480).data(), 480);
  sink.unbind(1);

  // Lease-handover rebind: the next operator track claims on its first frame.
  EXPECT_TRUE(sink.bind(2, 48000, 1));
  EXPECT_NO_THROW(sink.push(2, makeSamples(480).data(), 480));
}

TEST_F(TalkbackSinkTest, UnbindFromNonOwnerIsANoOp)
{
  TalkbackSink sink(kTestSinkFragment);

  EXPECT_TRUE(sink.bind(1, 48000, 1));
  sink.unbind(2);
  EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));
  EXPECT_FALSE(sink.bind(2, 48000, 1));
}

TEST_F(TalkbackSinkTest, StopDisablesBindAndKeepsPushSafe)
{
  TalkbackSink sink(kTestSinkFragment);

  sink.stop();
  EXPECT_FALSE(sink.bind(1, 48000, 1));
  EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));
  sink.stop();
}

TEST_F(TalkbackSinkTest, PushWithoutBindIsDropped)
{
  TalkbackSink sink(kTestSinkFragment);

  EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));
}

TEST_F(TalkbackSinkTest, SilenceThroughDoesNotDisturbTheClaim)
{
  TalkbackSink sink(kTestSinkFragment);

  EXPECT_TRUE(sink.bind(1, 48000, 1));
  // Mute arrives as continuous silent frames; the sink stays claimed and the
  // bridge needs no mute reaction.
  for (int frame_index = 0; frame_index < 10; ++frame_index) {
    EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));
  }
  EXPECT_FALSE(sink.bind(2, 48000, 1));
}

}  // namespace
}  // namespace livekit_ros2_bridge::audio
