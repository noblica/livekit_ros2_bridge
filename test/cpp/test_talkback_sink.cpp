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

// Tests assert external behavior: which reader claimed the sink, that lifecycle
// paths leave the sink rebindable, and that the restart loop stays rate-bounded
// and owner-gated. The pure timing helper is covered directly; buffer timestamps
// as delivered to GStreamer are a POC-verified property covered by the real-path
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

TEST_F(TalkbackSinkTest, FailedInitialStartKeepsTheClaimForRetry)
{
  // An empty fragment makes startPipelineLocked() throw deterministically. The
  // reader must still own the sink afterwards so its live frame cadence keeps
  // re-arming the restart loop and playback recovers when the device returns.
  TalkbackSink sink("");

  EXPECT_TRUE(sink.bind(1, 48000, 1));
  EXPECT_FALSE(sink.bind(2, 48000, 1));
  EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));

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

TEST_F(TalkbackSinkTest, UnbindStopsThePipeline)
{
  TalkbackSink sink(kTestSinkFragment);

  ASSERT_TRUE(sink.bind(1, 48000, 1));
  EXPECT_TRUE(sink.hasActivePipeline());

  // Reader finalize must release the audio.sink device, not just the claim.
  sink.unbind(1);
  EXPECT_FALSE(sink.hasActivePipeline());
}

TEST_F(TalkbackSinkTest, HandoverStartsAFreshPipelineWithNewCaps)
{
  TalkbackSink sink(kTestSinkFragment);

  ASSERT_TRUE(sink.bind(1, 48000, 1));
  const std::size_t attempts_after_first_bind = sink.pipelineStartAttempts();
  EXPECT_TRUE(sink.hasActivePipeline());
  EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));

  sink.unbind(1);
  EXPECT_FALSE(sink.hasActivePipeline());

  // The next operator track claims on its first frame; a fresh pipeline is built
  // with its own rate/channels, and the old pipeline is gone before it starts.
  ASSERT_TRUE(sink.bind(2, 44100, 2));
  EXPECT_EQ(sink.pipelineStartAttempts(), attempts_after_first_bind + 1);
  EXPECT_TRUE(sink.hasActivePipeline());
  EXPECT_NO_THROW(sink.push(2, makeSamples(44100 * 2).data(), 44100 * 2));
}

TEST_F(TalkbackSinkTest, IdleSinkDoesNotRestartWhilePipelineIsDown)
{
  // Empty fragment makes the initial start throw, so the sink is claimed but has
  // no pipeline. With no frames arriving, the rate-bounded loop must not cycle.
  TalkbackSink sink("");

  ASSERT_TRUE(sink.bind(1, 48000, 1));
  const std::size_t attempts_after_bind = sink.pipelineStartAttempts();

  std::this_thread::sleep_for(std::chrono::milliseconds(700));
  EXPECT_EQ(sink.pipelineStartAttempts(), attempts_after_bind);
}

TEST_F(TalkbackSinkTest, LiveFramesReArmBoundedRestarts)
{
  // With a dead device, each live frame re-arms the restart loop (bounded by the
  // 250 ms delay). Over 700 ms that is at least the initial bind plus a retry.
  TalkbackSink sink("");

  ASSERT_TRUE(sink.bind(1, 48000, 1));

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(700);
  while (std::chrono::steady_clock::now() < deadline) {
    EXPECT_NO_THROW(sink.push(1, makeSamples(480).data(), 480));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_GE(sink.pipelineStartAttempts(), 2U);
}

TEST_F(TalkbackSinkTest, NoRestartAfterUnbind)
{
  TalkbackSink sink("");

  ASSERT_TRUE(sink.bind(1, 48000, 1));
  for (int frame_index = 0; frame_index < 5; ++frame_index) {
    sink.push(1, makeSamples(480).data(), 480);
  }

  sink.unbind(1);
  const std::size_t attempts_after_unbind = sink.pipelineStartAttempts();

  // Frames from the released reader are ignored, and no pending/queued restart
  // may reopen the device after the claim is gone.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(700);
  while (std::chrono::steady_clock::now() < deadline) {
    sink.push(1, makeSamples(480).data(), 480);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(sink.pipelineStartAttempts(), attempts_after_unbind);
}

TEST_F(TalkbackSinkTest, TimingHelperIsCorrectForMonoAndStereo)
{
  // 480 frames at 48 kHz is 10 ms regardless of channel count; the interleaved
  // sample count only scales the byte size, not the duration.
  const TalkbackBufferTiming mono = computeTalkbackBufferTiming(480, 1, 48000, 0);
  EXPECT_EQ(mono.pts, 0U);
  EXPECT_EQ(mono.duration, 10'000'000U);

  const TalkbackBufferTiming stereo = computeTalkbackBufferTiming(960, 2, 48000, 0);
  EXPECT_EQ(stereo.duration, 10'000'000U);

  // A running PTS advances by exactly the previous buffer's duration.
  const TalkbackBufferTiming second = computeTalkbackBufferTiming(480, 1, 48000, mono.pts + mono.duration);
  EXPECT_EQ(second.pts, 10'000'000U);
}

}  // namespace
}  // namespace livekit_ros2_bridge::audio
