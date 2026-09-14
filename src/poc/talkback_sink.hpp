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

// TALKBACK POC (Layer B) — THROWAWAY CODE, DO NOT MERGE.
// Proves the bridge can play received PCM out a real audio device through a
// bridge-owned GStreamer appsrc sink pipeline. Feeds the results section of
// operator-talkback-design.md §7; delete this file (and its wiring) once the
// POC matrix is recorded.

#pragma once

#include <gst/app/gstappsrc.h>
#include <gst/gst.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>

#include "rclcpp/logger.hpp"
#include "utils/gstreamer_resources.hpp"
#include "utils/pipeline_failure_handler.hpp"

namespace livekit_ros2_bridge
{

// Plays received operator PCM through appsrc → queue (leaky) → audioconvert →
// audioresample → pulsesink. The pipeline is created lazily on the first bound
// frame because appsrc caps come from that frame's actual rate/channels.
//
// Ownership: created once by TalkbackPoc and captured BY COPY in each reader
// thread (refcounted; never captures TalkbackPoc `this`). The sink may outlive
// TalkbackPoc; ~TalkbackSink stops the pipeline and closes the failure handler.
//
// Concurrency: frames arrive on per-track reader threads, failures arrive on
// GStreamer bus threads. A single reader owns the sink at a time (atomic CAS on
// reader_id, design doc §5.6); frames from other readers are logged once and
// dropped.
class TalkbackSink
{
public:
  TalkbackSink();
  ~TalkbackSink();

  TalkbackSink(const TalkbackSink &) = delete;
  TalkbackSink & operator=(const TalkbackSink &) = delete;
  TalkbackSink(TalkbackSink &&) = delete;
  TalkbackSink & operator=(TalkbackSink &&) = delete;

  // Claims the sink for this reader (first caller wins) and, on success,
  // lazily starts the playback pipeline with caps built from this frame. Only
  // the owning reader's first frame ever starts the pipeline. Returns true
  // when this reader owns the sink.
  bool onFirstFrame(std::uint64_t reader_id, const std::string & track_sid, int sample_rate, int num_channels);

  // Pushes one interleaved S16 frame. Non-owner frames are logged once
  // (debug) and dropped. Push failures are logged and dropped — never tear
  // down (queue is leaky, appsrc is block=false).
  void push(std::uint64_t reader_id, const std::int16_t * samples, std::size_t count);

  // Releases the claim on reader finalize so a post-reconnect reader can
  // claim cleanly. Logs only when this reader actually owned the sink.
  void unbind(std::uint64_t reader_id);

  // Stops the pipeline and disables restarts. Idempotent; called from
  // ~TalkbackPoc for deterministic playback teardown and from ~TalkbackSink.
  void stop();

private:
  void startPipelineLocked();
  void stopPipelineLocked();
  void restartPipeline();
  void onBusMessage(GstMessage * message);
  void logIgnoredOnce(std::uint64_t reader_id);

  // Guards pipeline_/appsrc_/caps state. The failure path (onBusMessage →
  // schedule) must stay lock-free against this mutex: the sync bus handler can
  // fire from inside startPipelineLocked() while a caller holds it.
  std::mutex mutex_;
  utils::GstElementPtr pipeline_;
  utils::GstObjectPtr<GstAppSrc> appsrc_;
  int caps_rate_ = 0;
  int caps_channels_ = 0;

  // Samples already pushed onto the current pipeline instance; the running
  // source of buffer PTS/DURATION (audio clock, not wall clock). Reset in
  // startPipelineLocked() so a restarted pipeline sees timestamps from 0.
  std::uint64_t samples_pushed_ = 0;

  // Lock-free mirror of pipeline_ != nullptr so push() can re-arm the restart
  // loop without touching mutex_: a restart attempt that fails swallows its
  // own bus error (schedule() refuses callbacks while callback_running_), and
  // a dead pipeline emits no further messages — the 10 ms push cadence is
  // what keeps the ~4 restarts/sec loop alive (Layer B PRD story 5).
  std::atomic<bool> pipeline_active_{false};

  // 0 = unclaimed; otherwise the owning reader_id. Claim/release only via CAS.
  std::atomic<std::uint64_t> owner_{0};
  std::atomic<bool> is_shutdown_{false};

  std::mutex ignored_mutex_;
  std::set<std::uint64_t> ignored_logged_;

  utils::PipelineFailureHandler failure_handler_;
};

}  // namespace livekit_ros2_bridge
