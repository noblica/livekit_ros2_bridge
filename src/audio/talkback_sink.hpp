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

#pragma once

#include <gst/gst.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>

#include "utils/gstreamer_resources.hpp"
#include "utils/pipeline_failure_handler.hpp"

// The appsrc element is held as a plain GstElementPtr; appsrc call sites cast
// with GST_APP_SRC, so this header does not force gst-app includes on all
// transitive consumers.

namespace livekit_ros2_bridge::audio
{

// Plays received Talkback PCM through appsrc → queue (leaky) → audioconvert →
// audioresample → the configured sink fragment. The pipeline is created lazily
// because appsrc caps come from the first frame's actual rate/channels.
//
// Ownership: refcounted; each TalkbackManager reader thread captures it by
// copy, so the sink may outlive a single reader. ~TalkbackSink stops the
// pipeline and closes the failure handler.
//
// Concurrency: frames arrive on per-track reader threads, failures arrive on
// GStreamer bus threads. A single reader owns the sink at a time (atomic CAS on
// a reader id); frames from other readers are logged once and dropped, so a
// second live operator track can never steal the speaker from the active one.
class TalkbackSink
{
public:
  explicit TalkbackSink(std::string sink_fragment);
  ~TalkbackSink();

  TalkbackSink(const TalkbackSink &) = delete;
  TalkbackSink & operator=(const TalkbackSink &) = delete;
  TalkbackSink(TalkbackSink &&) = delete;
  TalkbackSink & operator=(TalkbackSink &&) = delete;

  // Claims the sink for this reader (first caller wins) and lazily starts the
  // playback pipeline with caps built from this frame. Only the owning reader's
  // first frame ever starts the pipeline. Returns true when this reader owns the
  // sink; a failed initial pipeline start still reports the claim, because the
  // owning reader's live frame cadence drives the restart loop until the device
  // returns.
  bool bind(std::uint64_t reader_id, int sample_rate, int num_channels);

  // Pushes one interleaved S16 frame. Non-owner frames are logged once and
  // dropped. Push failures are logged and dropped — never tear down (queue is
  // leaky, appsrc is block=false). While the pipeline is down, the caller's
  // live frame cadence re-arms the rate-bounded restart loop; nothing restarts
  // while no frames arrive.
  void push(std::uint64_t reader_id, const std::int16_t * samples, std::size_t count);

  // Releases the claim on reader finalize so the next operator track can claim
  // on its first frame (lease-handover rebind, with no bridge-side identity
  // knowledge). No-op when this reader did not own the sink.
  void unbind(std::uint64_t reader_id);

  // Stops the pipeline and disables restarts. Idempotent.
  void stop();

private:
  void startPipelineLocked();
  void stopPipelineLocked();
  void restartPipeline();
  void onBusMessage(GstMessage * message);
  void logIgnoredOnce(std::uint64_t reader_id);

  std::string sink_fragment_;

  // Guards pipeline_/appsrc_/caps state. The failure path (onBusMessage →
  // schedule) must stay lock-free against this mutex: the sync bus handler can
  // fire from inside startPipelineLocked() while a caller holds it.
  std::mutex mutex_;
  utils::GstElementPtr pipeline_;
  utils::GstElementPtr appsrc_element_;
  int caps_rate_ = 0;
  int caps_channels_ = 0;

  // Samples already pushed onto the current pipeline instance; the running
  // source of buffer PTS/DURATION (audio clock, not wall clock). Reset in
  // startPipelineLocked() so a restarted pipeline sees timestamps from 0.
  std::uint64_t samples_pushed_ = 0;

  // Lock-free mirror of pipeline_ != nullptr so push() can re-arm the restart
  // loop without touching mutex_: a restart attempt that fails swallows its
  // own bus error (schedule() refuses callbacks while callback_running_), and
  // a dead pipeline emits no further messages — the 10 ms push cadence is what
  // keeps the ~4 restarts/sec loop alive.
  std::atomic<bool> pipeline_active_{false};

  // 0 = unclaimed; otherwise the owning reader_id. Claim/release only via CAS.
  std::atomic<std::uint64_t> owner_{0};
  std::atomic<bool> is_shutdown_{false};

  std::mutex ignored_mutex_;
  std::set<std::uint64_t> ignored_logged_;

  utils::PipelineFailureHandler failure_handler_;
};

}  // namespace livekit_ros2_bridge::audio
