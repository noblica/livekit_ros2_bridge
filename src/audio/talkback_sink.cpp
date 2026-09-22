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

#include "audio/talkback_sink.hpp"

#include <gst/app/gstappsrc.h>

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "utils/log_event.hpp"

namespace livekit_ros2_bridge::audio
{

namespace
{

const auto kLogger = rclcpp::get_logger("livekit_ros2_bridge.talkback_sink");
constexpr auto kRestartDelay = std::chrono::milliseconds(250);
constexpr char kAppSrcName[] = "bridge_talkback_src";

// Receive tail: the bridge owns the edge and the output device's own buffering
// paces playback, so the configured fragment is used verbatim after the
// convert/resample stages. Unlike the publish tail, this queue must not leak —
// newest-wins would delete audio the operator is about to hear. The AudioStream
// ring buffer upstream already provides newest-wins, so the queue here is
// generous and lossless; the sink's jitter buffer absorbs the rest.
std::string buildSinkPipelineDescription(const std::string & sink_fragment)
{
  std::string description = "appsrc name=";
  description += kAppSrcName;
  description += " is-live=true block=false format=time do-timestamp=false";
  description += " ! queue max-size-buffers=0 max-size-bytes=0 max-size-time=2000000000";
  description += " ! audioconvert";
  description += " ! audioresample";
  description += " ! ";
  description += sink_fragment;
  return description;
}

}  // namespace

TalkbackSink::TalkbackSink(std::string sink_fragment)
: sink_fragment_(std::move(sink_fragment))
, failure_handler_(kRestartDelay, [this]() { restartPipeline(); })
{}

TalkbackSink::~TalkbackSink()
{
  stop();
}

bool TalkbackSink::bind(std::uint64_t reader_id, int sample_rate, int num_channels)
{
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return false;
  }

  std::uint64_t expected = 0;
  if (!owner_.compare_exchange_strong(expected, reader_id)) {
    logIgnoredOnce(reader_id);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    caps_rate_ = sample_rate;
    caps_channels_ = num_channels;
    try {
      startPipelineLocked();
    } catch (const std::exception & exc) {
      // A sink that is dead at bind time is not a binding failure: keep the claim so the owning
      // reader's live frame cadence re-arms the restart loop (push() schedules while the pipeline
      // is down) and playback self-heals when the device returns. Ownership is released only by
      // unbind()/reader finalize or stop().
      LogEvent(kLogger, "talkback_sink_start_failed").fieldOr("error", exc.what()).warn();
      stopPipelineLocked();
    }
  }

  return true;
}

void TalkbackSink::push(std::uint64_t reader_id, const std::int16_t * samples, std::size_t count)
{
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }
  if (owner_.load(std::memory_order_acquire) != reader_id) {
    logIgnoredOnce(reader_id);
    return;
  }
  if (samples == nullptr || count == 0) {
    return;
  }
  // Re-arm the restart loop: while the pipeline is down, the reader's 10 ms
  // push cadence retries schedule() until the handler accepts it. Lock-free
  // (see pipeline_active_ note in the header).
  if (!pipeline_active_.load(std::memory_order_acquire)) {
    (void)failure_handler_.schedule();
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (pipeline_ == nullptr || appsrc_element_ == nullptr) {
    return;
  }

  const std::size_t byte_size = count * sizeof(std::int16_t);
  utils::GstBufferPtr buffer(gst_buffer_new_allocate(nullptr, byte_size, nullptr));
  if (buffer == nullptr) {
    LogEvent(kLogger, "talkback_sink_push_failed").field("reason", "buffer_alloc_failed").warn();
    return;
  }

  {
    utils::GstBufferMap mapping(*buffer, GST_MAP_WRITE);
    if (!mapping.is_valid()) {
      LogEvent(kLogger, "talkback_sink_push_failed").field("reason", "buffer_map_failed").warn();
      return;
    }
    std::memcpy(mapping.get()->data, samples, byte_size);
  }

  // Explicit sample-count PTS/DURATION (do-timestamp=false): the audio clock
  // defines time as a perfectly regular stamp train, instead of wall-clock
  // arrival stamps that jitter against the pipeline clock and crackle.
  const auto rate = static_cast<GstClockTime>(caps_rate_);
  const GstClockTime duration = static_cast<GstClockTime>(count) * GST_SECOND / rate;
  const GstClockTime pts = static_cast<GstClockTime>(samples_pushed_) * GST_SECOND / rate;
  samples_pushed_ += count;

  GST_BUFFER_PTS(buffer.get()) = pts;
  GST_BUFFER_DTS(buffer.get()) = pts;
  GST_BUFFER_DURATION(buffer.get()) = duration;

  // Failure must not tear anything down — appsrc is block=false, so the next
  // push simply reclaims the pipeline.
  const GstFlowReturn result = gst_app_src_push_buffer(GST_APP_SRC(appsrc_element_.get()), buffer.release());
  if (result != GST_FLOW_OK) {
    LogEvent(kLogger, "talkback_sink_push_failed")
      .field("reason", "push_return")
      .field("flow_return", static_cast<int>(result))
      .warn();
  }
}

void TalkbackSink::unbind(std::uint64_t reader_id)
{
  std::uint64_t expected = reader_id;
  (void)owner_.compare_exchange_strong(expected, 0);
}

void TalkbackSink::stop()
{
  bool already_shutdown = false;
  (void)is_shutdown_.compare_exchange_strong(already_shutdown, true);
  failure_handler_.close();

  std::lock_guard<std::mutex> lock(mutex_);
  stopPipelineLocked();
}

// This path must stay lock-free against mutex_: the sync bus handler can
// deliver a failure from inside startPipelineLocked() or restartPipeline(),
// which hold mutex_ while GStreamer performs the state change. Locking here
// would deadlock against the calling thread itself. schedule() coalesces
// duplicate failures, and close() marks the handler closed before teardown, so
// no sink mutex_ is needed.
void TalkbackSink::onBusMessage(GstMessage * message)
{
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }
  if (message == nullptr || GST_MESSAGE_TYPE(message) != GST_MESSAGE_ERROR) {
    return;
  }

  GError * raw_error = nullptr;
  gst_message_parse_error(message, &raw_error, nullptr);
  utils::GErrorPtr error(raw_error);
  const std::string reason = error != nullptr && error->message != nullptr ? error->message : "error";

  if (!failure_handler_.schedule()) {
    return;
  }

  LogEvent(kLogger, "talkback_sink_restart_scheduled")
    .fieldOr("reason", reason)
    .field("restart_delay_ms", kRestartDelay.count())
    .warn();
}

void TalkbackSink::restartPipeline()
{
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }

  stopPipelineLocked();
  try {
    startPipelineLocked();
  } catch (const std::exception & exc) {
    // No retry cap: a permanently missing device restarts at ~4/s, bounded by
    // the 250 ms delay, while audio keeps arriving. Idle robots never restart:
    // re-arms come only from live frames on the track.
    LogEvent(kLogger, "talkback_sink_restart_failed").fieldOr("error", exc.what()).warn();
  }
}

void TalkbackSink::startPipelineLocked()
{
  utils::ensureGStreamerInitialized();

  if (sink_fragment_.empty()) {
    throw std::runtime_error("Talkback sink fragment is not configured.");
  }
  if (caps_rate_ <= 0 || caps_channels_ <= 0) {
    throw std::runtime_error("Talkback sink caps are not set.");
  }

  utils::GstElementPtr pipeline(gst_parse_launch(buildSinkPipelineDescription(sink_fragment_).c_str(), nullptr));
  if (pipeline == nullptr) {
    throw std::runtime_error("Failed to create GStreamer talkback sink pipeline.");
  }

  utils::GstElementPtr appsrc_element(gst_bin_get_by_name(GST_BIN(pipeline.get()), kAppSrcName));
  if (appsrc_element == nullptr || !GST_IS_APP_SRC(appsrc_element.get())) {
    throw std::runtime_error("Talkback sink pipeline did not create the expected appsrc.");
  }

  std::string caps_string = "audio/x-raw,format=S16LE,layout=interleaved,rate=";
  caps_string += std::to_string(caps_rate_);
  caps_string += ",channels=";
  caps_string += std::to_string(caps_channels_);

  GstCaps * raw_caps = gst_caps_from_string(caps_string.c_str());
  if (raw_caps == nullptr) {
    throw std::runtime_error("Failed to parse talkback sink caps: " + caps_string);
  }
  // gst_caps_from_string returns a floating reference. Sink it so the unique
  // ptr owns one full ref and set_caps takes its own; floating refs would be
  // consumed by the first taker and the unref here would dangle.
  g_object_ref_sink(raw_caps);
  utils::GstObjectPtr<GstCaps> caps(raw_caps);

  gst_app_src_set_caps(GST_APP_SRC(appsrc_element.get()), caps.get());
  gst_app_src_set_stream_type(GST_APP_SRC(appsrc_element.get()), GST_APP_STREAM_TYPE_STREAM);

  utils::GstBusPtr bus(gst_element_get_bus(pipeline.get()));
  gst_bus_set_sync_handler(
    bus.get(),
    [](GstBus *, GstMessage * message, gpointer user_data) -> GstBusSyncReply {
      static_cast<TalkbackSink *>(user_data)->onBusMessage(message);
      return GST_BUS_PASS;
    },
    this,
    nullptr);

  pipeline_ = std::move(pipeline);
  // The bin lookup's own reference keeps the appsrc valid for the pipeline's
  // lifetime; only the pointer is stored here.
  appsrc_element_ = std::move(appsrc_element);
  samples_pushed_ = 0;  // fresh pipeline = fresh clock base
  pipeline_active_.store(true, std::memory_order_release);

  const GstStateChangeReturn result = gst_element_set_state(pipeline_.get(), GST_STATE_PLAYING);
  if (result == GST_STATE_CHANGE_FAILURE) {
    stopPipelineLocked();
    throw std::runtime_error("Failed to set talkback sink pipeline to PLAYING.");
  }

  LogEvent(kLogger, "talkback_sink_started").field("caps", caps_string).info();
}

void TalkbackSink::stopPipelineLocked()
{
  if (pipeline_ == nullptr) {
    return;
  }

  utils::GstBusPtr bus(gst_element_get_bus(pipeline_.get()));
  gst_bus_set_sync_handler(bus.get(), nullptr, nullptr, nullptr);

  const GstStateChangeReturn result = gst_element_set_state(pipeline_.get(), GST_STATE_NULL);
  if (result == GST_STATE_CHANGE_ASYNC) {
    (void)gst_element_get_state(pipeline_.get(), nullptr, nullptr, GST_CLOCK_TIME_NONE);
  }

  appsrc_element_.reset();
  pipeline_.reset();
  pipeline_active_.store(false, std::memory_order_release);
}

void TalkbackSink::logIgnoredOnce(std::uint64_t reader_id)
{
  {
    std::lock_guard<std::mutex> ignored_lock(ignored_mutex_);
    if (!ignored_logged_.insert(reader_id).second) {
      return;
    }
  }
  LogEvent(kLogger, "talkback_sink_frame_dropped")
    .field("reader_id", reader_id)
    .field("owner_id", owner_.load(std::memory_order_acquire))
    .debug();
}

}  // namespace livekit_ros2_bridge::audio
