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

// TALKBACK POC — THROWAWAY CODE, DO NOT MERGE.
// See talkback_sink.hpp. POC results: operator-talkback-poc-results.md.

#include "poc/talkback_sink.hpp"

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "utils/log_event.hpp"

namespace livekit_ros2_bridge
{

namespace
{

const auto kLogger = rclcpp::get_logger("livekit_ros2_bridge.talkback_poc_sink");
constexpr auto kRestartDelay = std::chrono::milliseconds(250);

const char kAppSrcName[] = "bridge_talkback_src";

// Mirrors the publish tail's philosophy (bridge owns the edge, leaky queue,
// newest-audio-wins); pulsesink hardcoded.
std::string buildSinkPipelineDescription()
{
  std::string description = "appsrc name=";
  description += kAppSrcName;
  description += " is-live=true block=false format=time do-timestamp=false";
  description += " ! queue max-size-time=100000000 leaky=downstream";
  description += " ! audioconvert";
  description += " ! audioresample";
  description += " ! pulsesink sync=false";
  return description;
}

}  // namespace

TalkbackSink::TalkbackSink()
: failure_handler_(kRestartDelay, [this]() { restartPipeline(); })
{}

TalkbackSink::~TalkbackSink()
{
  stop();
}

bool TalkbackSink::onFirstFrame(
  std::uint64_t reader_id, const std::string & track_sid, int sample_rate, int num_channels)
{
  if (is_shutdown_.load()) {
    return false;
  }

  std::uint64_t expected = 0;
  if (!owner_.compare_exchange_strong(expected, reader_id)) {
    logIgnoredOnce(reader_id);
    return false;
  }

  LogEvent(kLogger, "talkback_poc_sink_bound").field("reader_id", reader_id).fieldOr("track_sid", track_sid).info();

  {
    std::lock_guard<std::mutex> lock(mutex_);
    caps_rate_ = sample_rate;
    caps_channels_ = num_channels;
    try {
      startPipelineLocked();
    } catch (const std::exception & exc) {
      LogEvent(kLogger, "talkback_poc_sink_restart_failed")
        .field("attempt", "initial_start")
        .fieldOr("error", exc.what())
        .warn();
      stopPipelineLocked();
      return false;
    }
  }

  return true;
}

void TalkbackSink::push(std::uint64_t reader_id, const std::int16_t * samples, std::size_t count)
{
  if (is_shutdown_.load()) {
    return;
  }
  if (owner_.load() != reader_id) {
    logIgnoredOnce(reader_id);
    return;
  }
  if (samples == nullptr || count == 0) {
    return;
  }
  // Re-arm the restart loop: while the pipeline is down, the reader's 10 ms
  // push cadence retries schedule() until the handler accepts it. Lock-free
  // (see pipeline_active_ note in the header).
  if (!pipeline_active_.load()) {
    failure_handler_.schedule();
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (pipeline_ == nullptr || appsrc_ == nullptr) {
    return;
  }
  if (samples == nullptr || count == 0) {
    return;
  }

  const std::size_t byte_size = count * sizeof(std::int16_t);
  utils::GstBufferPtr buffer(gst_buffer_new_allocate(nullptr, byte_size, nullptr));
  if (buffer == nullptr) {
    LogEvent(kLogger, "talkback_poc_sink_push_failed").field("reason", "buffer_alloc_failed").warn();
    return;
  }

  {
    utils::GstBufferMap mapping(*buffer, GST_MAP_WRITE);
    if (!mapping.is_valid()) {
      LogEvent(kLogger, "talkback_poc_sink_push_failed").field("reason", "buffer_map_failed").warn();
      return;
    }
    if (byte_size > 0) {
      std::memcpy(mapping.get()->data, samples, byte_size);
    }
  }

  // Explicit sample-count PTS/DURATION (do-timestamp=false): the audio clock
  // defines time as a perfectly regular stamp train, instead of wall-clock
  // arrival stamps that jitter against the pipeline clock and crackle.
  const int rate = caps_rate_;
  const GstClockTime duration =
    static_cast<GstClockTime>(count) * static_cast<GstClockTime>(GST_SECOND) / static_cast<GstClockTime>(rate);
  const GstClockTime pts = static_cast<GstClockTime>(samples_pushed_) * static_cast<GstClockTime>(GST_SECOND) /
                           static_cast<GstClockTime>(rate);
  samples_pushed_ += count;

  GST_BUFFER_PTS(buffer.get()) = pts;
  GST_BUFFER_DTS(buffer.get()) = pts;
  GST_BUFFER_DURATION(buffer.get()) = duration;

  // Failure must not tear anything down — the queue is leaky and appsrc is
  // block=false, so the next push simply reclaims the pipeline.
  const GstFlowReturn result = gst_app_src_push_buffer(appsrc_.get(), buffer.release());
  if (result != GST_FLOW_OK) {
    LogEvent(kLogger, "talkback_poc_sink_push_failed")
      .field("reason", "push_return")
      .field("flow_return", static_cast<int>(result))
      .warn();
  }
}

void TalkbackSink::unbind(std::uint64_t reader_id)
{
  std::uint64_t expected = reader_id;
  if (!owner_.compare_exchange_strong(expected, 0)) {
    return;
  }
  LogEvent(kLogger, "talkback_poc_sink_released").field("reader_id", reader_id).info();
}

void TalkbackSink::stop()
{
  bool already_shutdown = true;
  if (is_shutdown_.compare_exchange_strong(already_shutdown, true)) {
    LogEvent(kLogger, "talkback_poc_sink_stopped").info();
  }
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
  if (is_shutdown_.load()) {
    return;
  }
  if (GST_MESSAGE_TYPE(message) != GST_MESSAGE_ERROR) {
    return;
  }

  GError * raw_error = nullptr;
  gst_message_parse_error(message, &raw_error, nullptr);
  utils::GErrorPtr error(raw_error);
  const std::string reason = error != nullptr && error->message != nullptr ? error->message : "error";

  if (!failure_handler_.schedule()) {
    return;
  }

  LogEvent(kLogger, "talkback_poc_sink_restart_scheduled")
    .fieldOr("reason", reason)
    .field("restart_delay_ms", kRestartDelay.count())
    .warn();
}

void TalkbackSink::restartPipeline()
{
  if (is_shutdown_.load()) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (is_shutdown_.load()) {
    return;
  }

  stopPipelineLocked();
  try {
    startPipelineLocked();
  } catch (const std::exception & exc) {
    // No retry cap: a permanently missing device restarts at ~4/s, bounded by
    // the 250 ms delay, while the node stays alive.
    LogEvent(kLogger, "talkback_poc_sink_restart_failed").fieldOr("error", exc.what()).warn();
  }
}

void TalkbackSink::startPipelineLocked()
{
  utils::ensureGStreamerInitialized();

  if (caps_rate_ <= 0 || caps_channels_ <= 0) {
    throw std::runtime_error("Talkback sink caps are not set.");
  }

  utils::GstElementPtr pipeline(gst_parse_launch(buildSinkPipelineDescription().c_str(), nullptr));
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
  utils::GstObjectPtr<GstCaps> caps(raw_caps);

  gst_app_src_set_caps(GST_APP_SRC(appsrc_element.get()), raw_caps);
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
  appsrc_ = utils::GstObjectPtr<GstAppSrc>(GST_APP_SRC(appsrc_element.release()));
  samples_pushed_ = 0;  // fresh pipeline = fresh clock base
  pipeline_active_.store(true);

  const GstStateChangeReturn result = gst_element_set_state(pipeline_.get(), GST_STATE_PLAYING);
  if (result == GST_STATE_CHANGE_FAILURE) {
    stopPipelineLocked();
    throw std::runtime_error("Failed to set talkback sink pipeline to PLAYING.");
  }

  LogEvent(kLogger, "talkback_poc_sink_started").field("caps", caps_string).info();
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

  pipeline_.reset();
  appsrc_.reset();
  pipeline_active_.store(false);
}

void TalkbackSink::logIgnoredOnce(std::uint64_t reader_id)
{
  {
    std::lock_guard<std::mutex> ignored_lock(ignored_mutex_);
    if (!ignored_logged_.insert(reader_id).second) {
      return;
    }
  }
  LogEvent(kLogger, "talkback_poc_sink_ignored").field("reader_id", reader_id).field("owner_id", owner_.load()).debug();
}

}  // namespace livekit_ros2_bridge
