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

#include "runtime.hpp"

#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "livekit/remote_participant.h"
#include "livekit/remote_track_publication.h"
#include "livekit/room_event_types.h"
#include "protocol/constants.hpp"
#include "utils/log_event.hpp"

namespace livekit_ros2_bridge
{

namespace
{

// Talkback POC gate. Reads the env at Runtime construction; default
// OFF so normal runs are unchanged.
bool talkbackPocRequested()
{
  const char * flag = std::getenv("LIVEKIT_TALKBACK_POC");
  return flag != nullptr && std::string_view(flag) == "1";
}

constexpr char kTalkbackPocWavDir[] = "/tmp/talkback_poc";

const auto kPocLogger = rclcpp::get_logger("livekit_ros2_bridge.talkback_poc");

}  // namespace

Runtime::Runtime(Runtime::NodeInterfaces interfaces, std::unique_ptr<RoomConnection> connection, RuntimeConfig config)
: clock_(interfaces.get_node_clock_interface()->get_clock())
, logger_(interfaces.get_node_logging_interface()->get_logger())
, config_(std::move(config))
, room_connection_(std::move(connection))
, ros_executor_queue_(interfaces, clock_)
, ros_topic_publisher_(
    interfaces.get_node_topics_interface(), interfaces.get_node_graph_interface(), clock_, config_.access_policy)
, ros_service_caller_(
    interfaces.get_node_base_interface(),
    interfaces.get_node_graph_interface(),
    interfaces.get_node_waitables_interface())
, subscription_lease_manager_(
    interfaces.get_node_parameters_interface(),
    interfaces.get_node_topics_interface(),
    interfaces.get_node_graph_interface(),
    clock_,
    *room_connection_,
    config_.access_policy,
    &config_.subscription_qos,
    &config_.video_stream,
    &config_.audio_stream)
, rpc_router_(
    interfaces.get_node_graph_interface(),
    config_.access_policy,
    ros_executor_queue_,
    ros_service_caller_,
    subscription_lease_manager_)
, watchdog_(config_.watchdog, logger_)
{
  subscription_lease_manager_.startPruneTimer(
    interfaces.get_node_base_interface(), interfaces.get_node_timers_interface(), [this](std::function<void()> work) {
      submitRosWork(std::move(work));
    });

  const bool rpcs_registered = rpc_router_.registerRpcs(*room_connection_);
  if (!rpcs_registered) {
    throw std::runtime_error("Failed to register required RPC methods");
  }

  if (talkbackPocRequested()) {
    // TALKBACK POC — throwaway; delete with src/poc/.
    talkback_poc_ = std::make_unique<TalkbackPoc>(logger_, std::string(kTalkbackPocWavDir));
  } else {
    LogEvent(logger_, "talkback_poc_disabled").info();
  }

  room_connection_->start(config_.livekit, makeRoomCallbacks());
}

Runtime::~Runtime()
{
  if (callback_gate_.closeAndWait()) {
    LogEvent(logger_, "node_shutdown_start").info();
  }

  watchdog_.stop();
  ros_executor_queue_.shutdown();
  subscription_lease_manager_.shutdown();
  rpc_router_.unregisterRpcs();
  // Destroy the POC before the room stops: it flips every reader's stop flag
  // and closes the streams so no reader thread outlives this Runtime.
  talkback_poc_.reset();
  room_connection_->stop();
}

RoomEventCallbacks Runtime::makeRoomCallbacks()
{
  RoomEventCallbacks callbacks;
  callbacks.on_state_changed = [this](livekit::ConnectionState state) {
    (void)callback_gate_.run([this, state]() { watchdog_.onStateChanged(state); });
  };
  callbacks.on_user_packet_received = [this](const livekit::UserDataPacketEvent & event) {
    (void)callback_gate_.run([this, &event]() { onUserPacketReceived(event); });
  };
  callbacks.on_participant_disconnected = [this](const livekit::ParticipantDisconnectedEvent & event) {
    (void)callback_gate_.run([this, &event]() {
      if (talkback_poc_ != nullptr) {
        talkback_poc_->onParticipantDisconnected(event);
      }
      std::string identity = event.participant->identity();
      submitRosWork([this, identity = std::move(identity)]() { ros_service_caller_.cancelForRequester(identity); });
    });
  };

  // TALKBACK POC: these run on SDK delegate threads and are wrapped
  // in callback_gate_ like every other callback, so they are rejected once
  // teardown begins (the gate closes before talkback_poc_ is destroyed).
  // TalkbackPoc logs/threads only — no ROS work is submitted.
  if (talkback_poc_ != nullptr) {
    TalkbackPoc * poc = talkback_poc_.get();
    callbacks.on_track_published = [this](const livekit::TrackPublishedEvent & event) {
      (void)callback_gate_.run([&event]() {
        const auto * publication = event.publication.get();
        const std::string participant_identity =
          event.participant == nullptr ? std::string() : event.participant->identity();
        LogEvent(kPocLogger, "talkback_poc_track_published")
          .fieldOr("track_sid", publication == nullptr ? std::string() : publication->sid())
          .fieldOr("track_name", publication == nullptr ? std::string() : publication->name())
          .fieldOr("participant_identity", participant_identity)
          .info();
      });
    };
    callbacks.on_track_unpublished = [poc, this](const livekit::TrackUnpublishedEvent & event) {
      (void)callback_gate_.run([poc, &event]() { poc->onTrackUnpublished(event); });
    };
    callbacks.on_track_subscribed = [poc, this](const livekit::TrackSubscribedEvent & event) {
      (void)callback_gate_.run([poc, &event]() { poc->onTrackSubscribed(event); });
    };
    callbacks.on_track_unsubscribed = [poc, this](const livekit::TrackUnsubscribedEvent & event) {
      (void)callback_gate_.run([poc, &event]() { poc->onTrackUnsubscribed(event); });
    };
  }

  return callbacks;
}

void Runtime::onUserPacketReceived(const livekit::UserDataPacketEvent & event)
{
  const std::string topic = event.topic;
  const std::string requester = event.participant == nullptr ? "" : event.participant->identity();

  // SDK event and participant lifetimes do not extend to queued ROS work.
  if (topic == protocol::kPublishRequestTopic) {
    submitRosWork(
      [this, requester, payload = event.data]() { ros_topic_publisher_.handlePayload(requester, payload); });
    return;
  }

  if (topic == protocol::kHeartbeatTopic) {
    submitRosWork([this, requester, payload = event.data]() {
      subscription_lease_manager_.handleHeartbeatPayload(requester, payload);
    });
    return;
  }

  LogEvent(logger_, "livekit_packet_dropped")
    .field("reason", "unsupported_topic")
    .fieldOr("topic", topic)
    .fieldOr("requester_identity", requester)
    .warnThrottle(*clock_, std::chrono::seconds(5));
}

void Runtime::submitRosWork(std::function<void()> work)
{
  (void)callback_gate_.run(
    [this, work = std::move(work)]() mutable { (void)ros_executor_queue_.submit(std::move(work)); });
}

}  // namespace livekit_ros2_bridge
