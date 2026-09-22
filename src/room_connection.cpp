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

#include "room_connection.hpp"

#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "livekit/data_stream.h"
#include "livekit/data_track_error.h"
#include "livekit/livekit.h"
#include "livekit/local_audio_track.h"
#include "livekit/local_data_track.h"
#include "livekit/local_participant.h"
#include "livekit/local_video_track.h"
#include "livekit/remote_participant.h"
#include "livekit/remote_track_publication.h"
#include "livekit/room_delegate.h"
#include "livekit/rpc_error.h"
#include "livekit/video_source.h"
#include "protocol/constants.hpp"
#include "rclcpp/logging.hpp"
#include "room_connection/remote_publication_mirror.hpp"
#include "utils/log_event.hpp"

namespace livekit_ros2_bridge
{

namespace
{

const auto kLogger = rclcpp::get_logger("livekit_ros2_bridge.room_connection");
constexpr char kLocalParticipantUnavailable[] = "LiveKit local participant unavailable.";

struct ParticipantRef
{
  std::shared_ptr<livekit::Room> room;
  std::shared_ptr<livekit::LocalParticipant> participant;
  std::uint64_t room_generation = 0;
};

class SdkRoomConnection final : public RoomConnection, private livekit::RoomDelegate
{
public:
  SdkRoomConnection() = default;

  ~SdkRoomConnection() override
  {
    stop();
  }

  void start(LiveKitConfig config, RoomEventCallbacks callbacks) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (connect_task_.joinable()) {
      return;
    }

    config_ = std::move(config);
    callbacks_ = std::move(callbacks);
    stop_requested_ = false;
    state_ = livekit::ConnectionState::Disconnected;
    connect_task_ = std::thread([this]() { run(); });
  }

  void stop() override
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!connect_task_.joinable()) {
      return;
    }
    stop_requested_ = true;
    lock.unlock();

    // Join outside mutex_; the connection task may reacquire it before exit.
    connect_task_.join();

    detachRoom();

    bool shutdown_sdk = false;
    {
      std::lock_guard<std::mutex> clear_lock(mutex_);
      callbacks_ = RoomEventCallbacks{};
      state_ = livekit::ConnectionState::Disconnected;
      shutdown_sdk = sdk_initialized_;
      sdk_initialized_ = false;
    }
    if (shutdown_sdk) {
      livekit::shutdown();
    }
  }

  bool registerRpc(const std::string & method, livekit::LocalParticipant::RpcHandler handler) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    rpc_handlers_[method] = std::move(handler);
    return registerRpcLocked(method);
  }

  bool unregisterRpc(const std::string & method) override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    rpc_handlers_.erase(method);

    auto participant = lockedLocalParticipant(room_);
    if (participant == nullptr) {
      return true;
    }

    try {
      participant->unregisterRpcMethod(method);
    } catch (const std::exception & exc) {
      LogEvent(kLogger, "rpc_method_unregistration_failed").field("method", method).field("error", exc.what()).error();
      return false;
    }
    return true;
  }

  void publishData(
    const std::vector<std::uint8_t> & payload,
    bool reliable,
    const std::vector<std::string> & destination_identities,
    const std::string & topic) override
  {
    const auto ref = participantRef();
    if (ref.participant == nullptr) {
      throw std::runtime_error(kLocalParticipantUnavailable);
    }
    ref.participant->publishData(payload, reliable, destination_identities, topic);
  }

  std::shared_ptr<livekit::LocalDataTrack> publishDataTrack(const std::string & name) override
  {
    const auto ref = participantRef();
    if (ref.participant == nullptr) {
      LogEvent(kLogger, "data_track_publish_failed")
        .fieldOr("track_name", name)
        .field("reason", "local_participant_unavailable")
        .warn();
      throw std::runtime_error(kLocalParticipantUnavailable);
    }

    auto result = ref.participant->publishDataTrack(name);
    if (!result) {
      const auto & error = result.error();
      LogEvent(kLogger, "data_track_publish_failed")
        .fieldOr("track_name", name)
        .fieldEnum("sdk_error_code", error.code)
        .fieldOr("error", error.message)
        .warn();
      throw std::runtime_error("Failed to publish data track '" + name + "': " + result.error().message);
    }

    auto track = result.value();
    if (track == nullptr) {
      LogEvent(kLogger, "data_track_publish_failed").fieldOr("track_name", name).field("reason", "null_track").warn();
      throw std::runtime_error("LiveKit returned a null data track.");
    }

    const auto & info = track->info();
    LogEvent(kLogger, "data_track_published").fieldOr("track_name", info.name).fieldOr("track_sid", info.sid).info();
    return track;
  }

  livekit::Result<void, livekit::LocalDataTrackTryPushError> tryPushDataTrack(
    const std::shared_ptr<livekit::LocalDataTrack> & track, const livekit::DataTrackFrame & frame) override
  {
    return track->tryPush(frame);
  }

  void unpublishDataTrack(const std::shared_ptr<livekit::LocalDataTrack> & track) override
  {
    if (track == nullptr) {
      return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == livekit::ConnectionState::Disconnected) {
      return;
    }
    auto participant = lockedLocalParticipant(room_);
    if (participant == nullptr) {
      return;
    }
    const auto & info = track->info();
    try {
      participant->unpublishDataTrack(track);
    } catch (...) {
      LogEvent(kLogger, "data_track_unpublish_failed")
        .fieldOr("track_name", info.name)
        .fieldOr("track_sid", info.sid)
        .fieldException("error", std::current_exception())
        .warn();
      throw;
    }
    LogEvent(kLogger, "data_track_unpublished").fieldOr("track_name", info.name).fieldOr("track_sid", info.sid).info();
  }

  std::shared_ptr<livekit::LocalVideoTrack> publishVideoTrack(
    const std::string & name,
    const std::shared_ptr<livekit::VideoSource> & source,
    const livekit::TrackPublishOptions & options) override
  {
    if (name.empty()) {
      throw std::invalid_argument("Video track name is required.");
    }
    if (source == nullptr) {
      throw std::invalid_argument("Video source is required.");
    }

    const auto ref = participantRef();
    if (ref.participant == nullptr) {
      throw std::runtime_error(kLocalParticipantUnavailable);
    }

    try {
      auto track = livekit::LocalVideoTrack::createLocalVideoTrack(name, source);
      if (track == nullptr) {
        throw std::runtime_error("Failed to publish video track '" + name + "'.");
      }

      livekit::TrackPublishOptions publish_options = options;
      publish_options.source = livekit::TrackSource::SOURCE_CAMERA;
      ref.participant->publishTrack(track, publish_options);

      const auto publication = track->publication();
      if (publication == nullptr) {
        throw std::runtime_error("Failed to publish video track '" + name + "'.");
      }

      LogEvent(kLogger, "video_track_published")
        .fieldOr("track_sid", publication->sid())
        .fieldOr("track_name", publication->name())
        .info();

      recordTrackIfCurrent(name, track, ref.room_generation);
      return track;
    } catch (...) {
      LogEvent(kLogger, "video_track_publish_failed")
        .fieldOr("track_name", name)
        .field("track_width", source->width())
        .field("track_height", source->height())
        .fieldException("error", std::current_exception())
        .warn();
      throw;
    }
  }

  void unpublishVideoTrack(const std::shared_ptr<livekit::LocalVideoTrack> & track) override
  {
    if (track == nullptr) {
      return;
    }

    const std::string & name = track->name();
    try {
      unpublishVideoTrackIfCurrent(track);
    } catch (...) {
      try {
        LogEvent(kLogger, "video_track_unpublish_failed")
          .field("track_name", name)
          .fieldOr("track_sid", track->sid())
          .fieldException("error", std::current_exception())
          .warn();
      } catch (...) {}
    }
  }

  std::shared_ptr<livekit::LocalAudioTrack> publishAudioTrack(
    const std::string & name,
    const std::shared_ptr<livekit::AudioSource> & source,
    const livekit::TrackPublishOptions & options) override
  {
    if (name.empty()) {
      throw std::invalid_argument("Audio track name is required.");
    }
    if (source == nullptr) {
      throw std::invalid_argument("Audio source is required.");
    }

    const auto ref = participantRef();
    if (ref.participant == nullptr) {
      throw std::runtime_error(kLocalParticipantUnavailable);
    }

    try {
      auto track = livekit::LocalAudioTrack::createLocalAudioTrack(name, source);
      if (track == nullptr) {
        throw std::runtime_error("Failed to publish audio track '" + name + "'.");
      }

      livekit::TrackPublishOptions publish_options = options;
      publish_options.source = livekit::TrackSource::SOURCE_MICROPHONE;
      ref.participant->publishTrack(track, publish_options);

      const auto publication = track->publication();
      if (publication == nullptr) {
        throw std::runtime_error("Failed to publish audio track '" + name + "'.");
      }

      LogEvent(kLogger, "audio_track_published")
        .fieldOr("track_sid", publication->sid())
        .fieldOr("track_name", publication->name())
        .info();

      recordAudioTrackIfCurrent(name, track, ref.room_generation);
      return track;
    } catch (...) {
      LogEvent(kLogger, "audio_track_publish_failed")
        .fieldOr("track_name", name)
        .field("track_sample_rate", source->sampleRate())
        .field("track_channels", source->numChannels())
        .fieldException("error", std::current_exception())
        .warn();
      throw;
    }
  }

  void unpublishAudioTrack(const std::shared_ptr<livekit::LocalAudioTrack> & track) override
  {
    if (track == nullptr) {
      return;
    }

    const std::string & name = track->name();
    try {
      unpublishAudioTrackIfCurrent(track);
    } catch (...) {
      try {
        LogEvent(kLogger, "audio_track_unpublish_failed")
          .field("track_name", name)
          .fieldOr("track_sid", track->sid())
          .fieldException("error", std::current_exception())
          .warn();
      } catch (...) {}
    }
  }

  bool subscribeRemoteTrack(const std::string & participant_identity, const std::string & track_sid) override
  {
    if (participant_identity.empty() || track_sid.empty()) {
      return false;
    }

    std::shared_ptr<livekit::RemoteTrackPublication> publication;
    {
      // Read only the mirror: the SDK's own publication map is off-limits from this thread.
      std::lock_guard<std::mutex> lock(mutex_);
      const auto record = remote_publications_.find(track_sid);
      if (!record.has_value()) {
        LogEvent(kLogger, "remote_track_subscribe_failed")
          .field("reason", "publication_unavailable")
          .fieldOr("participant_identity", participant_identity)
          .fieldOr("track_sid", track_sid)
          .warn();
        return false;
      }
      if (record->entry.subscribed) {
        return true;
      }
      publication = record->handle;
    }

    if (publication == nullptr) {
      LogEvent(kLogger, "remote_track_subscribe_failed")
        .field("reason", "publication_unavailable")
        .fieldOr("participant_identity", participant_identity)
        .fieldOr("track_sid", track_sid)
        .warn();
      return false;
    }

    // setSubscribed() is a blocking FFI request; never hold mutex_ across it.
    try {
      publication->setSubscribed(true);
    } catch (const std::exception & exc) {
      LogEvent(kLogger, "remote_track_subscribe_failed")
        .fieldOr("participant_identity", participant_identity)
        .fieldOr("track_sid", track_sid)
        .field("error", exc.what())
        .warn();
      return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    remote_publications_.setSubscribed(track_sid, true);
    return true;
  }

  void unsubscribeRemoteTrack(const std::string & participant_identity, const std::string & track_sid) override
  {
    if (participant_identity.empty() || track_sid.empty()) {
      return;
    }

    std::shared_ptr<livekit::RemoteTrackPublication> publication;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto record = remote_publications_.find(track_sid);
      if (!record.has_value() || !record->entry.subscribed) {
        return;
      }
      publication = record->handle;
    }

    if (publication == nullptr) {
      return;
    }

    // setSubscribed() is a blocking FFI request; never hold mutex_ across it.
    try {
      publication->setSubscribed(false);
    } catch (const std::exception & exc) {
      LogEvent(kLogger, "remote_track_unsubscribe_failed")
        .fieldOr("participant_identity", participant_identity)
        .fieldOr("track_sid", track_sid)
        .field("error", exc.what())
        .warn();
      return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    remote_publications_.setSubscribed(track_sid, false);
  }

  std::vector<RoomConnection::RemoteTrackSnapshotEntry> remoteTrackSnapshot() override
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return remote_publications_.snapshot();
  }

  void sendByteStream(
    const std::string & topic,
    const std::string & name,
    const std::string & content_type,
    std::shared_ptr<const std::vector<std::uint8_t>> payload,
    const std::string & destination_identity) override
  {
    // Defend the interface: callers dispatch only when a cached value exists, so a null buffer is a
    // caller bug. Reject it before spawning a thread, and as an invalid argument (not a silent skip)
    // so the caller can't mistake "nothing sent" for a successful send.
    if (payload == nullptr) {
      throw std::invalid_argument("Byte-stream payload is required.");
    }

    const auto ref = participantRef();
    if (ref.participant == nullptr) {
      throw std::runtime_error(kLocalParticipantUnavailable);
    }

    // STOPGAP — converge with the broader uncancellable-blocking-.get() sweep.
    //
    // livekit::ByteStreamWriter::write() sends each chunk through an uncancellable blocking SDK
    // call. On robot networks these block far more often than the SDK surface suggests, and a
    // stalled client can hang it indefinitely. Running write()/close() on the ROS executor or a
    // LiveKit callback thread would therefore freeze live data relay, heartbeats, and every other
    // ROS callback. So the whole construct + write() + close() runs on a *detached, sacrificial*
    // thread that holds a strong reference to the room (keeping the local participant alive for the
    // transfer). A hung client wedges a sacrificial thread instead of a load-bearing one.
    //
    // This is a deliberately localized fix; it must merge into the systemic blocking-call sweep so
    // both land on one policy. Do not move write()/close() back onto a caller thread.
    std::thread([room = ref.room, topic, name, content_type, payload, destination_identity]() {
      // One backstop for the whole body: the ByteStreamWriter constructor, the write, and both
      // close() calls are uncancellable FFI that throw (per the SDK header) on transfer errors and
      // teardown races. An exception escaping a detached thread calls std::terminate() and aborts
      // the process, so — like RosExecutorQueue::drain() — nothing is allowed past this boundary,
      // and every failure is logged here exactly once.
      try {
        auto participant = lockedLocalParticipant(room);
        if (participant == nullptr) {
          LogEvent(kLogger, "byte_stream_send_skipped")
            .field("topic", topic)
            .field("reason", "local_participant_unavailable")
            .warn();
          return;
        }

        livekit::ByteStreamWriter writer(
          *participant, name, topic, {}, "", payload->size(), content_type, {destination_identity});
        try {
          writer.write(*payload);
          writer.close();
        } catch (...) {
          // The writer does not close on destruction; an unterminated stream leaves the remote
          // reader waiting forever, so close with a reason before letting the boundary below log it.
          try {
            writer.close("send failed");
          } catch (...) {}
          throw;
        }
      } catch (...) {
        LogEvent(kLogger, "byte_stream_send_failed")
          .field("topic", topic)
          .field("destination_identity", destination_identity)
          .fieldException("error", std::current_exception())
          .warn();
      }
    }).detach();
  }

private:
  // Requires mutex_ to be held (or to be passed a room snapshotted under it).
  static std::shared_ptr<livekit::LocalParticipant> lockedLocalParticipant(const std::shared_ptr<livekit::Room> & room)
  {
    return room == nullptr ? std::shared_ptr<livekit::LocalParticipant>{} : room->localParticipant().lock();
  }

  ParticipantRef participantRef() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ParticipantRef ref;
    ref.room = room_;
    ref.room_generation = room_generation_;
    if (state_ != livekit::ConnectionState::Disconnected) {
      ref.participant = lockedLocalParticipant(ref.room);
    }
    return ref;
  }

  void unpublishVideoTrackIfCurrent(const std::shared_ptr<livekit::LocalVideoTrack> & track)
  {
    std::uint64_t room_generation = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = track_room_generations_.find(track.get());
      if (it == track_room_generations_.end()) {
        return;
      }

      room_generation = it->second;
      track_room_generations_.erase(it);
    }

    const auto publication = track->publication();
    if (publication == nullptr) {
      return;
    }

    auto ref = participantRef();
    if (ref.participant == nullptr) {
      return;
    }
    if (ref.room_generation != room_generation) {
      return;
    }

    ref.participant->unpublishTrack(publication->sid());
    LogEvent(kLogger, "video_track_unpublished")
      .fieldOr("track_name", publication->name())
      .fieldOr("track_sid", publication->sid())
      .info();
  }

  void recordTrackIfCurrent(
    const std::string & name, const std::shared_ptr<livekit::LocalVideoTrack> & track, std::uint64_t room_generation)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (room_generation != room_generation_) {
      // The room changed while publishTrack() was in flight; leave this stale track untracked.
      LogEvent(kLogger, "video_track_publish_stale")
        .field("track_name", name)
        .fieldOr("track_sid", track->sid())
        .warn();
      return;
    }
    track_room_generations_[track.get()] = room_generation;
  }

  void unpublishAudioTrackIfCurrent(const std::shared_ptr<livekit::LocalAudioTrack> & track)
  {
    std::uint64_t room_generation = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = audio_track_room_generations_.find(track.get());
      if (it == audio_track_room_generations_.end()) {
        return;
      }

      room_generation = it->second;
      audio_track_room_generations_.erase(it);
    }

    const auto publication = track->publication();
    if (publication == nullptr) {
      return;
    }

    auto ref = participantRef();
    if (ref.participant == nullptr) {
      return;
    }
    if (ref.room_generation != room_generation) {
      return;
    }

    ref.participant->unpublishTrack(publication->sid());
    LogEvent(kLogger, "audio_track_unpublished")
      .fieldOr("track_name", publication->name())
      .fieldOr("track_sid", publication->sid())
      .info();
  }

  void recordAudioTrackIfCurrent(
    const std::string & name, const std::shared_ptr<livekit::LocalAudioTrack> & track, std::uint64_t room_generation)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (room_generation != room_generation_) {
      // The room changed while publishTrack() was in flight; leave this stale track untracked.
      LogEvent(kLogger, "audio_track_publish_stale")
        .field("track_name", name)
        .fieldOr("track_sid", track->sid())
        .warn();
      return;
    }
    audio_track_room_generations_[track.get()] = room_generation;
  }

  void run()
  {
    if (!livekit::initialize()) {
      LogEvent(kLogger, "livekit_initialize_failed").error();
      return;
    }

    bool should_connect = true;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      sdk_initialized_ = true;
      should_connect = !stop_requested_;
    }
    if (should_connect) {
      (void)connect();
    }
  }

  bool connect()
  {
    LiveKitConfig config;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      config = config_;
    }

    auto room = connectRoom(config);
    if (room == nullptr) {
      return false;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stop_requested_) {
        room->setDelegate(nullptr);
        return false;
      }
    }

    auto active_room = room;
    if (!activateRoom(std::move(room))) {
      detachRoom();
      return false;
    }

    LogEvent(kLogger, "room_connected")
      .fieldOr("url", config.url)
      .fieldOr("room_sid", active_room->roomInfo().sid)
      .fieldOr("room_name", active_room->roomInfo().name)
      .info();
    transitionState(livekit::ConnectionState::Connected);
    return true;
  }

  std::shared_ptr<livekit::Room> connectRoom(const LiveKitConfig & config)
  {
    auto room = std::make_shared<livekit::Room>();
    room->setDelegate(this);

    // The bridge subscribes only to tracks it names (ADR 0001); room-wide media
    // reception was never a contract. Track events still arrive for publications
    // the bridge deliberately subscribes to.
    livekit::RoomOptions options;
    options.auto_subscribe = false;

    bool connected = false;
    try {
      connected = room->connect(config.url, config.access_token, options);
      if (!connected) {
        LogEvent(kLogger, "room_connect_failed")
          .field("reason", "connect_returned_false")
          .fieldOr("url", config.url)
          .field("token_present", !config.access_token.empty())
          .error();
      }
    } catch (...) {
      LogEvent(kLogger, "room_connect_failed")
        .field("reason", "exception")
        .fieldOr("url", config.url)
        .field("token_present", !config.access_token.empty())
        .fieldException("error", std::current_exception())
        .error();
    }

    if (!connected) {
      room->setDelegate(nullptr);
      return nullptr;
    }

    if (room->localParticipant().lock() == nullptr) {
      LogEvent(kLogger, "room_connect_failed")
        .field("reason", "local_participant_unavailable")
        .fieldOr("url", config.url)
        .field("token_present", !config.access_token.empty())
        .error();
      room->setDelegate(nullptr);
      return nullptr;
    }

    return room;
  }

  bool activateRoom(std::shared_ptr<livekit::Room> room)
  {
    // The one unavoidable direct read of the SDK publication map. It runs while the room is not yet
    // published to room_, so no live reader can race the FFI thread; every later mirror mutation
    // happens on the FFI delegate thread. Collect before taking mutex_ so we never iterate
    // SDK-owned participants under our lock.
    std::vector<RemotePublicationMirror::Record> existing_publications;
    if (room != nullptr) {
      for (const auto & remote_handle : room->remoteParticipants()) {
        auto remote_participant = remote_handle.lock();
        if (remote_participant == nullptr) {
          continue;
        }
        for (const auto & [sid, publication] : remote_participant->trackPublications()) {
          if (publication == nullptr || sid.empty()) {
            continue;
          }
          existing_publications.push_back(
            RemotePublicationMirror::Record{
              RoomConnection::RemoteTrackSnapshotEntry{
                remote_participant->identity(),
                publication->sid(),
                publication->name(),
                publication->kind(),
                publication->subscribed()},
              publication});
        }
      }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    ++room_generation_;
    room_ = std::move(room);

    // Seed the mirror so tracks already present at connect are not re-emitted as fresh publications
    // by the SDK null-publication workaround (onConnected already re-subscribes them from snapshot).
    remote_publications_.clear();
    remote_publications_.merge(existing_publications);

    bool registered = true;
    for (const auto & entry : rpc_handlers_) {
      if (!registerRpcLocked(entry.first)) {
        registered = false;
      }
    }
    return registered;
  }

  void detachRoom()
  {
    std::shared_ptr<livekit::Room> detached_room;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      detached_room = std::move(room_);
      if (detached_room != nullptr) {
        ++room_generation_;
      }
      // Old-room tracks must not unpublish from the replacement room.
      track_room_generations_.clear();
      remote_publications_.clear();
      state_ = livekit::ConnectionState::Disconnected;
    }

    if (detached_room != nullptr) {
      detached_room->setDelegate(nullptr);
      detached_room.reset();
    }
  }

  void transitionState(livekit::ConnectionState state)
  {
    std::function<void(livekit::ConnectionState)> callback;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == state) {
        return;
      }
      state_ = state;
      callback = callbacks_.on_state_changed;
    }

    if (callback) {
      callback(state);
    }
  }

  static RemoteTrackEvent makeRemoteTrackEvent(
    std::string participant_identity, std::string track_sid, std::string track_name, livekit::TrackKind track_kind)
  {
    RemoteTrackEvent remote_event;
    remote_event.participant_identity = std::move(participant_identity);
    remote_event.track_sid = std::move(track_sid);
    remote_event.track_name = std::move(track_name);
    remote_event.track_kind = track_kind;
    return remote_event;
  }

  template <typename EventT>
  void forwardRemoteTrackEvent(
    const std::function<void(const RemoteTrackEvent &)> & callback,
    const EventT & event,
    const std::shared_ptr<livekit::Track> & track)
  {
    if (callback == nullptr) {
      return;
    }

    RemoteTrackEvent translated;
    if (event.participant != nullptr) {
      translated.participant_identity = event.participant->identity();
    }
    if (event.publication != nullptr) {
      translated.track_sid = event.publication->sid();
      translated.track_name = event.publication->name();
      translated.track_kind = event.publication->kind();
    }
    if (translated.track_sid.empty() && track != nullptr) {
      translated.track_sid = track->sid();
      translated.track_kind = track->kind();
    }
    translated.track = track;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == livekit::ConnectionState::Disconnected) {
        return;
      }
    }

    callback(translated);
  }

  void onParticipantDisconnected(livekit::Room &, const livekit::ParticipantDisconnectedEvent & event) override
  {
    const auto * participant = event.participant;
    if (participant == nullptr) {
      return;
    }

    std::function<void(const livekit::ParticipantDisconnectedEvent &)> callback;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      remote_publications_.eraseIdentity(participant->identity());
      if (state_ != livekit::ConnectionState::Connected) {
        return;
      }
      callback = callbacks_.on_participant_disconnected;
    }

    if (!callback) {
      return;
    }

    if (participant->identity().empty()) {
      return;
    }

    callback(event);
  }

  void onTrackPublished(livekit::Room &, const livekit::TrackPublishedEvent & event) override
  {
    if (event.participant == nullptr) {
      return;
    }

    std::vector<RemoteTrackEvent> events;
    std::function<void(const RemoteTrackEvent &)> callback;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Only act on live publications while fully connected. The SDK rehydrates remote publications
      // mid-reconnect; subscribing then sets the publication's subscribed flag without media able to
      // flow, so the post-reconnect snapshot (which skips already-subscribed tracks) would never
      // re-issue the request. Deferring to Connected lets onConnected re-subscribe.
      if (state_ != livekit::ConnectionState::Connected) {
        return;
      }
      callback = callbacks_.on_remote_track_published;
      if (callback == nullptr) {
        return;
      }

      if (event.publication != nullptr && !event.publication->sid().empty()) {
        const auto & publication = event.publication;
        remote_publications_.merge(
          RoomConnection::RemoteTrackSnapshotEntry{
            event.participant->identity(),
            publication->sid(),
            publication->name(),
            publication->kind(),
            publication->subscribed()},
          publication);
        events.push_back(makeRemoteTrackEvent(
          event.participant->identity(), publication->sid(), publication->name(), publication->kind()));
      } else {
        // livekit/client-sdk-cpp v1.6.0 room.cpp kTrackPublished move()s the publication into the
        // participant's map and then assigns the moved-from (null) shared_ptr to event.publication,
        // so the event carries no sid or name. The hydrated publication is already stored on the
        // participant, so recover every publication this identity has not seen yet. This
        // deliberately leaks no track names into the connection layer: the manager still applies
        // the exact-name gate.
        std::vector<RemotePublicationMirror::Record> hydrated_publications;
        for (const auto & [sid, publication] : event.participant->trackPublications()) {
          if (publication == nullptr || sid.empty()) {
            continue;
          }
          hydrated_publications.push_back(
            RemotePublicationMirror::Record{
              RoomConnection::RemoteTrackSnapshotEntry{
                event.participant->identity(),
                publication->sid(),
                publication->name(),
                publication->kind(),
                publication->subscribed()},
              publication});
        }
        for (const auto & newly_added : remote_publications_.merge(hydrated_publications)) {
          events.push_back(makeRemoteTrackEvent(
            newly_added.participant_identity, newly_added.track_sid, newly_added.track_name, newly_added.track_kind));
        }
      }
    }

    for (const auto & remote_event : events) {
      callback(remote_event);
    }
  }

  void onTrackUnpublished(livekit::Room &, const livekit::TrackUnpublishedEvent & event) override
  {
    if (event.publication != nullptr && !event.publication->sid().empty()) {
      std::lock_guard<std::mutex> lock(mutex_);
      remote_publications_.erase(event.publication->sid());
    }
    forwardRemoteTrackEvent(callbacks_.on_remote_track_unpublished, event, nullptr);
  }

  void onTrackSubscribed(livekit::Room &, const livekit::TrackSubscribedEvent & event) override
  {
    if (event.participant != nullptr && event.publication != nullptr) {
      const auto & publication = event.publication;
      std::lock_guard<std::mutex> lock(mutex_);
      remote_publications_.merge(
        RoomConnection::RemoteTrackSnapshotEntry{
          event.participant->identity(), publication->sid(), publication->name(), publication->kind(), true},
        publication);
    }
    forwardRemoteTrackEvent(callbacks_.on_remote_track_subscribed, event, event.track);
  }

  void onTrackUnsubscribed(livekit::Room &, const livekit::TrackUnsubscribedEvent & event) override
  {
    if (event.publication != nullptr && !event.publication->sid().empty()) {
      std::lock_guard<std::mutex> lock(mutex_);
      remote_publications_.setSubscribed(event.publication->sid(), false);
    }
    forwardRemoteTrackEvent(callbacks_.on_remote_track_unsubscribed, event, event.track);
  }

  void onTrackSubscriptionFailed(livekit::Room &, const livekit::TrackSubscriptionFailedEvent & event) override
  {
    std::function<void(const RemoteTrackSubscriptionFailedEvent &)> callback;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == livekit::ConnectionState::Disconnected) {
        return;
      }
      callback = callbacks_.on_remote_track_subscription_failed;
    }

    if (callback == nullptr) {
      return;
    }

    RemoteTrackSubscriptionFailedEvent translated;
    if (event.participant != nullptr) {
      translated.participant_identity = event.participant->identity();
    }
    translated.track_sid = event.track_sid;
    translated.error = event.error;

    callback(translated);
  }

  void onRoomSidChanged(livekit::Room & room, const livekit::RoomSidChangedEvent & event) override
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (room_.get() != &room || state_ == livekit::ConnectionState::Disconnected) {
        return;
      }
    }

    LogEvent(kLogger, "room_sid_changed").fieldOr("room_sid", event.sid).info();
  }

  void onRoomMoved(livekit::Room &, const livekit::RoomMovedEvent & event) override
  {
    LogEvent(kLogger, "room_moved").fieldOr("room_sid", event.info.sid).info();
  }

  void onUserPacketReceived(livekit::Room &, const livekit::UserDataPacketEvent & event) override
  {
    std::function<void(const livekit::UserDataPacketEvent &)> callback;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ == livekit::ConnectionState::Disconnected) {
        return;
      }
      callback = callbacks_.on_user_packet_received;
    }

    if (callback) {
      callback(event);
    }
  }

  void onConnectionStateChanged(livekit::Room &, const livekit::ConnectionStateChangedEvent & event) override
  {
    transitionState(event.state);
  }

  void onDisconnected(livekit::Room &, const livekit::DisconnectedEvent & event) override
  {
    LogEvent(kLogger, "room_disconnected").fieldEnum("disconnect_reason", event.reason).warn();
    transitionState(livekit::ConnectionState::Disconnected);
  }

  void onReconnecting(livekit::Room & room, const livekit::ReconnectingEvent &) override
  {
    LogEvent(kLogger, "room_reconnecting").fieldOr("room_sid", room.roomInfo().sid).warn();
    transitionState(livekit::ConnectionState::Reconnecting);
  }

  void onReconnected(livekit::Room & room, const livekit::ReconnectedEvent &) override
  {
    LogEvent(kLogger, "room_reconnected").fieldOr("room_sid", room.roomInfo().sid).info();
    transitionState(livekit::ConnectionState::Connected);
  }

  void onRoomEos(livekit::Room &, const livekit::RoomEosEvent &) override
  {
    LogEvent(kLogger, "room_eos").warn();
    transitionState(livekit::ConnectionState::Disconnected);
  }

  bool registerRpcLocked(const std::string & method)
  {
    auto participant = lockedLocalParticipant(room_);
    const auto it = rpc_handlers_.find(method);
    if (participant == nullptr || it == rpc_handlers_.end()) {
      return true;
    }

    try {
      participant->unregisterRpcMethod(method);
    } catch (const std::exception &) {
      // Re-registering refreshes any SDK-retained callback; absence is harmless.
    }

    try {
      // LiveKit retains this callback independently of rpc_handlers_.
      participant->registerRpcMethod(
        method,
        [method, handler = it->second](const livekit::RpcInvocationData & invocation) -> std::optional<std::string> {
          try {
            return handler(invocation);
          } catch (const livekit::RpcError &) {
            throw;
          } catch (...) {
            LogEvent(kLogger, "rpc_request_failed")
              .field("method", method)
              .fieldOr("request_id", invocation.request_id)
              .fieldOr("requester_identity", invocation.caller_identity)
              .fieldException("error", std::current_exception())
              .error();
            throw livekit::RpcError(protocol::kInternalRpcCode, "Internal error handling RPC method");
          }
        });
    } catch (const std::exception & exc) {
      LogEvent(kLogger, "rpc_method_registration_failed").field("method", method).field("error", exc.what()).error();
      return false;
    }
    return true;
  }

  mutable std::mutex mutex_;
  std::thread connect_task_;

  std::shared_ptr<livekit::Room> room_;
  LiveKitConfig config_;
  RoomEventCallbacks callbacks_;

  std::unordered_map<std::string, livekit::LocalParticipant::RpcHandler> rpc_handlers_;
  // Guards video unpublish against tracks published by an older room.
  std::unordered_map<const livekit::LocalVideoTrack *, std::uint64_t> track_room_generations_;
  // Guards audio unpublish against tracks published by an older room.
  std::unordered_map<const livekit::LocalAudioTrack *, std::uint64_t> audio_track_room_generations_;
  // Bridge-owned mirror of the remote participants' publications, keyed by track sid. Mutated only on
  // the FFI delegate thread (plus the one connect-time seed in activateRoom) and read everywhere else,
  // so every access is serialized by mutex_. Guarded by mutex_.
  RemotePublicationMirror remote_publications_;

  bool stop_requested_ = false;
  bool sdk_initialized_ = false;
  std::uint64_t room_generation_ = 0;
  livekit::ConnectionState state_ = livekit::ConnectionState::Disconnected;
};

}  // namespace

std::unique_ptr<RoomConnection> createRoomConnection()
{
  return std::make_unique<SdkRoomConnection>();
}

}  // namespace livekit_ros2_bridge
