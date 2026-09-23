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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "livekit/remote_track_publication.h"
#include "livekit/room_event_types.h"
#include "livekit/track.h"
#include "room_connection.hpp"
#include "room_connection/remote_publication_mirror.hpp"

namespace livekit_ros2_bridge
{

// One SDK remote-track event as plain data; each part is optional because the SDK may pass nulls.
struct RemoteTrackEventInput
{
  struct MediaTrack
  {
    std::shared_ptr<livekit::Track> handle;
    std::string track_sid;
    livekit::TrackKind track_kind = livekit::TrackKind::KIND_UNKNOWN;
  };

  std::optional<std::string> participant_identity;
  std::optional<RemotePublicationMirror::Record> publication;
  // Set only on subscribed/unsubscribed events.
  std::optional<MediaTrack> track;
};

enum class SubscriptionAction
{
  // The mirrored publication is already in the requested state; no SDK request is needed.
  kNone,
  // No mirrored publication (or no handle) to send the request through.
  kUnavailable,
  // Send setSubscribed() through `publication`.
  kRequest,
};

struct SubscriptionPlan
{
  SubscriptionAction action = SubscriptionAction::kNone;
  std::shared_ptr<livekit::RemoteTrackPublication> publication;
};

// SdkRoomConnection's remote-track decisions, free of SDK calls so they can be unit tested.
// Between beginSeed() and completeSeed() every mirror change is also logged, and completeSeed()
// replays the log over the connect-time seed. Not locked: the caller holds its mutex_.
class RemoteTrackRouter
{
public:
  using Record = RemotePublicationMirror::Record;
  using Entry = RoomConnection::RemoteTrackSnapshotEntry;

  void beginSeed()
  {
    replay_log_.clear();
    seeding_ = true;
  }

  void completeSeed(const std::vector<Record> & seed)
  {
    mirror_.clear();
    (void)mirror_.merge(seed);
    for (const auto & change : replay_log_) {
      change(mirror_);
    }
    replay_log_.clear();
    seeding_ = false;
  }

  void clear()
  {
    mirror_.clear();
    replay_log_.clear();
    seeding_ = false;
  }

  // Merges in every state; forwards only while Connected. `participant_publications` is read only
  // for the v1.6.0 null-publication case.
  std::vector<RemoteTrackEvent> trackPublished(
    const RemoteTrackEventInput & input,
    const std::function<std::vector<Record>()> & participant_publications,
    livekit::ConnectionState state)
  {
    if (!input.participant_identity.has_value()) {
      return {};
    }

    std::vector<RemoteTrackEvent> events;
    if (input.publication.has_value() && !input.publication->entry.track_sid.empty()) {
      const auto & entry = input.publication->entry;
      (void)merge({*input.publication});
      events.push_back(makeEvent(*input.participant_identity, entry.track_sid, entry.track_name, entry.track_kind));
    } else {
      // livekit/client-sdk-cpp v1.6.0 room.cpp kTrackPublished move()s the publication into the
      // participant's map and then assigns the moved-from (null) shared_ptr to event.publication, so
      // the event carries no sid or name. The hydrated publication is already stored on the
      // participant, so recover every publication this identity has not seen yet. This deliberately
      // leaks no track names into the connection layer: the manager still applies the exact-name gate.
      for (const auto & newly_added : merge(participant_publications())) {
        events.push_back(makeEvent(
          newly_added.participant_identity, newly_added.track_sid, newly_added.track_name, newly_added.track_kind));
      }
    }

    if (state != livekit::ConnectionState::Connected) {
      return {};
    }
    return events;
  }

  std::optional<RemoteTrackEvent> trackUnpublished(const RemoteTrackEventInput & input, livekit::ConnectionState state)
  {
    if (input.publication.has_value() && !input.publication->entry.track_sid.empty()) {
      erase(input.publication->entry.track_sid);
    }
    return translate(input, state);
  }

  std::optional<RemoteTrackEvent> trackSubscribed(const RemoteTrackEventInput & input, livekit::ConnectionState state)
  {
    if (input.participant_identity.has_value() && input.publication.has_value()) {
      auto record = *input.publication;
      record.entry.participant_identity = *input.participant_identity;
      record.entry.subscribed = true;
      (void)merge({record});
    }
    return translate(input, state);
  }

  std::optional<RemoteTrackEvent> trackUnsubscribed(const RemoteTrackEventInput & input, livekit::ConnectionState state)
  {
    if (input.publication.has_value() && !input.publication->entry.track_sid.empty()) {
      setSubscribed(input.publication->entry.track_sid, false);
    }
    return translate(input, state);
  }

  // Returns whether the disconnect is forwarded: only while Connected, since SDK reconnects suppress
  // transient disconnects. The participant's publications leave the mirror in every state.
  bool participantDisconnected(const std::string & participant_identity, livekit::ConnectionState state)
  {
    eraseIdentity(participant_identity);
    return state == livekit::ConnectionState::Connected && !participant_identity.empty();
  }

  // Reads only the mirror, never the SDK's map.
  SubscriptionPlan planSetSubscribed(const std::string & track_sid, bool subscribed) const
  {
    const auto record = mirror_.find(track_sid);
    if (!record.has_value()) {
      return SubscriptionPlan{SubscriptionAction::kUnavailable, nullptr};
    }
    if (record->entry.subscribed == subscribed) {
      return SubscriptionPlan{SubscriptionAction::kNone, nullptr};
    }
    if (record->handle == nullptr) {
      return SubscriptionPlan{SubscriptionAction::kUnavailable, nullptr};
    }
    return SubscriptionPlan{SubscriptionAction::kRequest, record->handle};
  }

  // Records a completed setSubscribed() request.
  void markSubscribed(const std::string & track_sid, bool subscribed)
  {
    setSubscribed(track_sid, subscribed);
  }

  std::vector<Entry> snapshot() const
  {
    return mirror_.snapshot();
  }

private:
  using MirrorChange = std::function<void(RemotePublicationMirror &)>;

  static RemoteTrackEvent makeEvent(
    std::string participant_identity, std::string track_sid, std::string track_name, livekit::TrackKind track_kind)
  {
    RemoteTrackEvent remote_event;
    remote_event.participant_identity = std::move(participant_identity);
    remote_event.track_sid = std::move(track_sid);
    remote_event.track_name = std::move(track_name);
    remote_event.track_kind = track_kind;
    return remote_event;
  }

  // Unpublished/subscribed/unsubscribed events forward in every state but Disconnected.
  static std::optional<RemoteTrackEvent> translate(const RemoteTrackEventInput & input, livekit::ConnectionState state)
  {
    if (state == livekit::ConnectionState::Disconnected) {
      return std::nullopt;
    }

    RemoteTrackEvent translated;
    if (input.participant_identity.has_value()) {
      translated.participant_identity = *input.participant_identity;
    }
    if (input.publication.has_value()) {
      translated.track_sid = input.publication->entry.track_sid;
      translated.track_name = input.publication->entry.track_name;
      translated.track_kind = input.publication->entry.track_kind;
    }
    if (!input.track.has_value()) {
      return translated;
    }
    if (translated.track_sid.empty()) {
      translated.track_sid = input.track->track_sid;
      translated.track_kind = input.track->track_kind;
    }
    translated.track = input.track->handle;
    return translated;
  }

  void apply(const MirrorChange & change)
  {
    if (seeding_) {
      replay_log_.push_back(change);
    }
    change(mirror_);
  }

  std::vector<Entry> merge(const std::vector<Record> & records)
  {
    if (seeding_) {
      replay_log_.push_back([records](RemotePublicationMirror & mirror) { (void)mirror.merge(records); });
    }
    return mirror_.merge(records);
  }

  void erase(const std::string & track_sid)
  {
    apply([track_sid](RemotePublicationMirror & mirror) { mirror.erase(track_sid); });
  }

  void eraseIdentity(const std::string & participant_identity)
  {
    apply([participant_identity](RemotePublicationMirror & mirror) { mirror.eraseIdentity(participant_identity); });
  }

  void setSubscribed(const std::string & track_sid, bool subscribed)
  {
    apply([track_sid, subscribed](RemotePublicationMirror & mirror) { mirror.setSubscribed(track_sid, subscribed); });
  }

  RemotePublicationMirror mirror_;
  bool seeding_ = false;
  // Every mirror change since beginSeed(), in delivery order.
  std::vector<MirrorChange> replay_log_;
};

}  // namespace livekit_ros2_bridge
