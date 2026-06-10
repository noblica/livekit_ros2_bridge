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

#include "protocol/snapshot_json.hpp"

#include "nlohmann/json.hpp"
#include "protocol/cdr.hpp"
#include "protocol/constants.hpp"

namespace livekit_ros2_bridge::protocol::snapshot
{

std::string serialize(const LatchedSnapshot & snapshot)
{
  // Only latched data topics are snapshotted; `kind` mirrors the subscription status vocabulary.
  nlohmann::json body = {
    {"v", protocol::kProtocolVersion},
    {"type", protocol::kSnapshotTopic},
    {"kind", "topic"},
    {"name", snapshot.name},
    {"interface_type", snapshot.interface_type},
    {"track_name", snapshot.track_name},
    {"message", protocol::cdr::serialize(snapshot.cdr)},
  };

  return body.dump();
}

}  // namespace livekit_ros2_bridge::protocol::snapshot
