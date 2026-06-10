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

#include <cstdint>
#include <string>
#include <vector>

namespace livekit_ros2_bridge
{

// The most recent message of a latched (`transient_local`) topic, delivered targeted to a
// late-joining subscriber over the `lkros.snapshot` data-packet topic. `name`, `interface_type`,
// and `track_name` mirror the live subscription so the client can route the snapshot onto the
// data track it already listens to.
struct LatchedSnapshot
{
  std::string name;
  std::string interface_type;
  std::string track_name;
  // Raw CDR bytes of the cached message; carried base64-encoded on the wire.
  std::vector<std::uint8_t> cdr;
};

}  // namespace livekit_ros2_bridge
