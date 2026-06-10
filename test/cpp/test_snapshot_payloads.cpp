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

#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "nlohmann/json.hpp"
#include "protocol/constants.hpp"
#include "protocol/detail/base64.hpp"
#include "protocol/snapshot.hpp"
#include "protocol/snapshot_json.hpp"

namespace livekit_ros2_bridge
{
namespace
{

LatchedSnapshot makeSnapshot()
{
  LatchedSnapshot snapshot;
  snapshot.name = "/map";
  snapshot.interface_type = "nav_msgs/msg/OccupancyGrid";
  snapshot.track_name = "lkros.data.map";
  snapshot.cdr = {0x00, 0x01, 0x02, 0xFF};
  return snapshot;
}

TEST(SnapshotPayloadsTest, SerializeWrapsCdrPayloadAndRoundTripsBytes)
{
  const auto body = nlohmann::json::parse(protocol::snapshot::serialize(makeSnapshot()));

  EXPECT_EQ(body["v"], protocol::kProtocolVersion);
  EXPECT_EQ(body["type"], protocol::kSnapshotTopic);
  EXPECT_EQ(body["kind"], "topic");
  EXPECT_EQ(body["name"], "/map");
  EXPECT_EQ(body["interface_type"], "nav_msgs/msg/OccupancyGrid");
  EXPECT_EQ(body["track_name"], "lkros.data.map");

  ASSERT_TRUE(body["message"].is_object());
  EXPECT_EQ(body["message"]["content_type"], protocol::kCdrContentType);

  const auto decoded = protocol::detail::base64::decode(body["message"]["payload_base64"].get<std::string>());
  ASSERT_EQ(decoded.status, protocol::detail::base64::Status::Ok);
  EXPECT_EQ(decoded.bytes, (std::vector<std::uint8_t>{0x00, 0x01, 0x02, 0xFF}));
}

TEST(SnapshotPayloadsTest, SerializeMatchesExpectedEnvelope)
{
  LatchedSnapshot snapshot;
  snapshot.name = "/map";
  snapshot.interface_type = "nav_msgs/msg/OccupancyGrid";
  snapshot.track_name = "lkros.data.map";
  // Empty CDR still serializes; payload validity is decided by the consuming endpoint.
  snapshot.cdr = {};

  const nlohmann::json expected = {
    {"v", protocol::kProtocolVersion},
    {"type", protocol::kSnapshotTopic},
    {"kind", "topic"},
    {"name", "/map"},
    {"interface_type", "nav_msgs/msg/OccupancyGrid"},
    {"track_name", "lkros.data.map"},
    {"message", {{"content_type", protocol::kCdrContentType}, {"payload_base64", ""}}},
  };

  EXPECT_EQ(nlohmann::json::parse(protocol::snapshot::serialize(snapshot)), expected);
}

}  // namespace
}  // namespace livekit_ros2_bridge
