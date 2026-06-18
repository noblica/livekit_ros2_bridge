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

#include "protocol/current_value_json.hpp"

#include <exception>
#include <stdexcept>

#include "nlohmann/json.hpp"
#include "protocol/detail/json_fields.hpp"
#include "protocol/validation_error.hpp"
#include "rclcpp/expand_topic_or_service_name.hpp"
#include "utils/trim.hpp"

namespace livekit_ros2_bridge::protocol::current_value
{

using Json = nlohmann::json;

namespace
{

constexpr char kPayloadField[] = "payload";
constexpr char kKindField[] = "kind";
constexpr char kNameField[] = "name";
constexpr char kResultField[] = "result";
constexpr char kTopicExpansionNodeName[] = "livekit_ros2_bridge";
constexpr char kTopicExpansionNamespace[] = "/";

}  // namespace

CurrentValueRequest parse(const std::string & payload)
{
  Json body;
  try {
    body = detail::parseObject(
      payload, "Invalid JSON in current-value request", "Current-value request must be a JSON object");
  } catch (const std::invalid_argument & exc) {
    throw ValidationError(kPayloadField, exc.what());
  }

  CurrentValueRequest request;

  const auto kind_field = body.find(kKindField);
  if (kind_field == body.end() || !kind_field->is_string()) {
    throw ValidationError(kKindField, "current-value 'kind' must be a string");
  }
  // Only "topic" is implemented; reject every other (including future) kind explicitly so the
  // caller learns immediately rather than receiving a silent `none`.
  if (trim(kind_field->get_ref<const std::string &>()) != "topic") {
    throw ValidationError(kKindField, "current-value 'kind' must be 'topic'");
  }
  request.kind = SubscriptionTargetKind::Topic;

  const auto name_field = body.find(kNameField);
  if (name_field == body.end() || !name_field->is_string()) {
    throw ValidationError(kNameField, "current-value 'name' must be a string");
  }
  try {
    request.name = rclcpp::expand_topic_or_service_name(
      trim(name_field->get_ref<const std::string &>()), kTopicExpansionNodeName, kTopicExpansionNamespace);
  } catch (const std::exception & exc) {
    throw ValidationError(kNameField, exc.what());
  }

  return request;
}

std::string serialize(CurrentValueResult result)
{
  switch (result) {
    case CurrentValueResult::Sent:
      return Json{{kResultField, "sent"}}.dump();
    case CurrentValueResult::None:
      return Json{{kResultField, "none"}}.dump();
  }
  throw std::invalid_argument("current-value result is invalid");
}

}  // namespace livekit_ros2_bridge::protocol::current_value
