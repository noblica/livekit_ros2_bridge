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

#include <optional>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "utils/concurrency_limiter.hpp"

namespace livekit_ros2_bridge
{
namespace
{

using Reservation = PerKeyConcurrencyLimiter::Reservation;

TEST(PerKeyConcurrencyLimiterTest, ReservesUpToMaxThenRejects)
{
  PerKeyConcurrencyLimiter limiter(2);

  auto first = limiter.tryReserve("client");
  auto second = limiter.tryReserve("client");
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());

  // The third concurrent reservation for the same identity is rejected.
  EXPECT_FALSE(limiter.tryReserve("client").has_value());
}

TEST(PerKeyConcurrencyLimiterTest, ReleasingAReservationFreesASlot)
{
  PerKeyConcurrencyLimiter limiter(1);

  {
    auto reservation = limiter.tryReserve("client");
    ASSERT_TRUE(reservation.has_value());
    EXPECT_FALSE(limiter.tryReserve("client").has_value());
  }

  // The scoped reservation released on destruction, so a slot is free again.
  EXPECT_TRUE(limiter.tryReserve("client").has_value());
}

TEST(PerKeyConcurrencyLimiterTest, IdentitiesAreIndependent)
{
  PerKeyConcurrencyLimiter limiter(1);

  auto first_client = limiter.tryReserve("client-a");
  ASSERT_TRUE(first_client.has_value());

  // A different identity has its own quota and is unaffected by client-a's slot.
  auto second_client = limiter.tryReserve("client-b");
  EXPECT_TRUE(second_client.has_value());
  EXPECT_FALSE(limiter.tryReserve("client-a").has_value());
}

TEST(PerKeyConcurrencyLimiterTest, MovingAReservationTransfersOwnershipAndReleasesOnce)
{
  PerKeyConcurrencyLimiter limiter(1);

  auto reservation = limiter.tryReserve("client");
  ASSERT_TRUE(reservation.has_value());

  // Moving the held reservation out of the optional must not release the slot...
  Reservation moved = std::move(*reservation);
  EXPECT_FALSE(limiter.tryReserve("client").has_value());

  // ...the moved-to reservation still owns the single slot until it is destroyed.
  {
    Reservation owner = std::move(moved);
    EXPECT_FALSE(limiter.tryReserve("client").has_value());
  }

  // After the owning reservation is destroyed exactly once, the slot is free.
  EXPECT_TRUE(limiter.tryReserve("client").has_value());
}

TEST(PerKeyConcurrencyLimiterTest, SlotIsReleasedEvenAfterTheLimiterIsDestroyed)
{
  // The shared state must outlive the limiter so a reservation carried onto a
  // detached worker thread can release safely after its owner is gone.
  std::optional<Reservation> reservation;
  {
    PerKeyConcurrencyLimiter limiter(1);
    reservation = limiter.tryReserve("client");
    ASSERT_TRUE(reservation.has_value());
  }

  // Destroying the reservation here touches the shared state, not the dead limiter.
  EXPECT_NO_FATAL_FAILURE(reservation.reset());
}

}  // namespace
}  // namespace livekit_ros2_bridge
