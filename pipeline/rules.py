"""Business rules shared by both silver engines. Reason order is the order rules are applied."""

BOOKING_STATUSES = ("CONFIRMED", "CANCELLED", "CHECKED_IN", "FLOWN")
LOYALTY_TIERS = ("NONE", "BLUE", "SILVER", "GOLD")

# A row that breaks several rules is reported under the first one that applies.
BOOKING_REASONS = (
    "malformed_value",
    "missing_passenger_id",
    "negative_fare",
    "invalid_status",
    "unknown_route",
)
PASSENGER_REASONS = ("malformed_value", "missing_passenger_id", "invalid_loyalty_tier")
