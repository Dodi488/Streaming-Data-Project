# Feature Dictionary

This document defines the engineered features stored in the `curated.features` MongoDB collection.

| Feature Name                | Data Type | Description                                                                                    |
|-----------------------------|-----------|------------------------------------------------------------------------------------------------|
| `event_id`                  | String    | The unique identifier for the event, used to join with `curated.events`.                         |
| `entity_id`                 | String    | The identifier for the entity that generated the event.                                        |
| `event_timestamp`           | ISODate   | The standardized timestamp of the event.                                                       |
| `rolling_mean_3`            | Float     | [cite_start]The rolling mean of the `event_value` for the entity over the last 3 events. [cite: 74]            |
| `rolling_std_3`             | Float     | [cite_start]The rolling standard deviation of the `event_value` for the entity over the last 3 events. [cite: 74] |
| `rate_of_change`            | Float     | [cite_start]The difference in `event_value` between the current and the previous event for the same entity. [cite: 77] |
| `time_since_last_event_sec` | Float     | [cite_start]The time in seconds that has passed since the last event from the same entity. [cite: 75]            |
| `is_high_activity`          | Boolean   | [cite_start]A flag that is `true` if the `event_value` is 50% greater than the 3-event rolling mean. [cite: 78] |
