## Filter plugin : Watermark

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.3.0

### Description

Allows the user to specify the threshold of late data, and allows the engine to accordingly clean up old state.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [event_time](#event_time-string) | string | yes |  |
| [delay_threshold](#delay_threshold-string) | string | yes |  |

##### event_time [string]

The name of the column that contains the event time of the row.

##### delay_threshold [string]

The minimum delay to wait to data to arrive late, relative to the latest record that has been processed in the form of an interval (e.g. "1 minute" or "5 hours").

### Example

```
Watermark {
    event_time = "datetime"
    delay_threshold = "5 minutes"
}
```