# Reflection

The hardest faults to catch were the subtle AI infrastructure faults, especially feature skew and embedding drift. Their measurements can sit close to the clean-stream range, so a threshold that catches more faults can also create false alarms. Lineage faults also need an expected graph to compare against; the public interface gives the observed upstream edges but does not publish a canonical expected-upstream set in the event payload.

I used one metered measurement per event and compared the result with the published baseline. This kept the private run within its 320-credit budget and produced no false positives. The tradeoff was lower recall: the private result was TPR 53.7%, FPR 0%, with a score of 26.85. Practice and Public were useful for confirming the interface and obvious fault checks, but their stronger fault signals did not transfer fully to the subtler private stream.

With more time, I would use `ctx.state` to maintain robust per-event-type running summaries of clean-looking measurements, such as medians and median absolute deviations, and combine them with the fixed baselines. I would validate any adaptive thresholds on multiple streams to avoid learning from faults or overfitting one phase. I would also investigate whether the public lineage metadata can provide a general expected-upstream rule before adding dependency checks.
