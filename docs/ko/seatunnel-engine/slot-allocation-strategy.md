# Slot Allocation Strategy

Slot allocation strategy is an important part of SeaTunnel Engine, which determines how SeaTunnel Engine allocates tasks to different slots. The slot allocation strategy is a configurable component, and users can configure the slot allocation strategy according to their needs.

**Configuration method:**

Set the parameter `slot-allocation-strategy`, optional values are `RANDOM`, `SYSTEM_LOAD`, `SLOT_RATIO`.

Example:

```yaml
seatunnel:
  engine:
    slot-service:
      slot-allocation-strategy: RANDOM
```

## RANDOM (default value)

The random allocation strategy is the default slot allocation strategy of SeaTunnel Engine, which randomly allocates tasks to different slots.

## SYSTEM_LOAD

The system load strategy allocates slots based on the system load, dynamically adjusting the slot allocation according to the system load.

### 1. **Design of time weight**

Time weight reflects the impact of time on scheduling priority:

- Recent data is given higher weight, and historical data gradually decays.

- Using the distribution $4, 2, 2, 1, 1$ and normalizing it, the time weight for each statistic is:

  $$ \text{Time weight ratio} = \frac{\text{Current weight}}{10} $$

> When the cluster is just started and there are less than 5 data points, normalization is done separately, and the calculation formula will be dynamically adjusted, which will not be elaborated here.

### 2. **Resource utilization calculation**

Evaluate the idle rate of CPU and memory resources comprehensively according to the weight:

$$ \text{Resource idle rate} = \frac{(1 - \text{CPU utilization}) \cdot \text{CPU weight} + (1 - \text{Memory utilization}) \cdot \text{Memory weight}}{\text{CPU weight} + \text{Memory weight}} $$

- $(1 - \text{CPU utilization})$ and $(1 - \text{Memory utilization})$ in the formula are idle rates.

- The weights of CPU and memory can be adjusted according to specific needs (e.g., $0.6$ and $0.4$), flexibly adapting to different scenarios.

### 3. **Time decay and scheduling priority formula**

After introducing time weight decay, the formula for calculating scheduling priority is:

$$
\text{Comprehensive resource idle rate} = \sum_{i=1}^{5} \left( \frac{(1 - \text{CPU utilization}_i) \cdot \text{CPU weight} + (1 - \text{Memory utilization}_i) \cdot \text{Memory weight}}{\text{CPU weight} + \text{Memory weight}} \cdot \text{Time weight}_i \right)
$$

### 4. **Dynamic adjustment of resource idle rate for slot allocation**

When allocating multiple slots, considering the real-time update and dynamic simulation of resource status (because the resource load of the same task will not change quickly):

- **Resource ratio used by each slot** = (1 - Comprehensive resource idle rate) ÷ Number of allocated slots

- Update the idle rate of the corresponding node after allocating the slot:

  $$ \text{Idle rate after slot allocation} = \text{Comprehensive resource idle rate} - \text{Resource ratio used by each slot} $$

- By default, a single slot uses 10% of resources (it is not known how much resources a slot occupies when it is first started, so it is set to 10% by default. The reason for not setting it too low is to prevent allocating too many resources and causing the node to be overloaded. The next time monitoring information is captured, it will be relatively accurate).

This method makes scheduling more in line with the actual resource usage.

### 5. **Introduction of balance factor**

Only dynamically adjusting the resource idle rate through slot allocation may also have errors. We introduce a balance factor based on the number of slots to measure the current load status of the node and avoid over-concentration of scheduling resource allocation:

> This number can be counted in real-time to optimize the scheduling priority indicator.

$$
\text{BalanceFactor}_i = 1 - \frac{S_{\text{used},i}}{S_{\text{total},i}}
$$

- $S_{\text{used},i}$: Number of slots allocated to node $i$.
- $S_{\text{total},i}$: Total number of slots of node $i$.

Adjust the scheduling priority through the balance factor:

$$
W_i = \alpha \cdot \text{Idle rate after slot allocation}_i + \beta \cdot \text{BalanceFactor}_i
$$

**Parameter meaning**:
- $\alpha$: Weight focusing on resource utilization: 0.7
- $\beta$: Weight of the balance factor to prevent single-point overload: 0.3

### 6. **Dynamic adjustment logic**

- Collect CPU and memory utilization regularly, maintaining the most recent 5 statistics.
- Dynamically update weights for the same task, gradually decaying old data.
- Dynamic balance based on slot usage.

> Explanation:
> For example, if we have two nodes and need to allocate 10 slots, A has 10 idle slots, and B has 20 idle slots. After calculating the weights of the 10 slots through steps 4 and 5, the weights of node A are higher than those of node B.
> Then we still think that node A should allocate resources. This may be because the slot configuration of node B in the cluster is not optimal (the slot configuration of the worker node is too small).

## SLOT_RATIO

The slot ratio strategy schedules based on the slot usage rate, with higher priority given to slots with lower usage rates.

**Calculation logic**:

1. Get the total number of slots of the worker.
2. Get the number of unallocated slots.
3. Usage rate = (Total number of slots - Number of unallocated slots) / Total number of slots.