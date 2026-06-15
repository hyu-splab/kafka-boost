# Kafka-Boost

This document describes the features added to the existing Apache Kafka broker in the paper *"Kafka-Boost: An Adaptive
Service Boosting Data Streaming Platform for V2X In Edge"*.

To view the original Kafka documentation, please refer to
[`KAFKA_README.md`](KAFKA_README.md).

## Table of Contents
- [Key Components](#key-components)
  - [1. Dedicated Processors](#1-dedicated-processors)
  - [2. Client Boost Manager (for Static Client Boost)](#2-client-boost-manager-for-static-client-boost)
  - [3. DynamicChannelBoostManager (for Dynamic Channel Boost)](#3-dynamicchannelboostmanager-for-dynamic-channel-boost)
  - [4. Monitoring and Telemetry](#4-monitoring-and-telemetry)
- [Newly Added Configurations](#newly-added-configurations)
- [How to Reproduce Evaluations](#how-to-reproduce-evaluations)

## Key Components

To achieve adaptive service boosting, Kafka-Boost introduces two primary mechanisms: **Static Client Boost** and **Dynamic
Channel Boost**. These features are realized through several architectural enhancements within the Kafka broker, as detailed below.

### 1. Dedicated Processors
In addition to the standard processors (network threads), the broker now supports **Dedicated Processors**. These
processors are specifically reserved for handling channels that have been granted a "boost" ensuring low latency and
high throughput for critical services.

Dedicated processors are assigned to channels through two distinct boosting mechanisms:
*   **Static Client Boost**: Administrators can explicitly register specific `clientId`s in advance using the Admin Client.
*   **Dynamic Channel Boost**: The broker automatically detects the traffic load on each channel and dynamically registers
    or unregisters the boost based on real-time demand.

### 2. Client Boost Manager (for Static Client Boost)
The `ClientBoostManager` is responsible for managing the mapping between registered `clientId`s and their assigned Dedicated
Processors. It does not run as a separate thread. Instead, standard processors utilize this manager to identify and
reassign channels originating from specific client IDs to Dedicated Processors. Furthermore, it utilizes the
**Spare Processor Pattern** to ensure fast and efficient allocation of processors to boosted clients.

#### Explicit Admin CLI Control
Administrators retain manual control over the boosting mechanism via the `AdminClient` and a newly provided CLI tool.
This is particularly useful for forcing specific critical V2X services to use dedicated network paths.
*   **Admin API**: `Admin.registerClientBoost(clientId)` and `Admin.unregisterClientBoost(clientId)` methods have been
    added to the Java client for programmatic control.
*   **CLI Tool**: The commands shown below allow administrators to explicitly register or unregister clients for the boost feature.
    ``` shell
    # register
    bin/kafka-client-boost.sh --register --client-id <id1> --client-id <id2> ...
    
    # unregister
    bin/kafka-client-boost.sh --unregister --client-id <id1> --client-id <id2> ...
    ```

### 3. DynamicChannelBoostManager (for Dynamic Channel Boost)
The `DynamicChannelBoostManager` is the core auto-scaling engine driving the dynamic feature. It runs as a background
thread (`kafka-socket-boost-manager-*`) and periodically evaluates the state of all active socket channels.

#### Auto-Scaling Mechanism
*   **Stat Collection**: Maintains a sliding window of processed request statistics (e.g., TPS) for both normal channels
    and currently boosted channels.
*   **Boost (Promotion)**: At every check interval, the manager identifies channels in the normal pool that are "fully
    saturated" (exceeding capacity thresholds over a window of time). The single channel with the highest request rate is
    selected and dynamically reassigned to an idle Dedicated Processor.
*   **Unboost (Demotion)**: If the request rate of a boosted channel falls below a dynamically calculated threshold
    (`returnThreshold`) for a sustained period (idle window), the manager migrates the channel back to the normal network
    thread, freeing up the dedicated thread for other potential spikes.

#### Thrashing Prevention
To prevent channels from endlessly ping-ponging between normal and dedicated threads, the manager employs several cooldown mechanics:
*   `globalReassignCooldown`: A global pause before the manager is allowed to execute another channel migration.
*   `channelReassignCooldown`: A channel-specific cooldown ensuring a channel stays in its new path for a minimum
    duration to accumulate stable statistics.
*   `newChannelCooldown`: Prevents brand-new connections from being boosted immediately, absorbing initial connection traffic surges.

### 4. Monitoring and Telemetry
To effectively evaluate and monitor throughput and latency, a new interceptor has been integrated.
*   **LogProduceRequestStatInterceptor**: This interceptor traces `ProduceRequest` metrics. It records both the throughput
    and request-fetching interval statistics into `channel-requests.log`, allowing developers to closely monitor channel
    performance and the effectiveness of the boosting mechanism.

## Newly Added Configurations
The feature introduces several new broker configurations to tune the sensitivity and behavior of the boosting engine:

| Configuration | Default | Description |
| :--- | :--- |:---|
| `num.dedicated.threads` | 3 | Number of dedicated threads reserved for **Dynamic Channel Boost**. |
| `booster.check.interval.ms` | 10 | Frequency of the dynamic channel boost manager's evaluation loop. |
| `booster.saturation.window.size` | 10 | The number of samples needed to confirm a channel is saturated and requires boosting. |
| `booster.idle.window.size` | 10 | The number of samples needed to confirm a boosted channel is idle and should be demoted. |
| `booster.requests.stat.window.size` | 100 | The history window size for calculating a channel's average request rate. |
| `booster.global.reassign.cooldown.ticks` | 10 | Ticks to wait between any channel reassignment operations. |
| `booster.channel.reassign.cooldown.ticks`| 100 | Ticks a specific channel must wait before being moved again. |
| `booster.new.channel.cooldown.ticks` | 10 | Ticks to ignore newly connected channels. |

## How to Reproduce Evaluations
You can reproduce the evaluations presented in the paper using this project along with the official testbed repository:
[https://github.com/hyu-splab/kafka-boost-testbed](https://github.com/hyu-splab/kafka-boost-testbed)

In our experiments, all configurations were kept at their default values except for `num.dedicated.threads`,
which was configured according to the specific requirements of each evaluation as described in the paper.

For detailed performance metrics, check the `channel-requests.log` generated by the `LogProduceRequestStatInterceptor`.