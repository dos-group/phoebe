# Phoebe: QoS-Aware Distributed Stream Processing through Anticipating Dynamic Workloads

This is the repository associated with the scientific paper of the same name which can be found [here](https://arxiv.org/abs/2206.09679). Contained in this repository is the prototype, data, and experimental artifacts.

## Paper Abstract

Distributed Stream Processing systems have become an essential part of big data processing platforms. They are characterized by the high-throughput processing of near to real-time event streams with the goal of delivering low-latency results and thus enabling time-sensitive decision making. At the same time, results are expected to be consistent even in the presence of partial failures where exactly-once processing guarantees are required for correctness. Stream processing workloads are oftentimes dynamic in nature which makes static configurations highly inefficient as time goes by. Static resource allocations will almost certainly either negatively impact upon the Quality of Service and/or result in higher operational costs.

In this paper we present Phoebe, a proactive approach to system auto-tuning for Distributed Stream Processing jobs executing on dynamic workloads. Our approach makes use of parallel profiling runs, QoS modeling, and runtime optimization to provide a general solution whereby configuration parameters are automatically tuned to ensure a stable service as well as alignment with recovery time Quality of Service targets. Phoebe makes use of Time Series Forecasting to gain an insight into future workload requirements thereby delivering scaling decisions which are accurate, long-lived, and reliable. Our experiments demonstrate that Phoebe is able to deliver a stable service while at the same time reducing resource over-provisioning.

## Repository Structure

The folder repository structure and contents are described as follows:

- analytics: Contains the Pyhthon based analytics engine.
- binaries: Contains (helm)[https://helm.sh/] binary and streaming jobs.
- data: Contains metrics from experimental runs.
- src/main: Contains the phoebe protoype which interacts with the Kubernetes cluster and targeted jobs.
