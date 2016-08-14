# Weather Assistant

The Weather Assistant provides weather forecast scores for over 70,000 cities across the world. The application can
be categorized into the following sections:

  - Stream and archive weather forecasts and current weather conditions for 70,000
  - Deserialize, Transform, and enhance the weather data stream using the Kafka messaging queue and Strom processing engine
  - Store data in Hive and query using Presto "speed of thought" query engine
  - Score cities on forecasting accuracy and visualize using Tableau


### Repository
The repository is organized into folders with name endings that provide hints on what will be found within.
Here are descriptions of the folder name endings used:
  - _Utils: Includes code that was used to test or verify certain functionality of the system  
  - _Scripts: Includes scripts that were used to initialize or configure the system
  - _Attempt: Includes archived code that was not used in the production system (Used for evaluating the system build-out)
  - _Production: Includes the production code that is executable on the configured system

### Architecture
The Weather Evaluator was configured to run on AWS EMR (emr-4.7.1) using the Presto-Sandbox (0.147) configuration.

AWS S3 was configured as the EMRFS (alternative to locally configured HDFS) for storing data in external Hive tables.

Kafka was installed directly onto the EMR instance in a single node configuration. The highest
throughput expected is 3 files per second, perfectly manageable by a single Kafka Node.

Storm was configured as a cluster of 4 EC2 instances: 1 Nimbus node, 1 Zookeeper node, and 2 Supervisor nodes. However, the EMR instance also has storm configured for running the topology in local mode.

### Instructions
1. Log into the master node of the configured EMR instance.

2. Execute the pipeline initialization .jar. This executable drops all weather data (in json format) from S3 archive onto a Kafka topic. The Storm topology is already running and waiting to process incoming files. This execution performs a replay of all captured data. A pattern that is essential to managing a streaming pipeline built on the Kappa architecture fundamentals. 

```sh
$ cd ~/apache-storm-1.0.1/bin/
$ storm jar ~/storm-starter-1.0.2.jar org.apache.storm.starter.WeatherEvaluatorTopology
```