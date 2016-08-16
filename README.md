# Weather Assistant

The Weather Assistant provides weather forecast scores for over 70,000 cities across the world. The application can
be categorized into the following sections:

  - Stream and archive weather forecasts and current weather conditions for 70,000
  - Deserialize, Transform, and enhance the weather data stream using the Kafka messaging queue and Strom processing engine (a working Batch processing version in also included)
  - Store data in Hive and query using Presto "speed of thought" query engine
  - Score cities on forecasting accuracy and visualize using Tableau


### Repository
The repository is organized into folders with name endings that provide hints on what will be found within.
Here are descriptions of the folder name endings used:
  - _Utils: Includes code that was used to test or verify certain functionality of the system  
  - _Scripts: Includes scripts that were used to initialize or configure the system
  - _Attempt: Includes archived code that was not used in the production system (Used for evaluating the system build-out)
  - _Production: Includes the production code that is executable on the configured system
  - _Analysis: Includes the code used to model the data in preparation for visualization through Tableau

Additional information about the contents of each folder can be found in the README file in the folders.

### Architecture
The Weather Evaluator was configured to run on AWS EMR (emr-4.7.1) using the Presto-Sandbox (0.147) configuration.

AWS S3 was configured as the EMRFS (alternative to locally configured HDFS) for storing data in external Hive tables.

Kafka was installed directly onto the EMR instance in a single node configuration. The highest
throughput expected is 3 files per second, perfectly manageable by a single Kafka Node.

Storm was configured as a cluster of 4 EC2 instances: 1 Nimbus node, 1 Zookeeper node, and 2 Supervisor nodes. Howevever, we experienced system configuration issues with the Zookeeper node
and wrote an alternative batch processing version. The Storm cluster and topology was kept in place to show our efforts and ability to manage the project and arrive at a working solution in the face of major challenges.

### Instructions

API Data Pull:
Instructions are included in the README file of the Data_Retrieval_Production folder of this repository

Streaming:
1. Log into the master node of the configured EMR instance.

2. Execute the pipeline initialization .jar. This executable drops all weather data (in json format) from S3 archive onto a Kafka topic. This execution performs a replay of all captured data. A pattern that is essential to managing a streaming pipeline built on the Kappa architecture fundamentals. 

```sh
$ cd ~/mids-205-weather-assistant/Stream_Production/
$ java -jar initiate-stream.jar
```

Batch:
1. The batch processing can be executed from any machine. The executable is already compiled and included in the Batch_Production folder of this repository. 

```sh
$cd ~/mids-205-weather-assistant/Batch_Production/
$ java -jar initiate-batch.jar
```