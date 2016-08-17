Java files in this folder were used for pulling weather data from www.openweathermap.org and storing the same in Amazon S3.

All the necessary keys and URL are configured in the codebase. 

Following are the steps for executing the code base.

1. These files will have to be run a linux server with Java 1.7 installed

2. Create a directory by name weather in the root directory (/weather). It is important to creat under the root folder.

3. Copy code and data directories under weather folder

4. Change /weather director permission to 744 (chmod -R 744 /weather).

5. Make /weather/code/run_weather.sh executable (chmod +x run_weather.sh)

5. Change directory to /weather/code 

5. Run run_weather.sh

6. Data pulled will be stored in data folder as well as copied to S3



Below are the some of the configuration information. This for information only.

1. Configuration

Update the following parameters in weather.properties file

#AWS keys will go here
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

#Data retrieved will be stored here
WORKING_DIRECTORY=

#This directory should point to the config CSV folder (resources directory)
CONFIG_DIRECTORY=

#Open Weather
OPEN_WEATHER_API_KEY=


#Email config
smtp.userName=
smtp.password=
stmp.host=
stmp.port=
smtp.auth=
smtp.starttls.enable=
adminEmail=

#Kafka Config (only if Kafka queue is used)
bootstrap.servers={Kafka Sever}
acks=all
retries=0
batch.size=16384
auto.commit.interval.ms=1000
linger.ms=0
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
block.on.buffer.full=true
timeout.ms=10000

2. Scheduling using CRON
Set up cRON to trigger weather.sh. This for Linux server.

3. Pulling data for the current hour - run weather.sh