Java files in this folder were used for pulling weather data and storing the same in Amazon S3.

Execution Steps:

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