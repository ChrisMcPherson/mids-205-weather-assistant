import sys
from kafka import KafkaConsumer

def main():
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('weather_data',
                             bootstrap_servers=['ec2-52-91-56-164.compute-1.amazonaws.com:9092'],
                             group_id='json-parse',
                             value_deserializer=lambda m: json.loads(m.decode('ascii')))
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
        i = i +1
        if i == 100:
            break
    # consume earliest available messages, dont commit offsets
    #KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # consume json messages
    #KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

if __name__ == "__main__":
    sys.exit(int(main() or 0))