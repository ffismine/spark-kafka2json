## spark-kafka-json



The main thing completed: parsing Kafka data into a structured dataset.


The topic data in Kafka consists of four parts: offset, key, message, and timestamp.

The message data is read through *spark.read().format("kafka")*, 
but the message data is as follows: 

{"value":"{\"id\":\"1\",\"float_num\":0.1234567}"}

it cannot be directly stored as a dataset.

A feasible solution is to store it as a json file, and then convert it into a dataset through *spark.read().json*