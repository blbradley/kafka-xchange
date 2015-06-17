kafka-xchange
=============

Kafka producer for data collection from cryptocurrency exchanges

Configuration
-------------

**config.properties**

This is for Kakfa producer configuration options. Available options are [here](http://kafka.apache.org/documentation.html#producerconfigs).

Running locally
---------------

With a Kafka server running locally:

        ./gradlew run

Stop the program using `Ctrl+C`, for now. 

Running from distribution
-------------------------

        bin/kafka-xchange
