kafka-xchange
=============

Kafka producer for data collection from cryptocurrency exchanges

Configuration
-------------

**Environment Variables**

* ```PRODUCER_EXCHANGE```: Exchange that producer will collect data from. Fully qualified name of ```Exchange``` subclass in ```com.xeiam.xchange```.  

**config.properties**

This is for Kakfa producer configuration options. Available options are [here](http://kafka.apache.org/documentation.html#producerconfigs).

Example
-------

        PRODUCER_EXCHANGE='com.xeiam.xchange.bitstamp.BitstampExchange' ./gradlew run

Stop the program using `Ctrl+C`, for now. 
