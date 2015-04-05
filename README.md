kafka-xchange
=============

Kafka producer for data collection from cryptocurrency exchanges

Configuration
-------------

**Environment Variables**

* ```PRODUCER_EXCHANGE```: Exchange that producer will collect data from. Fully qualified name of ```Exchange``` subclass in ```com.xeiam.xchange```.  

Example
-------

        PRODUCER_EXCHANGE='com.xeiam.xchange.bitstamp.BitstampExchange' ./gradlew run

Stop the program using `Ctrl+C`, for now. 
