# THIS PROJECT IS NO LONGER MAINTAINED

## Please see [Kafka Cryptocoin](https://github.com/blbradley/kafka-cryptocoin) for the replacement.


kafka-xchange
=============

Kafka producer for data collection from cryptocurrency exchanges

![Travis CI Status](https://travis-ci.org/blbradley/kafka-xchange.svg)

Configuration
-------------

###config.properties

This is the main configuration for kafka-xchange.

**exchanges.active**

Comma-separated list of exchanges to pull data from. Available exchange names:

* `bitstamp`
* `bitfinex`
* `coinbase`

###producer.properties

This is for Kakfa producer configuration options. Available options are [here](http://kafka.apache.org/documentation.html#producerconfigs).

Running locally
---------------

With a Kafka server running locally:

        ./gradlew run

Stop the program using `Ctrl+C`, for now. 

Running from distribution
-------------------------

        bin/kafka-xchange

Development
-----------

###Eclipse project files

        # Generate Eclipse files
        ./gradlew eclipse

        # Clean Eclipse files
        ./gradlew cleanEclipse

More about this [here](https://docs.gradle.org/2.3/userguide/eclipse_plugin.html).
