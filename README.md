# Bleach

Bleach is distributed stream data cleaning system built on Apache Storm. Unlike other data cleaning systems 
which mainly focus on batch data cleaning, Bleach performs data cleaning directly on data streams without 
waiting for all the data to be acquired. It aims to achieve efficient and accurate qualitative data cleaning 
under real-time constraints. It currently support FD rules and CFD rules. More details can be found in our
[paper](https://arxiv.org/abs/1609.05113#).


## How to run it

First, you need a cluster of machines in which Storm, Kafka and Zookeeper are installed. Next, download Bleach code and compile it by mvn:

    $ git clone git://github.com:ychtian/Bleach
    $ cd Bleach && mvn assembly:assembly

Then, submit the jar to Storm cluster to start Bleach:

    $ storm jar target/bleach-1.0.0-jar-with-dependencies.jar storm.dataclean.TestTopology.TestRepair -config job.conf

All the configuration is included in file job.conf.
