Sample code to reproduce Flink checkpointing problem with AsyncDataStream.

The second task of the first checkpoint will sit in "start delay" until the
job is finished, at which point the useless final checkpoint will be taken 
and then discarded because the job is finished.

Tested using the docker-compose.yml file in this directory. Flink version is 1.15.0

