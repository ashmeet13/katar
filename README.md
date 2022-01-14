# Katar

A very elementary data streaming framework in pure Python.


## But, why. What's the motivation?

This is a learning project. A few of the goals I plan on achieving through this project - 


1. Better documentation and specing out a project well before starting.
2. This project is heavily inspired by Apache Kafka. I plan to understand more of how data streaming works and implement a few pieces of them on my own.  


## Quick Note

Everything in this doc is a work in progress. The document may change as I experiment and try to understand more of the underlying systems for a data streaming framework.


## Katar’s Basic Blocks
1. There will be a `producer` - `producer` here is a person/service who is accumulating data and wants to push it into Katar.
2. There is going to be a `consumer` - `consumer` here is a person/service who wants to get the data that is being stored in Katar.
3. Katar will have a storage `engine` - the Katar storage `engine` which will be used by the `producer` and `consumer` for managing their ordered data. The `engine` will be made up of several `topics`
4. A `topic` is nothing but a queue. The `producer` will be sending the topic name it wants to write the message to. The `engine` will write the message to that specific `topic`. A `consumer` will choose the `topic` it wants to receive data from.


## Katar’s Broker

Katar will be a Dumb Broker.
By dumb broker - Katar's broker will not be responsible for storing any state information with respect to either the producer or consumer.


- Katar's producers will be responsible for pushing the data with all parameters set by the Producer itself.
- Coming to Consumer, the pull pattern will be followed in which the control is with consumer to get the data from the engine. To elaborate - the consumer should remember/keep a state of the last retrieved message and send that to Katar to continue streaming data.


## Katar v1.0.0 Limitations

A few things which I will not be trying to implement till I have figured out and implemented the basics for a data streaming framework.


1. Katar will be I/O heavy and network heavy to start with. 
    Kafka is known for its efficiency which it solves by reducing I/O operations. Each message in Katar will be sent one at a time to the broker. Messages will be stored on disk one at a time. Each message will be sent to the Consumer one at a time.


    Kafka uses batching to solve for less disk writes and better use of network requests. Messages are buffered in memory for better reads and hence solving for less disk reads. This is something I plan to solve once I have most of the basic pieces in place. 
    
2. Katar will be a single broker framework to start with.
    Kafka has solved data persistence and recovery by being a replication first system. It has also solved efficiency using partitions. Both of these pieces require work on building multi-broker systems which I am yet to dwell into and will pick up once the basic pieces are in place.
    
## Katar v1.0.0 Target

I will be explaining here the complete storage engine here and how exactly Katar plans on storing data within it. This is most likely a very naive way to design a streaming framework hence suggestions and improvements are welcome!

**How will the data be stored?**

The idea is mostly similar to how Kafka does it and will try my best to explain it here. I’ll explain the engine in a top-down approach. The following pieces will be explained - 

1. Directory Structure of Katar
2. What all files does a Katar topic have?
3. Log Segmentation of the topic files
4. What makes a Katar log?
5. How is data being stored in the topic files

Lets get to it.

**Directory Structure of Kafka**

All the data will be stored in the home Katar directory marked by the environment variable `KATAR_DIR`. This path defaults to `~/.katar`. The tree representation of the directory would be something like - 


    .
    ├── katar.log
    ├── sample_topic_1
    │   ├── 0.index
    │   ├── 0.katar
    │   └── 0.timeindex
    └── sample_topic_2
        ├── 0.index
        ├── 0.katar
        └── 0.timeindex

There will be a folder each within `KATAR_DIR` representing each of the exiting topics.
`katar.log` is the file that saves the logging for the running broker. This will be used for debugging.

**What all files does a Katar topic have?**

Okay, now that we have the directory structure fixed let's look at what files does the topic require to work? As you can see from the above directory tree we have three files that are required for katar to work. These files together make one log segment.


1. `0.katar` - This is the file where the logs would be stored that the producer wants to save.
2. `0.index` - This file maintains a map from the offset of the log (think - unique id) to the memory location where the log is saved in the `0.katar` file. Think of this as a hashmap on disk.
3. `0.timeindex` - This file similar to `0.index` maintains a map to the location of the log saved in `0.katar` but the key used here is a timestamp so that querying from a certain time can be allowed for the consumer.

**Log Segmentation of the topic files**

As I mentioned above that the above three files together make one log segment, I will explain what a log segment is here -
The file saving the logs will have a max size limit. This is to allow for quicker search times using the offset in the index files. The default size for the file will be 1GB.

Once the file reaches the max size set - Katar will create a new segment of files. The name of this segment will be the value of the first offset in this segment of files. Since offset values start at 0 the first three files were named - 


    0.katar
    0.index
    0.timeindex

 
If the offset of the last log saved in this segment of files is 56 and we reach the max capacity - the following three files will be created and logs would be written to these files.


    57.katar
    57.index
    57.timeindex

**What makes a Katar log?**

A log that is stored in Katar is made up of two pieces - the log record itself and the other size of the log record in bytes. The log record will be a python dictionary serialized as string and converted to bytes for storage. The length of this serialized byte string is what makes the other piece in the log i.e. size of the log record in bytes.

A log record would be roughly of the following format - 


    {
        "base_offset" : int,
        "offset" : int,
        "timestamp" : int,
        "payload" : <record>
    }


- What is the `base_offset`?
    It’s the value of the first offset in the segment of files. For example in the above two segments mentioned - the first segment would have the base offset 0 while the second segment will have the base offset 57.
    
- What is the `offset` then?
    Rather than storing an always incrementing offset - Katar will use the `base_offset` as an anchor and use that to calculate the relative offset for logs entering the segment. For example,


    true_offset = 57, base_offset = 57, offset = 0
    true_offset = 58, base_offset = 57, offset = 1
    true_offset = 59, base_offset = 57, offset = 2
    true_offset = 60, base_offset = 57, offset = 3
    true_offset = 61, base_offset = 57, offset = 4


    This is being done so that the offset in the segment can always be limited to a certain byte size which makes consistent entries in the index files for easy reading.
    
- What is the `timestamp`?
    Timestamp is the time at which the record hits the broker.

**How is data being stored in the topic files**

The log records in `.katar` file are saved in the following format -


    <length1><record1><length2><record2>.....<length><record>

In the above pseudo example - `<length>` is 4 bytes of data marking how many next bytes to read so as to get a complete record log. For example if `<length>` has the value 100, then the next 100 bytes will be required to get the `<record>`

The data in the `.index` file will be stored in the following format - 


    <offset1><location1><offset2><location2>.....<offset><location>

Every entry in the index file is an 8 byte entry. The first 4 bytes mark the `offset` (Note - This is the relative offset and not `base_offset`) and the next 4 bytes store the location the file reader should seek to find the `<length>` of the required `<record>`.

The index file will not maintain an index for every record. Rather it would be storing index for every `X` bytes of data (configurable) inserted to the `.katar` file. A modified binary search will be used to get to the closest index in the file and then continue a linear search from there.


## API Design

The engine would expose the following endpoints to support the producers and consumers


1. `GET /topic` - Returns all existing topics. Will only be returning names of the existing topics
    
2. `GET /topic/{topic_name}` - Returns the configuration set for all topics passed as parameters
    
3. `POST /topic` - Creates a topic with the configuration sent as a body. Is not idempotent i.e. if a request to create a new topic with the same name comes a topic resource would be created but with an extension in the name.
    Example - If topic with name `sample` exists and another request for creating a topic with name `sample` comes a topic would be created with name `sample__0`
    
4. `PUT /topic/{topic_name}` - Will reset an existing topics configuration with the parameters sent. The method expects a complete configuration body as it will reset the configuration. If only parts of the configuration are sent the engine will reset the remaining settings to the default settings.
    
5. `PATCH /topic/{topic_name}` - Will update an existing topics singular or multiple configuration i.e. will not be a hard reset rather a single setting update
    
6. `DELETE /topic/{topic_name}` - Will delete all data related to a topic
    
7. `POST /publish` - Will publish data to topic(s)
    
8. `POST /subscribe` - Will subscribe the client to the topic(s)

