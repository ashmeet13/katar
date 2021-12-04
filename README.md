# Katar
A very elementary data streaming framework in pure Python


### Let's start with what will Katar have? The big chunks

1. There will be a producer - producer here is a person/service who is accumulating data and wants to push it into a `queued datastore`
2. There is going to be a consumer - consumer here is a person/service who wants the data being accumulated in the `queued datastore`
3. Finally the `Queued Datastore` - the storage engine which will be used by the producer and consumer for managing their ordered data. Naming this Katar Engine.


### A few questions to answer

1. Will it be a Push or Pull System?
    
    The general paradigm for producers in a streaming framework is majorly a Push system i.e. the producers will be pushing their data to the engine. Katar will follow the same pattern.
    
    Coming to Consumer there are pattern is different. In a push pattern the engine would control the sending of data to the consumer. While in the pull pattern the control is with consumer to get the data from the engine. Katar will most likely 
    
2. Will there be replication of data?
    
    To start with no. But I do wish to support over time and hence will be exploring writing internal replication services and using an external framework such as Zookeeper
    

## Engine

This is how the streaming data storage would work in the Katart Engine.

Disclaimer - This is a very preliminary design and could be subject to change as I learn better ways to implement the systems.

### How will the data be stored? [WIP]

The idea is mostly similar to how Kafka does it and will try my best to explain it here. The file storage
engine is going to have two sets of files. Namely - `index.katar` and `logs.katar`.

The `index.katar` file would be storing mapping from a value called the `offset` to the byte location of
the message of that specific offset in the `logs.katar` file. `log.katar` would be simple in a way that it would be an append only file storing two data points coupled togeather. `<N><M>`, where N is the length in bytes of the message M.

A sample of both files is shown below

`index.katar`
```
00001 B1
00002 B2
00003 B3
```

`logs.katar`
```
N1M1N2M2N3M3
B1  B2  B3    <-- Byte locations
```


## API Design

The engine would expose the following endpoints to support the producers and consumers

- `GET /topic`

Returns all existing topics. Will only be returning names of the existing topics

- `GET /topic/{topic_name}`

Returns the configuration set for all topics passed as parameters

- `POST /topic`

Creates a topic with the configuration sent as a body. Is not idempotent i.e. if a request to create a new topic with the same name comes a topic resource would be created but with an extension in the name.

Example - If topic with name `sample` exists and another request for creating a topic with name `sample` comes a topic would be created with name `sample__0`

- `PUT /topic/{topic_name}`

Will reset an existing topics configuration with the parameters sent. The method expects a complete configuration body as it will reset the configuration. If only parts of the configuration are sent the engine will reset the remaining settings to the default settings.

- `PATCH /topic/{topic_name}`

Will update an existing topics singular or multiple configuration i.e. will not be a hard reset rather a single setting update

- `DELETE /topic/{topic_name}`

Will delete all data related to a topic

- `POST /publish`

Will publish data to topic(s)

- `POST /subscribe`

Will subscribe the client to the topic(s)