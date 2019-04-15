# Flujo

**Flujo** is a Python library for stream processing. The library is designed to facilitate easy and quick definition of streaming applications.  Yet, it can be also very efficient. 

It can be used in any domain.  For example, in the vision domain it can be used to quickly develop video streaming analytics applications such as people detection, people tracking, people counting, etc. 

See the [**samples**](./samples/) folder for sample applications.

## Building Simple Flow applications

A flow application usually consists of three parts:

1. In the first part you define a flow as a directed acyclic graph.  A flow is made of producers, processors and consumers.  Producers create data and add it to the flow (commonly they will get the data from a source that is external to the flow and add it to it).  Processors read data from the flow and write data back to the flow.  Consumers read data from the flow but do not write back.

2. Once a flow is defined you can start it.  Starting the flow means that the producers start putting data into the flow and processors and consumers start receiving data.  Starting the flow also means allocating resources for producers, processors and consumers.

3. Once the flow starts, you can also stop it.  When you stop the flow, it will happen organically.  Producers will stop producing data.  The rest of the nodes in the flow will continue running until the pipes run dry.  The resources used in the flow are deallocated progressively (not all at the same time). For example, when a producer stops producing data, it deallocates itself and all the resources that are exclusive to him. 

You can stop part of a flow instead of the entire flow.  For example, in Figure 1, if producer B receives signal to stop producing data, then the pipe that connects nodes B, C, D, E will eventually run dry and the nodes will deallocate themselves.  The rest of the flow in the graph will be left untouched. (remember to connect another producer F to B, as well as make other independent children of F).


### Meaning
**Flujo** is the word for **flow** in Spanish.  

