# CW

In this coursework you will build a distributed storage system. The system has one Controller 
and N Data Stores (Dstores). It supports multiple concurrent clients sending store, load, list, remove 
requests. You will implement Controller and Dstores; the client will be provided. Each file 
is replicated R times over different Dstores. Files are stored by the Dstores, the 
Controller orchestrates client requests and maintains an index with the allocation of files 
to Dstores, as well as the size of each stored file. The client actually gets the files directly 
from Dstores – which improves scalability. For simplicity, all these processes will be on 
the same machine, but the principles are similar to a system distributed over several 
servers. Files in the distributed storage are not organised in folders and sub-folders. 
Filenames do not contain spaces.
The Controller is started first, with R as an argument. It waits for Dstores to join the 
storage system (see Rebalance operation). The Controller does not serve any client 
request until at least R Dstores have joined the system.
As Dstores may fail and new Dstores can join the storage system at runtime, rebalance 
operations are required to make sure each file is replicated R times and files are 
distributed evenly over the Dstores.


![Capture](https://user-images.githubusercontent.com/90190006/197465385-436a2f86-8aa4-4901-9a7e-3146c5c3e1aa.PNG)

Controller, Dstores and Clients will communicate with each other via TCP connections. 
Because they will be on the same machine, the Dstores will listen on different ports.

The Dstores will establish connections with the Controller as soon as they start. These 
connections will be persistent (i.e., they are expected to be kept alive for as long as the 
Dstore is running). If the Controller detects that the connection with one of the Dstores
dropped, then such a Dstore will be removed from the set of Dstores that are part of the 
storage system.

Processes should send textual messages (e.g., LIST – see below) using the println()
method of PrintWriter class, and receive using the readLine() method of BufferedReader
class. For data messages (i.e., file content), processes should send using the write()
method of OutputStream class and receive using the readNBytes() method of 
InputStream class.
