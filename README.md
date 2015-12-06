# eventstreams

This is the source code for the post on azure stream analytics with sciml found on www.esciencegroup.com
To use it you will need an Azure subscription that you can use to create an eventhub channel, 
a message queue in the message bus and an azure table service.
you will also need an AzureML service.   Another blog post describes how to create that.

There are two python files here. 
run-sciml-to-eventhub.py assembles a set of the ARXIV document records and sends them to the event hub.
pull-events-from-eventhub-through-queue.py which pulls events from the event hub, invokes the azureml service
and then saves the response in the table.

the event puller can be run as a standalone python program anywhere or it can be run in a docker contianer.
the Dockerfile for the container is included here.    When invoked it must be supplied with one integer
parameter in the range 0 to 3 to identify which azureml service endpoint to use.   

More details in the blog post.
