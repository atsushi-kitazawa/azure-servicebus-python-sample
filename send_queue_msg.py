import os
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.identity.aio import DefaultAzureCredential

# reference
# https://learn.microsoft.com/ja-jp/azure/service-bus-messaging/service-bus-python-how-to-use-queues?tabs=passwordless

# set enviroment variables before execute script
# export FULLY_QUALIFIED_NAMESPACE=xxx.servicebus.windows.net
# export QUEUE_NAME=q1

FULLY_QUALIFIED_NAMESPACE = os.environ.get('FULLY_QUALIFIED_NAMESPACE')
QUEUE_NAME = os.environ.get('QUEUE_NAME')
credential = DefaultAzureCredential()

async def send_single_message(sender, msg):
    # Create a Service Bus message and send it to the queue
    message = ServiceBusMessage(msg)
    await sender.send_messages(message)
    print("Sent a single message")

async def run():
    # create a Service Bus client using the credential
    async with ServiceBusClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        credential=credential,
        logging_enable=True) as servicebus_client:
        # get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
        async with sender:
            # send one message
            await send_single_message(sender, 'test message!!')
            # send a list of messages
            # await send_a_list_of_messages(sender)
            # send a batch of messages
            # await send_batch_message(sender)

        # Close credential when no longer needed.
        await credential.close()

if __name__ == "__main__":
    asyncio.run(run())
    print("Done sending messages")
    print("-----------------------")