#!/usr/bin/env python

import pika
import time
class Consumer():
    """ consumer steps:
    #1. create connection
    connect = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    #2. create channel
    channel = connect.channel()
    #3. declare queue
    channel.queue_declare(queue="mavric",durable=True)
    #4. define a callback function
        
    #5. consume the message which stores in the dedicated queue
    #channel.basic_consume(callback,queue="hello",no_ack=True)
    channel.basic_consume(callback,queue="mavric")
    print("[*] waiting for messages. To exit press CTRL+C")
    #6. start the consume process 
    channel.start_consuming()
    """
    def __init__(self):
        self.connect = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = self.connect.channel()
    
    def create_queue(self,queue_name="default"):
        if not "default" == queue_name:
            print("creating queue <%s>"%queue_name)                        
            #durable, makes the rabbitMQ to remember the queue even after restart.
            #self.channel.queue_declare(queue=queue_name,durable=True)
            self.channel.queue_declare(queue=queue_name)
        else :
            print("queue name not specified, <default> queue created.")
            self.channel.queue_declare(queue="default",durable=True)

    def listen(self,queue_name="default"):
        self.channel.basic_consume(self.callback,queue_name) 
        print("[x] listening on queue :< %s >,To exit press CTRL+C"%queue_name)
        self.channel.start_consuming()

    def callback(self,ch,method,properties,body):
        print("[x] Received %r"%body)
        time.sleep(3)
        #acknowledge, to ensure the message will never lost, 
        #even the queue is broken or the queue is down.
        ch.basic_ack(delivery_tag = method.delivery_tag)

def main():
    q_name = "mavric"
    consumer = Consumer()
    consumer.create_queue(q_name)
    consumer.listen(q_name)

if __name__ == "__main__":
    main()

