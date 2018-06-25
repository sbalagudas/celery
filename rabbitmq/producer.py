#!/usr/bin/env python

import pika
import time

class RabbitMQ_Test():
    """  rabbitmq steps
         1. connect to the broker : rabbitmq 
         connect = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
         2. create a channel
         channel = connect.channel()
         3. create a queue
         channel.queue_declare(queue="hello")
         4. send a message to queue
         channel.basic_publish(exchange="",routing_key="hello",body="hello world")
         print("[x] Sent 'Hello World!'")
         5. close the connection
         connect.close()
    """   
    def __init__(self):
        #1. connect to the broker : rabbitmq 
        self.connect = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        print("connection to rabbitmq established.")
        #2. create a channel
        self.channel = self.connect.channel()
        print("channel established on connection.")

    def create_queue(self,queue_name="default"):
        if not "default" == queue:
            print("creating queue <%s>"%queue_name)
            #durable, makes the rabbitMQ to remember the queue even after restart.
            self.channel.queue_declare(queue=queue_name,durable=True)
        else :
            print("queue name not specified, <default> queue created.")
            #self.channel.queue_declare(queue="default",durable=True) 
            self.channel.queue_declare(queue="default") 
 
    def send_message(self,queue_name,message):
        print("sending message <%s> on queue <%s>"%(message,queue_name))
        self.channel.basic_publish(exchange="",
                                   routing_key=queue_name,
                                   body=message,
                                   #to make the message persistence
                                   properties=pika.BasicProperties(delivery_mode = 2))
        #self.connect.close() 
    
def main():
    queue_list = ["mavric","iceman","jaguar"]
    message = ["hello,mavric","hello,iceman","helle,jaguar"]
    print("sending...")
    for i in range(0,3):
        rabbit = RabbitMQ_Test()
        map(rabbit.create_queue,queue_list)
        #rabbit.send_message(queue_list[i],message[i])
        rabbit.send_message("mavric","mavric[%s]"%(i))
               

if __name__ == "__main__":
    main()
