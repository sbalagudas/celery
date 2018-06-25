#!/usr/bin/env python

import pika
import time

class Queue_Basic():
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

    def create_exchange(self,e_name,e_type="fanout"):
        self.channel.exchange_declare(exchange=e_name,exchange_type=e_type)

    def bind_exchange_queue(self,exchange,queue_name):
        self.channel.queue_bind(exchange=exchange,queue=queue_name)

    def create_queue(self,queue_type,queue_name=""):
        if not queue_name:
            print("creating queue <%s> with type <%s>"%(queue_name,queue_type))
            #durable, makes the rabbitMQ to remember the queue even after restart.
            #self.channel.queue_declare(queue=queue_name,durable=True)
            try : 
                q_name = self.channel.queue_declare(queue=queue_name,exclusive=True,durable=True)
            except :
                print("create queue <%s> failed,maybe the name is duplicate ? "%queue_name)
        else :
            print("queue name not specified, <random> queue created.")
            #self.channel.queue_declare(queue="default",durable=True) 
            q_name = self.channel.queue_declare(exclusive=True,durable=True)
        return q_name
 
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
