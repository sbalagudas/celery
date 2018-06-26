#!/usr/bin/env python

import pika
import time
import logging

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
        logging.basicConfig(format="%(levelname)s : %(message)s",level=logging.INFO)
        #1. connect to the broker : rabbitmq 
        self.connect = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        logging.info("connection to rabbitmq established.")
        #2. create a channel
        self.channel = self.connect.channel()
        logging.info("connection to channel established.")

    def create_exchange(self,e_name,e_type="fanout"):
        logging.info("creating exchange : < %s >"%e_name)
        self.channel.exchange_declare(exchange=e_name,exchange_type=e_type)

    def bind_exchange_queue(self,exchange,queue_name):
        logging.info("binding exchange with queue < %s : %s >"%(exchange,queue_name))
        self.channel.queue_bind(exchange=exchange,queue=queue_name)

    def create_queue(self,queue_name=""):
        logging.info("queue name < %s > in create_queue"%queue_name)
        if queue_name:
            logging.info("creating queue <%s>"%(queue_name))
            #durable, makes the rabbitMQ to remember the queue even after restart.
            #self.channel.queue_declare(queue=queue_name,durable=True)
            try : 
                q_name = self.channel.queue_declare(queue=queue_name)
            except :
                logging.info("create queue <%s> failed,maybe the name is duplicate ? "%queue_name)
        else :
            logging.info("queue name not specified, <random> queue created.")
            #self.channel.queue_declare(queue="default",durable=True) 
            q_name = self.channel.queue_declare(exclusive=True)
        return q_name
 
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
