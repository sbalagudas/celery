#!/usr/bin/env python

import pika
import time
from common import Queue_Basic as QB

class Consumer():
    def __init__(self):
        self.qb = QB()
         
    def listen(self,queue_name):
        self.qb.channel.basic_consume(self.callback,queue_name) 
        print("[x] listening on queue :< %s >,To exit press CTRL+C"%queue_name)
        self.qb.channel.start_consuming()

    def callback(self,ch,method,properties,body):
        print("[x] Received %r : %r"%(method.routing_key,body))
        time.sleep(3)
        #acknowledge, to ensure the message will never lost, 
        #even the queue is broken or the queue is down.
        ch.basic_ack(delivery_tag = method.delivery_tag)

def p1_ex1_q1(q_name):
    consumer = Consumer()
    queue_obj = consumer.qb.create_queue(q_name)
    #queue_name = queue_obj.method.queue
    consumer.listen(q_name)

def p1_ex1_q2(q_name):
    consumer = Consumer()
    queue_obj = consumer.qb.create_queue(q_name)
    #queue_name = queue_obj.method.queue
    consumer.listen(q_name)
    #consumer.listen(q_name_list[1])

def main():
    p1_ex1_q1("p1_1_2_02")
    #p1_ex1_q2("q1_1_2_01")

if __name__ == "__main__":
    main()

