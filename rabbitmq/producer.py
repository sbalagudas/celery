#!/usr/bin/env python

import pika
import time
from common import Queue_Basic as QB

class Producer():
    def __init__(self):
        self.qb = QB()
 
    def send_message(self,message,exchange_name,routing_key):
        print("sending message <%s> with rule <%s>"%(message,routing_key))
        self.qb.channel.basic_publish(exchange=exchange_name,
                                   routing_key=routing_key,
                                   body=message,
                                   #to make the message persistence
                                   properties=pika.BasicProperties(delivery_mode = 2))
        #self.qb.connect.close() 

def p1_ex1_q1(exc_name,q_name):
    producer = Producer()
    producer.qb.create_exchange(exc_name)
    queue_obj = producer.qb.create_queue(q_name)
    queue_name = queue_obj.method.queue
    print("[x] queue_name : ",queue_name)
    producer.qb.bind_exchange_queue(exc_name,queue_name)
    #send message via declared queue
    producer.send_message("hello,p1_ex1_q1",exc_name)

def p1_ex1_q2(exc_name):
    producer = Producer()
    producer.qb.create_exchange(exc_name)
    queue_obj1 = producer.qb.create_queue("p1_1_2_01")
    queue_obj2 = producer.qb.create_queue("p1_1_2_02")
    queue_name1 = queue_obj1.method.queue
    queue_name2 = queue_obj2.method.queue
    producer.qb.bind_exchange_queue(exc_name,queue_name1,"black")
    producer.qb.bind_exchange_queue(exc_name,queue_name2,"red")
    #send message via declared queue
    rule=["black","red"]
    for i in range(1,11):
        if 0 == i % 2 : 
            producer.send_message("hello,p1_1_2_01[%s]"%i,"ex1_1_2",rule[0])
        else : 
            producer.send_message("hello,p1_1_2_02[%s]"%i,"ex1_1_2",rule[1])
   
def p1_ex2_q1():
    pass
def p1_ex2_q2():
    pass
def p2_ex1_q1():
    pass
def p2_ex2_q1():
    pass
def p2_ex2_q2():
    pass
    

def main():
    #p1_ex1_q1("p1_ex1_q1","p1_ex1_q1")
    p1_ex1_q2("ex1_1_2_01")

if __name__ == "__main__":
    main()
