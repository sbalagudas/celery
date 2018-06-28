#!/usr/bin/env python

from common import Queue_Basic as QB

class Client():
    def __init__(self):
        self.response = None
        self.qb = QB()
        random_queue = self.qb.create_queue()
        self.random_queue_name = random_queue.method.queue
        self.qb.channel.basic_consume(self.on_response,no_ack=True,queue=self.random_queue_name)

    def on_response(self,ch,method,props,body):
        if self.corr_id == props.correlation_id:
            self.response = body
    
    def call(self,n):
        self.corr_id = "111"
        self.qb.send_message(str(n),self.corr_id,"","s_c_queue_01",self.random_queue_name)
        while self.response is None:
            self.qb.connect.process_data_events()
        return self.response

def main():
    client = Client()
    print("[x] Requesting factorial(6)")
    response = client.call(6)
    print("got response %s",response)

if __name__ == "__main__":
    main()
