#!/usr/bin/env python

from common import Queue_Basic as QB

class Server():
    def __init__(self):
        self.qb = QB()
        self.qb.create_queue("s_c_queue_01")
       
    
    def factorial(self,n):
        s = 1
        if n >= 0:
            while n > 1:
                s = s * n 
                n -= 1
        return s

    def on_request(self,ch,method,props,body):
        n = int(body)
        print("calculating %d"%n)
        
        #send ack back
        print(("%s : %s : %s : %s")%((self.factorial(n)),props.correlation_id,"",props.reply_to))
        self.qb.send_message(str(self.factorial(n)),props.correlation_id,"",props.reply_to,"")
        self.qb.channel.basic_ack(delivery_tag = method.delivery_tag)

def main():
    server = Server()
    server.qb.channel.basic_qos(prefetch_count=1)
    server.qb.channel.basic_consume(server.on_request,queue="s_c_queue_01")
    print("[x] Awaiting RPC requests...")
    server.qb.channel.start_consuming()

if __name__ == "__main__":
    main()
        
        
