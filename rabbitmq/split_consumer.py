#!/usr/bin/env python

from producer import RabbitMQ_Test as RBT

rabbit = RBT()
for i in range(1,10):
    rabbit.send_message("mavric","mavric[%s]"%(i))

