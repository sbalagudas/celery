#!/usr/bin/env python

from celery import Celery
import time

app = Celery("celery_taste",backend="amqp",broker="amqp://localhost")

@app.task
def test_add(x,y):
    time.sleep(5)
    return x+y


if __name__ == "__main__":
    test_add.apply_async(3,4)
    

