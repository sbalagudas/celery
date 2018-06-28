#!/usr/bin/env python

from celery_taste import test_add

result = test_add.delay(4,5)
print("result.ready : %s"%result.ready())
