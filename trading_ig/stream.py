#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import sys
import traceback
import logging
import nnpy
import dill
from concurrent import futures
import json

from .lightstreamer import LSClient, Subscription

logger = logging.getLogger(__name__)
CHANNELS = {
    "trade": 'inproc://trade_events'
}


class IGStreamService(object):
    def __init__(self, ig_service):
        self.ig_service = ig_service
        self.ig_session = None
        self.ls_client = None

    def create_session(self):
        ig_session = self.ig_service.create_session()
        self.ig_session = ig_session
        return ig_session

    def connect(self, accountId):
        cst = self.ig_service.crud_session.CLIENT_TOKEN
        xsecuritytoken = self.ig_service.crud_session.SECURITY_TOKEN
        lightstreamerEndpoint = self.ig_session[u'lightstreamerEndpoint']
        # clientId = self.ig_session[u'clientId']
        ls_password = 'CST-%s|XST-%s' % (cst, xsecuritytoken)

        # Establishing a new connection to Lightstreamer Server
        logger.info("Starting connection with %s" % lightstreamerEndpoint)
        # self.ls_client = LSClient("http://localhost:8080", "DEMO")
        # self.ls_client = LSClient("http://push.lightstreamer.com", "DEMO")
        self.ls_client = LSClient(lightstreamerEndpoint, adapter_set="",
                                  user=accountId, password=ls_password)
        try:
            self.ls_client.connect()
        except Exception:
            logger.error("Unable to connect to Lightstreamer Server")
            logger.error(traceback.format_exc())
            sys.exit(1)

        # Create subsciption channel for trade events
        subscription = Subscription(
            mode="DISTINCT",
            items=["TRADE:%s" % accountId],
            fields=["CONFIRMS", "OPU", "WOU"])
        pub = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
        pub.bind(CHANNELS['trade'])

        def on_item_update(item_update):
            pub.send(dill.dumps(item_update))

        subscription.addlistener(on_item_update)
        self.ls_client.subscribe(subscription)

    def wait(self, find_function):
        def subscribe():
            sub = nnpy.Socket(nnpy.AF_SP, nnpy.SUB)
            sub.connect(CHANNELS['trade'])
            sub.setsockopt(nnpy.SUB, nnpy.SUB_SUBSCRIBE, '')
            order = None
            while order is None:
                data = dill.loads(sub.recv())
                for value in data.get('values', {}).values():
                    if value is not None:
                        value = json.loads(value)
                        if find_function(value):
                            order = value
                            break
            sub.close()
            return order

        executor = futures.ThreadPoolExecutor()
        future = executor.submit(subscribe)
        return future

    def unsubscribe_all(self):
        # To avoid a RuntimeError: dictionary changed size during iteration
        subscriptions = self.ls_client._subscriptions.copy()
        for subcription_key in subscriptions:
            self.ls_client.unsubscribe(subcription_key)

    def disconnect(self):
        self.unsubscribe_all()
        self.ls_client.disconnect()
