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

SUB_TRADE_CONFIRMS = 'inproc://sub_trade_confirms'
SUB_TRADE_OPU = 'inproc://sub_trade_opu'
SUB_TRADE_WOU = 'inproc://sub_trade_wou'


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
        self._create_subscription_channels(accountId)

    def _create_subscription_channels(self, accountId):
        """
        Function to create a subscription with the Lightstream server and
        create a local publish/subscription system to read those events when
        they are needed using the 'wait_event' function.
        """
        self.publishers = []
        subscription = Subscription(
            mode="DISTINCT",
            items=["TRADE:%s" % accountId],
            fields=["CONFIRMS", "OPU", "WOU"])

        pub_confirms = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
        pub_confirms.bind(SUB_TRADE_CONFIRMS)
        self.publishers.append(pub_confirms)

        pub_opu = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
        pub_opu.bind(SUB_TRADE_OPU)
        self.publishers.append(pub_opu)

        pub_wou = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
        pub_wou.bind(SUB_TRADE_WOU)
        self.publishers.append(pub_wou)

        def on_item_update(data):
            logger.info(data)
            values = data.get('values', {})
            # Publish confirms
            event = values.get('CONFIRMS')
            if event:
                pub_confirms.send(dill.dumps(event))
            # Publish opu
            event = values.get('OPU')
            if event:
                pub_opu.send(dill.dumps(event))
            # Publish wou
            event = values.get('WOU')
            if event:
                pub_wou.send(dill.dumps(event))

        subscription.addlistener(on_item_update)
        self.ls_client.subscribe(subscription)

    def wait_event(self, sub_channel, func_event_type):
        """
        Function to subscribe to a channel and wait until a specific event
        happens.

        The function will return a future object which will return the event
        value once it's ready.
        """
        def subscribe():
            sub = nnpy.Socket(nnpy.AF_SP, nnpy.SUB)
            sub.connect(sub_channel)
            sub.setsockopt(nnpy.SUB, nnpy.SUB_SUBSCRIBE, '')
            event = None
            while event is None:
                data = json.loads(dill.loads(sub.recv()))
                if func_event_type(data):
                    event = data
            sub.close()
            return event

        executor = futures.ThreadPoolExecutor()
        future = executor.submit(subscribe)
        return future

    def unsubscribe_all(self):
        # To avoid a RuntimeError: dictionary changed size during iteration
        subscriptions = self.ls_client._subscriptions.copy()
        for subcription_key in subscriptions:
            self.ls_client.unsubscribe(subcription_key)

    def disconnect(self):
        for publisher in self.publishers:
            publisher.close()
        self.publishers = []
        self.unsubscribe_all()
        self.ls_client.disconnect()
