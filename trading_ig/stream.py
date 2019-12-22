#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import nnpy
import dill
from threading import Thread, Lock
from queue import Queue
import json
import time

from trading_ig.lightstreamer import LSClient, Subscription

logger = logging.getLogger(__name__)

ADDR_CONFIRMS = 'inproc://sub_trade_confirms'
ADDR_OPU = 'inproc://sub_trade_opu'
ADDR_WOU = 'inproc://sub_trade_wou'


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
        except Exception as exc:
            logger.exception("Unable to connect to Lightstreamer Server")
            raise exc

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
        pub_confirms.bind(ADDR_CONFIRMS)
        self.publishers.append(pub_confirms)

        pub_opu = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
        pub_opu.bind(ADDR_OPU)
        self.publishers.append(pub_opu)

        pub_wou = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
        pub_wou.bind(ADDR_WOU)
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

    def unsubscribe_all(self):
        # To avoid a RuntimeError: dictionary changed size during iteration
        subscriptions = self.ls_client._subscriptions.copy()
        for subcription_key in subscriptions:
            self.ls_client.unsubscribe(subcription_key)

    def disconnect(self):
        logging.info("Disconnect from the light stream.")
        for publisher in self.publishers:
            try:
                # Send None to all subscribers to give the chance to unsuscribe
                publisher.send(dill.dumps(None))
                # Sleep before closing channel, otherwise message might get
                # lost
                time.sleep(0.1)
                # Close publisher
                publisher.close()
            except Exception:
                logging.exception("Failed to close publisher %s", publisher)
        self.publishers = []
        self.unsubscribe_all()
        self.ls_client.disconnect()


class ChannelClosedException(Exception):
    msg = "Channel is already closed. Create a new channel for new events."

    def __str__(self):
        return self.msg


class Channel:
    def __init__(self, addr, timeout):
        """
        Class to subscribe to the publisher on address 'addr' and queue the
        events so they can be processed later on.

        The channel will timeout if no succesfull events are found.
        """
        self.addr = addr
        self.lock = Lock()
        # Subscribe to addr
        self.sub = nnpy.Socket(nnpy.AF_SP, nnpy.SUB)
        self.sub.connect(self.addr)
        self.sub.setsockopt(nnpy.SUB, nnpy.SUB_SUBSCRIBE, '')
        # Create queue and start updating it
        self.queue = Queue()
        # Start updating the queue
        Thread(target=self._update_queue).start()
        # Timeout subscriber
        Thread(target=self._kill_subscriber, args=(timeout,)).start()

    def _update_queue(self):
        while True:
            try:
                data = dill.loads(self.sub.recv())
                if data is None:
                    logger.warning("Stop updating queue as None was received.")
                    break
                self.queue.put(json.loads(data))
            # Read can mainly fail for 2 reasons:
            # 1. sub.recv() exception due to closed subscription
            # 2. sub is None as subscriber has been disabled after recv()
            except (nnpy.errors.NNError, AttributeError):
                break
        # Put None on queue so the consumer stops.
        self.queue.put(None)

    def _kill_subscriber(self, timeout=0):
        while timeout > 0 and self.sub:
            time.sleep(0.1)
            timeout -= 0.1
        with self.lock:  # Lock to only allow one thread to close subscriber
            if self.sub:
                # Close subscriber to stop _update_queue
                self.sub.close()
                # Disable subscriber so it cannot be called again
                self.sub = None

    def _process_queue(self, function):
        data = None
        while True:
            data = self.queue.get()
            if data is None:
                logging.error("Exiting as None was read from queue.")
                break
            elif function(data):
                break
        return data

    def wait_event(self, key, value):
        logging.info("Wait for event '%s' == '%s'", key, value)
        if not self.sub:
            raise ChannelClosedException
        event = self._process_queue(lambda v: v[key] == value)
        self._kill_subscriber()
        return event


class ConfirmChannel(Channel):
    def __init__(self, timeout=120):
        super().__init__(ADDR_CONFIRMS, timeout)


class OPUChannel(Channel):
    def __init__(self, timeout=120):
        super().__init__(ADDR_OPU, timeout)


class WOUChannel(Channel):
    def __init__(self, timeout=120):
        super().__init__(ADDR_WOU, timeout)
