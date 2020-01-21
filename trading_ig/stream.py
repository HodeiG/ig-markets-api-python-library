#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from threading import Thread, Lock
from queue import Queue
import json
import time

from trading_ig.lightstreamer import LSClient, Subscription

logger = logging.getLogger(__name__)


class IGStreamService(object):
    def __init__(self, ig_service):
        self.ig_service = ig_service
        self.ig_session = None
        self.ls_client = None
        self.trades_listeners = []
        self.trades_listeners_lock = Lock()

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

    def on_item_update(self, data):
        logger.info(data)
        if not self.trades_listeners:
            return
        # Lock so the listener list doesn't change during the iteration
        with self.trades_listeners_lock:
            for listener in self.trades_listeners:
                for key, item in data.get('values', {}).items():
                    if item:
                        listener.send(json.loads(item))

    def _create_subscription_channels(self, accountId):
        """
        Function to create a subscription with the Lightstream server and
        create a local publish/subscription system to read those events when
        they are needed using the listeners.
        """
        subscription = Subscription(
            mode="DISTINCT",
            items=["TRADE:%s" % accountId],
            fields=["CONFIRMS", "OPU", "WOU"])

        subscription.addlistener(self.on_item_update)
        self.ls_client.subscribe(subscription)

    def unsubscribe_all(self):
        # To avoid a RuntimeError: dictionary changed size during iteration
        subscriptions = self.ls_client._subscriptions.copy()
        for subcription_key in subscriptions:
            self.ls_client.unsubscribe(subcription_key)

    def close_listeners(self):
        """
        Send None to all listeners to stop listening and empty listeners list
        """
        # Lock so the listener list doesn't change during the iteration
        with self.trades_listeners_lock:
            for listener in self.trades_listeners:
                try:
                    listener.close()
                except Exception:
                    logging.exception("Failed to close listener %s", listener)
                self.trades_listeners = []

    def add_trade_listener(self, timeout=120):
        # Lock to only allow one thread create a listener
        with self.trades_listeners_lock:
            listener = Listener(timeout)
            self.trades_listeners.append(listener)
        return listener

    def del_trade_listener(self, listener):
        # Lock to only allow one thread remove a listener
        with self.trades_listeners_lock:
            listener.close()
            self.trades_listeners.remove(listener)

    def disconnect(self):
        logging.info("Disconnect from the light stream.")
        self.unsubscribe_all()
        self.ls_client.disconnect()
        self.close_listeners()


class Listener:
    def __init__(self, timeout=120):
        self.lock = Lock()
        self.listening = True
        self.queue = Queue()
        Thread(target=self.close, args=(timeout,)).start()

    def send(self, data):
        """
        Function to feed the queue.
        """
        self.queue.put(data)

    def close(self, timeout=0):
        """
        Function to stop feeding the queue.

        If timeout provided, wait until timeout to close queue.
        """
        while timeout > 0 and self.listening:
            time.sleep(0.1)
            timeout -= 0.1
        with self.lock:
            if self.listening:
                self.listening = False
                self.queue.put(None)

    def listen(self, function):
        """
        Function to listen for an event represented by a function.
        """
        data = None
        while self.listening:
            _data = self.queue.get()
            if _data is None:
                logging.error("Exiting as None was read from queue.")
                break
            elif function(_data):
                data = _data
                break
        return data

    def listen_event(self, key, value):
        """
        Function to listen for a specific key/value event.
        """
        logging.info("Listen for event '%s' == '%s'", key, value)
        data = self.listen(lambda v: v[key] == value)
        return data
