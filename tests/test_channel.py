def test_listener():
    from trading_ig.stream import IGStreamService
    from threading import Thread

    stream = IGStreamService(None)
    FINISH = False

    def publisher():
        index = 0
        while not FINISH:
            item = {
                'values': {
                    'WOU': None,
                    'OPU': None,
                    'CONFIRMS': '{"dealReference": %s}' % index
                }
            }
            stream.on_item_update(item)
            index += 1

    # Test that the publisher data gets read and that the channels get closed
    # Create first the channels so the deals start getting queued
    assert len(stream.trades_listeners) == 0
    channel1 = stream.add_trade_listener()  # Using default timeout
    assert len(stream.trades_listeners) == 1
    channel2 = stream.add_trade_listener()  # Using default timeout
    assert len(stream.trades_listeners) == 2

    # Create publisher after the listeners so both listeners read the same data
    thread = Thread(target=publisher)
    thread.start()

    # Test we can get any deal
    deal = channel1.listen(lambda x: True)  # Get the first deal
    assert 'dealReference' in deal.keys()
    stream.del_trade_listener(channel1)  # Stop listener
    assert len(stream.trades_listeners) == 1

    # Test we can get a deal with higher reference number
    ref1 = deal['dealReference']
    ref2 = ref1 + 1   # Find a ref higher than ref1
    deal = channel2.listen_event("dealReference", ref2)
    assert deal['dealReference'] == ref2
    stream.del_trade_listener(channel2)  # Stop listener
    assert len(stream.trades_listeners) == 0

    # Test that the subscriber timeouts
    channel = stream.add_trade_listener(timeout=0.2)
    assert len(stream.trades_listeners) == 1
    assert channel.listen_event("dealReference", 0) is None
    stream.del_trade_listener(channel)  # Stop listener
    assert len(stream.trades_listeners) == 0

    # Stop publisher thread
    FINISH = True
