import pytest
import time


def test_confirmation_channel():
    from trading_ig.stream import TradesChannel
    from trading_ig.stream import ADDR_TRADES
    from trading_ig.stream import ChannelClosedException
    import nnpy
    import dill
    from threading import Thread

    pub = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
    pub.bind(ADDR_TRADES)

    # Create publisher thread
    def publisher():
        index = 0
        while True:
            try:
                pub.send(dill.dumps('{"dealReference": %s}' % index))
            except nnpy.errors.NNError:
                break
            index += 1

    # Test that the publisher data gets read and that the channels get closed
    # Create first the channels so the deals start getting queued
    channel1 = TradesChannel()  # Using default timeout
    channel2 = TradesChannel()  # Using default timeout

    # Create publisher after the channel, so both channels read the same data
    thread = Thread(target=publisher)
    thread.start()

    deal = channel1._process_queue(lambda x: True)  # Get the first deal
    channel1._kill_subscriber()  # Kill subscriber
    ref1 = deal['dealReference']
    ref2 = ref1 + 1   # Find a ref higher than ref1
    deal = channel2.wait_event("dealReference", ref2)
    assert deal['dealReference'] == ref2
    with pytest.raises(ChannelClosedException):
        assert channel1.wait_event("dealReference", 0)

    # Test that the subscriber timeouts
    channel = TradesChannel(timeout=0.2)
    assert channel.wait_event("dealReference", 0) is None

    # Test that the susbscriber deals with None received before publisher gets
    # closed
    channel = TradesChannel()
    # Simulate publisher sends None before it gets closed
    pub.send(dill.dumps(None))
    time.sleep(0.1)  # Give time to the subscriber to read None
    pub.close()  # Stop thread
    assert channel.wait_event("dealReference", 0) is None
