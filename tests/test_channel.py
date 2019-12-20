import pytest


def test_confirmation_channel():
    from trading_ig.stream import ConfirmChannel
    from trading_ig.stream import ADDR_CONFIRMS
    from trading_ig.stream import ChannelClosedException
    import nnpy
    import dill
    from threading import Thread

    pub = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
    pub.bind(ADDR_CONFIRMS)

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
    channel1 = ConfirmChannel()  # Using default timeout
    channel2 = ConfirmChannel()  # Using default timeout

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
    channel = ConfirmChannel(timeout=0.2)
    assert channel.wait_event("dealReference", 0) is None

    pub.close()  # Stop thread
