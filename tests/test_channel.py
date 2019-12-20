import pytest


def test_confirmation_channel():
    from trading_ig.stream import ConfirmChannel
    from trading_ig.stream import ADDR_CONFIRMS
    from trading_ig.stream import ChannelClosedException
    import nnpy
    import dill
    import time
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
            time.sleep(0.05)

    # Test that the publisher data gets read and that the channel gets closed
    # Create first the channel so the deals start getting queued
    channel = ConfirmChannel()  # Using default timeout
    thread = Thread(target=publisher)
    thread.start()
    # Wait for the queue to get updated, so we can truly test that the events
    # are queued
    while channel.queue.qsize() <= 2:
        time.sleep(0.02)
    assert channel.wait_event("dealReference", 0) == {'dealReference': 0}
    with pytest.raises(ChannelClosedException):
        assert channel.wait_event("dealReference", 0)

    # Test that the subscriber timeouts
    channel = ConfirmChannel(timeout=0.5)
    assert channel.wait_event("dealReference", 0) is None

    pub.close()  # Stop thread
