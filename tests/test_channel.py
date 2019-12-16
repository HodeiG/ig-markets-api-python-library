import pytest


def test_confirmation_channel():
    from trading_ig.stream import ConfirmChannel
    from trading_ig.stream import SUB_TRADE_CONFIRMS
    from trading_ig.stream import ChannelClosedException
    import nnpy
    import dill
    import time
    from threading import Thread

    pub = nnpy.Socket(nnpy.AF_SP, nnpy.PUB)
    pub.bind(SUB_TRADE_CONFIRMS)

    # Create publisher thread
    def publisher():
        index = 0
        while True:
            try:
                pub.send(dill.dumps('{"dealReference": %s}' % index))
            except nnpy.errors.NNError:
                break
            index += 1
            time.sleep(0.1)

    channel = ConfirmChannel()
    thread = Thread(target=publisher)
    thread.start()
    # Wait for the queue to get updated, so we can truly test events are queued
    while channel.queue.qsize() == 0:
        time.sleep(0.1)
    assert channel.wait_event("dealReference", 0) == {'dealReference': 0}
    with pytest.raises(ChannelClosedException):
        assert channel.wait_event("dealReference", 2)
    pub.close()  # Stop thread
