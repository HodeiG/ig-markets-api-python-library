import pytest


KWARGS_CLOSE_OPEN_POSITION = {
    "deal_id": None,
    "direction": None,
    "epic": None,
    "expiry": None,
    "level": None,
    "order_type": None,
    "quote_id": None,
    "size": None
}
KWARGS_CREATE_OPEN_POSITION = {
    "currency_code": None,
    "direction": None,
    "epic": None,
    "expiry": None,
    "force_open": None,
    "guaranteed_stop": None,
    "level": None,
    "limit_distance": None,
    "limit_level": None,
    "order_type": None,
    "quote_id": None,
    "size": None,
    "stop_distance": None,
    "stop_level": None,
    "trailing_stop": None,
    "trailing_stop_increment": None,
}
KWARGS_UPDATE_OPEN_POSITION = {
    "limit_level": None,
    "stop_level": None,
    "deal_id": "1",
}
KWARGS_CREATE_WORKING_ORDER = {
        "currency_code": None,
        "direction": None,
        "epic": None,
        "expiry": None,
        "guaranteed_stop": None,
        "level": None,
        "size": None,
        "time_in_force": None,
        "order_type": None,
}
KWARGS_DELETE_WORKING_ORDER = {
    "deal_id": "1",
}
KWARGS_UPDATE_WORKING_ORDER = {
    "deal_id": "1",
    "good_till_date": None,
    "level": None,
    "limit_distance": None,
    "limit_level": None,
    "stop_distance": None,
    "stop_level": None,
    "time_in_force": None,
    "order_type": None,
}


def start_publisher(ig, item):
    from trading_ig.stream import IGStreamService
    from threading import Thread
    ig.stream = IGStreamService(None)
    ig.stream.FINISH = False

    def publisher():
        index = 0
        while not ig.stream.FINISH:
            ig.stream.on_item_update(item)
            index += 1

    thread = Thread(target=publisher)
    thread.start()


def stop_publisher(ig):
    ig.stream.FINISH = True


@pytest.fixture()
def ig_mock():
    # 1. Setup fixture
    from trading_ig.rest import IGService
    stream_data = {
        'values': {
            'WOU': None,
            'OPU': None,
            'CONFIRMS': '{"dealReference": 1}'
        }
    }
    ig = IGService(None, None, None)
    ig.crud_session.HEADERS['DELETE'] = None
    ig.crud_session.HEADERS['LOGGED_IN'] = {'Version': "2"}
    ig.crud_session.create = ig.crud_session._create_logged_in
    start_publisher(ig, stream_data)
    # 2. yield resource
    yield ig
    # 3. Tear down fixture
    stop_publisher(ig)


class TestIgService:
    def test_close_open_position(self, requests_mock, ig_mock):
        from trading_ig.rest import IGException

        # Test that the decorator handles the deals
        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/positions/otc',
            text='{"dealReference": 1}')
        assert ig_mock.close_open_position(**KWARGS_CLOSE_OPEN_POSITION) == \
            {'dealReference': 1}

        # Test that the decorator deals with exception
        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/positions/otc',
            status_code=400)
        with pytest.raises(IGException):
            ig_mock.close_open_position(**KWARGS_CLOSE_OPEN_POSITION)

    def test_create_open_position(self, requests_mock, ig_mock):
        from trading_ig.rest import IGException

        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/positions/otc',
            text='{"dealReference": 1}',
            headers={'CST': ''})
        assert ig_mock.create_open_position(**KWARGS_CREATE_OPEN_POSITION) == \
            {'dealReference': 1}

        # Test that the decorator reads stream data
        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/positions/otc',
            status_code=400)
        with pytest.raises(IGException):
            ig_mock.create_open_position(**KWARGS_CREATE_OPEN_POSITION)

    def test_update_open_position(self, requests_mock, ig_mock):
        from trading_ig.rest import IGException

        requests_mock.put(
            'https://demo-api.ig.com/gateway/deal/positions/otc/1',
            text='{"dealReference": 1}',
            headers={'CST': ''})
        assert ig_mock.update_open_position(**KWARGS_UPDATE_OPEN_POSITION) == \
            {'dealReference': 1}

        # Test that the decorator reads stream data
        requests_mock.put(
            'https://demo-api.ig.com/gateway/deal/positions/otc/1',
            status_code=400)
        with pytest.raises(IGException):
            ig_mock.update_open_position(**KWARGS_UPDATE_OPEN_POSITION)

    def test_create_working_order(self, requests_mock, ig_mock):
        from trading_ig.rest import IGException

        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/workingorders/otc',
            text='{"dealReference": 1}',
            headers={'CST': ''})
        assert ig_mock.create_working_order(**KWARGS_CREATE_WORKING_ORDER) == \
            {'dealReference': 1}

        # Test that the decorator reads stream data
        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/workingorders/otc',
            status_code=400)
        with pytest.raises(IGException):
            ig_mock.create_working_order(**KWARGS_CREATE_WORKING_ORDER)

    def test_delete_working_order(self, requests_mock, ig_mock):
        from trading_ig.rest import IGException

        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/workingorders/otc/1',
            text='{"dealReference": 1}',
            headers={'CST': ''})
        assert ig_mock.delete_working_order(**KWARGS_DELETE_WORKING_ORDER) == \
            {'dealReference': 1}

        # Test that the decorator reads stream data
        requests_mock.post(
            'https://demo-api.ig.com/gateway/deal/workingorders/otc/1',
            status_code=400)
        with pytest.raises(IGException):
            ig_mock.delete_working_order(**KWARGS_DELETE_WORKING_ORDER)

    def test_update_working_order(self, requests_mock, ig_mock):
        from trading_ig.rest import IGException

        requests_mock.put(
            'https://demo-api.ig.com/gateway/deal/workingorders/otc/1',
            text='{"dealReference": 1}',
            headers={'CST': ''})
        assert ig_mock.update_working_order(**KWARGS_UPDATE_WORKING_ORDER) == \
            {'dealReference': 1}

        # Test that the decorator reads stream data
        requests_mock.put(
            'https://demo-api.ig.com/gateway/deal/workingorders/otc/1',
            status_code=400)
        with pytest.raises(IGException):
            ig_mock.update_working_order(**KWARGS_UPDATE_WORKING_ORDER)
