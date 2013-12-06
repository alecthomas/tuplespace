import json

import requests


"""A Python interface to http://github.com/alecthomas/tuplespace"""


HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}


class Error(Exception):
    """Base of all tuplespace exceptions."""


class Timeout(Error):
    """Tuplespace operation timed out."""


def _check_response(response, status_codes):
    if response.headers['content-type'] != 'application/json':
        raise Error('expected application/json response, got %s' % response.headers['content-type'])
    if response.status_code == 504:
        raise Timeout('tuplespace timeout')
    response.data = json.loads(response.content)
    if response.status_code not in status_codes:
        raise Error('unexpected status code %d: %s' % (response.status_code, response.data.get('error', '?')))
    return response


def _timeout(timeout):
    if timeout == 0:
        return 0
    return long(timeout * 1000000000.0)


class TupleSpace(object):
    """Tuplespace client for https://github.com/wharf/tuplespace.

    Tuples are represented as dictionaries.

    Match expressions are C-like expressions evaluated against
    the tuple. An expression must evaluate to true for a match to occur.

    An example expression might be:

        Command == "CreateUser" && (Id % 4) == 1
    """
    def __init__(self, url='http://127.0.0.1:2619/tuplespace/'):
        self.url = url

    def send(self, tuple, timeout=0.0):
        """Send a tuple with an optional timeout.

        :param tuple: Tuple.
        :param timeout: Optional timeout.
        """
        request = {'tuples': [tuple], 'timeout': _timeout(timeout)}
        self._post('', request, (201,))

    def send_many(self, tuples, timeout=0.0):
        """Send tuples with an optional timeout.

        :param tuples: A list of tuples.
        :param timeout: Optional timeout.
        """
        request = {'tuples': tuples, 'timeout': _timeout(timeout)}
        self._post('', request, (201,))

    def read(self, match, timeout=0.0):
        """Read a tuple with an optional timeout.

        :param match: A tuple-like pattern where null values are treated as wildcards.
        :param timeout: Optional timeout.
        """
        request = {'match': match, 'timeout': _timeout(timeout)}
        response = self._get('', request, (200,))
        return response.data['tuples'][0]

    def read_all(self, match, timeout=0.0):
        """Read a tuple with an optional timeout.

        :param match: A tuple-like pattern where null values are treated as wildcards.
        :param timeout: Optional timeout.
        :return: List of matching tuples.
        """
        request = {'match': match, 'timeout': _timeout(timeout), 'all': True}
        response = self._get('', request, (200,))
        return response.data['tuples']

    def take(self, match, timeout=0.0):
        """Take (read and remove) a tuple from the tuplespace.

        :param match: A tuple-like pattern where null values are treated as wildcards.
        :param timeout: Optional timeout.
        :return: Matching tuple.
        """
        timeout = _timeout(timeout)
        request = {'match': match, 'timeout': timeout}
        response = self._delete('', request, (200,))
        return response.data['tuples'][0]

    def take_all(self, match, timeout=0.0):
        """Take (read and remove) all matching tuple from the tuplespace.

        :param match: A tuple-like pattern where null values are treated as wildcards.
        :param timeout: Optional timeout.
        :return: List of matching tuples.
        """
        timeout = _timeout(timeout)
        request = {'match': match, 'timeout': timeout, 'all': True}
        response = self._delete('', request, (200,))
        return response.data['tuples']

    def _req(self, method, path, data, status_codes):
        encoded = json.dumps(data)
        return _check_response(
            method(self.url + path, data=encoded, headers=HEADERS),
            status_codes,
            )

    def _post(self, path, data, status_codes):
        return self._req(requests.post, path, data, status_codes)

    def _get(self, path, data, status_codes):
        return self._req(requests.get, path, data, status_codes)

    def _delete(self, path, data, status_codes):
        return self._req(requests.delete, path, data, status_codes)
