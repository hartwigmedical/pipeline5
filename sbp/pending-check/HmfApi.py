import json
import os
import requests

class HmfApi(object):
    def __init__(self):
        self._url = os.getenv('HMF_APIHOST')
        self._headers = {}

        if self._url is None:
            self._url = 'http://hmfapi'

        self._version = '/hmf/v1/'
        self.id = None

        if not hasattr(self, '_type'):
            self._type = None

        if hasattr(self, '_keys'):
            for k in self._keys:
                setattr(self, k, None)

        else:
            self._keys = []

    def __str__(self):
        data = {}

        for key in self._keys + ['id']:
            if hasattr(self, key):
                data[key] = getattr(self, key)

        return json.dumps(data, sort_keys=True, indent=4)

    def get_all(self, _class, selector):
        url = self._url + self._version + _class()._type

        r = requests.get(url, params=selector, headers=self._headers)

        if r.status_code == 200:
            response = r.json()
            result = []

            for item in response:
                a = _class()

                for key in a._keys + ['id']:
                    if key in item:
                        setattr(a, key, item[key])

                result.append(a)

            return result

        raise ValueError("Invalid API response for " + url + " with status code " + str(r.status_code))

    def save(self):
        data = {}
        headers = self._headers
        headers.update({'Content-type': 'application/json', 'Accept': 'text/plain'})

        for key in self._keys:
            if getattr(self, key) is not None:
                data[key] = getattr(self, key)

        if self.id is not None:
            url = self._url + self._version + self._type + '/' + str(self.id)

            patch = requests.patch(url, json=data, headers=headers)

            if patch.status_code == 200:
                return True

            raise ValueError("Invalid API response for " + url + " with status code " + str(patch.status_code))
        else:
            url = self._url + self._version + self._type

            post = requests.post(url, json=data, headers=headers)

            if post.status_code in [200, 201]:
                if 'id' in post.json():
                    self.id = post.json()['id']
                    return True

            raise ValueError("Invalid API response for " + url + " with status code " + str(post.status_code))


class Sample(HmfApi):
    def __init__(self):
        self._type      = 'samples'
        self._keys      = ['barcode', 'name', 'submission', 'type', 'status', 'q30', 'q30_req', 'yld', 'yld_req', 'version']
        super(Sample, self).__init__()


class Set(HmfApi):
    def __init__(self):
        self._type = 'sets'
        self._keys = ['name', 'size', 'createTime', 'entity_id', 'ref_sample', 'tumor_sample', 'fastq_portal']
        super(Set, self).__init__()


class Run(HmfApi):
    def __init__(self):
        self._type = 'runs'
        self._keys = ['set_id', 'stack_id', 'startTime', 'endTime', 'status', 'ini_id', 'bucket', 'cluster']
        super(Run, self).__init__()


class Ini(HmfApi):
    def __init__(self):
        self._type = 'inis'
        self._keys = ['name']
        super(Ini, self).__init__()


class Stack(HmfApi):
    def __init__(self):
        self._type = 'stacks'
        self._keys = ['name']
        super(Stack, self).__init__()

