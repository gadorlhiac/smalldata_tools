class MpiData(object):
    """Collect all the data we're interested in for shmem run"""
    def __init__(self, exp_name, keys):
        """Init the in memory data storage object
        for a shmem run

        Parameters
        ----------
        exp_name: str
            The experiment/run name (xppd7114:run=43)
        """
        self._exp_name = exp_name
        self._keys = keys

    @property
    def data_keys(self):
        """All the data keys that have been added"""
        return self._keys

    def add_data(self, key, data):
        """Add to the data, validation occurs here"""
        self._keys.append(key)
        self._data.append({key: data})