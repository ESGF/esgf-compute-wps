import cwt

class Tracker:
    def __init__(self, **kwargs):
        self.job = kwargs.pop('job', None)
        self.user = kwargs.pop('user', None)
        self.process = kwargs.pop('process', None)

        super().__init__(**kwargs)

        self.metrics = {}

    def init_state(self, data):
        raise NotImplementedError()

    def store_state(self):
        raise NotImplementedError()

    def track_src_bytes(self, nbytes):
        raise NotImplementedError()

    def track_in_bytes(self, nbytes):
        raise NotImplementedError()

    def track_out_bytes(self, nbytes):
        raise NotImplementedError()

    def update_metrics(self, state):
        raise NotImplementedError()

    def set_status(self, status, output=None, exception=None):
        raise NotImplementedError()

    def message(self, fmt, *args, **kwargs):
        raise NotImplementedError()

    def accepted(self):
        raise NotImplementedError()

    def started(self):
        raise NotImplementedError()

    def failed(self, exception):
        raise NotImplementedError()

    def succeeded(self, output):
        raise NotImplementedError()

    def processes(self):
        raise NotImplementedError()

    def register_process(self, **params):
        raise NotImplementedError()

    def track_file(self, file):
        raise NotImplementedError()

    def track_process(self):
        raise NotImplementedError()

    def unique_status(self):
        raise NotImplementedError()

    def files_distinct_users(self):
        raise NotImplementedError()

    def user_cert(self):
        raise NotImplementedError()

    def user_details(self):
        raise NotImplementedError()

    def track_output(self, path):
        raise NotImplementedError()
