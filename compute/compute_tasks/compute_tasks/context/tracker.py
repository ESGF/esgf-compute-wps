import hashlib
import os
import uuid

import cwt

class Tracker(object):
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

    def cache_local_path(self, url):
        """ Builds path for a local cached file.

        TODO: Make this a little smarter, duplicates can exist since the url could point to a
        replica and uid is based of the entire url not just the unique portion related to the file.
        """
        uid = hashlib.sha256(url.encode()).hexdigest()

        filename_ext = '{!s}.nc'.format(uid)

        base_path = os.path.join(os.environ['DATA_PATH'], 'cache')

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        return os.path.join(base_path, filename_ext)

    def generate_local_path(self, extension, filename=None):
        if filename is None:
            filename = str(uuid.uuid4())

        filename_ext = '{!s}.{!s}'.format(filename, extension)

        base_path = os.path.join(os.environ['DATA_PATH'], str(self.user), str(self.job))

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        return os.path.join(base_path, filename_ext)

    def build_output(self, extension, mime_type, filename=None, var_name=None, name=None):
        local_path = self.generate_local_path(extension, filename=filename)

        self.track_output(local_path)

        self.output.append(cwt.Variable(local_path, var_name, name=name, mime_type=mime_type))

        return local_path

    def build_output_variable(self, var_name, name=None):
        return self.build_output('nc', 'application/netcdf', var_name=var_name, name=name)
