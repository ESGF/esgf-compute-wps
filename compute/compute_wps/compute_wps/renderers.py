from rest_framework import renderers

from compute_wps import models
from compute_wps.util import wps_response

class WPSRenderer(renderers.BaseRenderer):
    media_type = 'application/xml'
    format = 'wps'

    def render(self, data, media_type=None, renderer_context=None):
        view = renderer_context['view']

        item = view.get_object()

        return wps_response.execute(item)
