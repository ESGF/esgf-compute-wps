#! /usr/bin/env python

from builtins import range
import cwt
import mock
from django import test
from django.conf import settings

from wps import WPSError
from wps.tasks import preprocess

class PreprocessTestCase(test.TestCase):

    def test_generate_chunks_process(self):
        input = mock.MagicMock()
        input.chunk = []
        input.is_cached = False
        input.mapped_order = ['time', 'lat', 'lon']
        input.mapped = {
            'time': slice(0, 365, 1),
            'lat': slice(0, 180, 1),
            'lon': slice(0, 360, 1),
        }

        op = mock.MagicMock()
        op.get_parameter.return_value.values = ['time',]

        context = mock.MagicMock()
        context.inputs = [input,]
        type(context).operation = mock.PropertyMock(return_value=op)

        with self.settings(WORKER_MEMORY=100000000):
            new_context = preprocess.generate_chunks(context)

        self.assertEqual(input.chunk_axis, 'lat')
        self.assertEqual(input.chunk, [slice(0, 95, 1), slice(95, 180, 1)])

    def test_generate_chunks_no_axes(self):
        input = mock.MagicMock()
        input.chunk = []
        input.is_cached = False
        input.mapped_order = ['time', 'lat', 'lon']
        input.mapped = {
            'time': slice(0, 365, 1),
            'lat': slice(0, 180, 1),
            'lon': slice(0, 360, 1),
        }

        op = mock.MagicMock()
        op.get_parameter.return_value = None

        context = mock.MagicMock()
        context.inputs = [input,]
        type(context).operation = mock.PropertyMock(return_value=op)

        with self.settings(WORKER_MEMORY=100000000):
            new_context = preprocess.generate_chunks(context)

        self.assertEqual(input.chunk_axis, 'time')
        self.assertEqual(input.chunk, [slice(0, 192, 1), slice(192, 365, 1)])

    @mock.patch('wps.models.Cache.objects.filter')
    def test_check_cache_entries_invalid(self, filter):
        entry = mock.MagicMock()
        entry.is_superset.return_value = True

        filter.return_value = [
            entry,
        ]

        input = mock.MagicMock()

        context = mock.MagicMock()

        result = preprocess.check_cache_entries(input, context)

        self.assertEqual(result, entry)

    @mock.patch('wps.models.Cache.objects.filter')
    def test_check_cache_entries_invalid(self, filter):
        entry = mock.MagicMock()
        entry.validate.side_effect = Exception()

        filter.return_value = [
            entry,
        ]

        input = mock.MagicMock()

        context = mock.MagicMock()

        result = preprocess.check_cache_entries(input, context)

        self.assertIsNone(result)

        entry.delete.assert_called()

    @mock.patch('wps.models.Cache.objects.filter')
    def test_check_cache_entries(self, filter):
        filter.return_value = []

        input = mock.MagicMock()

        context = mock.MagicMock()

        result = preprocess.check_cache_entries(input, context)

        self.assertIsNone(result)

    @mock.patch('wps.tasks.preprocess.check_cache_entries')
    def test_check_cache_not_mapped(self, check_cache_entries):
        input = mock.MagicMock()
        input.mapped = None
        input.cache = None

        context = mock.MagicMock()
        context.inputs = [input,]

        new_context = preprocess.check_cache(context)

        self.assertIsNone(new_context.inputs[0].cache)

        check_cache_entries.assert_not_called()

    @mock.patch('wps.tasks.preprocess.check_cache_entries')
    def test_check_cache(self, check_cache_entries):
        input = mock.MagicMock()
        input.cache = None

        context = mock.MagicMock()
        context.inputs = [input,]

        new_context = preprocess.check_cache(context)

        self.assertEquals(new_context.inputs[0].cache,
                          check_cache_entries.return_value)

        check_cache_entries.assert_called_with(input, context)

    def test_map_axis_interval_exception(self):
        axis = mock.MagicMock()
        axis.mapInterval.side_effect = Exception()

        with self.assertRaises(WPSError):
            preprocess.map_axis_interval(axis, 20, 300)

    def test_map_axis_interval(self):
        axis = mock.MagicMock()
        axis.mapInterval.return_value = (20, 200)

        mapped = preprocess.map_axis_interval(axis, 20, 300)

        self.assertEqual(mapped, (20, 200)) 

        axis.mapInterval.assert_called_with((20, 300))

    def test_map_axis_indices_dimension_larger(self):
        axis = mock.MagicMock()
        axis.isTime.return_vale = False
        axis.__len__.return_value = 365

        dimension = cwt.Dimension('time', 20, 700, cwt.INDICES)

        selector = preprocess.map_axis_indices(axis, dimension)

        self.assertEqual(selector, slice(20, 365, 1))

        self.assertEqual(dimension.start, 0)
        self.assertEqual(dimension.end, 335)

    def test_map_axis_indices_dimension(self):
        axis = mock.MagicMock()
        axis.isTime.return_vale = False
        axis.__len__.return_value = 365

        dimension = cwt.Dimension('time', 20, 300, cwt.INDICES)

        selector = preprocess.map_axis_indices(axis, dimension)

        self.assertEqual(selector, slice(20, 300, 1))

        self.assertEqual(dimension.start, 0)
        self.assertEqual(dimension.end, 0)

    def test_map_axis_indices(self):
        axis = mock.MagicMock()
        axis.isTime.return_vale = False
        axis.__len__.return_value = 365

        selector = preprocess.map_axis_indices(axis, None)

        self.assertEqual(selector, slice(0, 365, 1))
    
    @mock.patch('wps.tasks.preprocess.map_axis_interval')
    def test_map_axis_values(self, map_axis_interval):
        map_axis_interval.return_value = (0, 365)

        axis = mock.MagicMock()

        dimension = cwt.Dimension('time', 0, 365)

        selector = preprocess.map_axis_values(axis, dimension)

        self.assertEqual(select, slice(0, 365, 1))

        map_axis_timestamps.assert_called_with(axis, 0, 365)

    @mock.patch('wps.tasks.preprocess.map_axis_interval')
    def test_map_axis_timestamps(self, map_axis_interval):
        map_axis_interval.return_value = (0, 365)

        axis = mock.MagicMock()

        dimension = cwt.Dimension('time', 'days since 2020', 'days since 2040')

        selector = preprocess.map_axis_timestamps(axis, dimension)

        self.assertEqual(select, slice(0, 365, 1))

        map_axis_timestamps.assert_called_with(axis, 'days since 2020', 
                                               'days since 2040')

    def test_map_axis_time_axis_no_units(self):
        axis = mock.MagicMock()
        axis.isTime.return_value = True

        dimension = cwt.Dimension('time', 0, 365, cwt.TIMESTAMPS)

        preprocess.map_axis(axis, dimension, None)

        axis.clone.assert_not_called()

    def test_map_axis_time_axis(self):
        axis = mock.MagicMock()
        axis.isTime.return_value = True

        dimension = cwt.Dimension('time', 0, 365, cwt.TIMESTAMPS)

        preprocess.map_axis(axis, dimension, 'days since 2020')

        axis.clone.assert_called()

        axis.clone.return_value.toRelativeTime.assert_called_with(
            'days since 2020')

    @mock.patch('wps.tasks.preprocess.map_axis_timestamps')
    def test_map_axis_timestamps(self, target):
        axis = mock.MagicMock()
        axis.isTime.return_value = False

        dimension = cwt.Dimension('time', 0, 365, cwt.TIMESTAMPS)

        preprocess.map_axis(axis, dimension, 'days since 2020')

        target.assert_called()

    @mock.patch('wps.tasks.preprocess.map_axis_values')
    def test_map_axis_values(self, target):
        axis = mock.MagicMock()
        axis.isTime.return_value = False

        dimension = cwt.Dimension('time', 0, 365, cwt.VALUES)

        preprocess.map_axis(axis, dimension, 'days since 2020')

        target.assert_called()

    @mock.patch('wps.tasks.preprocess.map_axis_indices')
    def test_map_axis_no_dimension(self, target):
        axis = mock.MagicMock()
        axis.isTime.return_value = False

        preprocess.map_axis(axis, None, 'days since 2020')

        target.assert_called()

    @mock.patch('wps.tasks.preprocess.map_axis_indices')
    def test_map_axis_indices(self, target):
        axis = mock.MagicMock()
        axis.isTime.return_value = False

        dimension = cwt.Dimension('time', 0, 365, cwt.INDICES)

        preprocess.map_axis(axis, dimension, 'days since 2020')

        target.assert_called()

    @mock.patch('wps.tasks.preprocess.map_axis_indices')
    def test_map_axis_unknown_crs(self, target):
        axis = mock.MagicMock()
        axis.isTime.return_value = False

        dimension = cwt.Dimension('time', 0, 365, cwt.CRS('test'))

        with self.assertRaises(WPSError):
            preprocess.map_axis(axis, dimension, 'days since 2020')

    def test_merge_dimensions_no_domain(self):
        context = mock.MagicMock()
        context.domain = None

        dimensions = ['time', 'lat', 'lon']

        uniq_dim = preprocess.merge_dimensions(context, dimensions)

        self.assertEqual(uniq_dim, set(dimensions))

    def test_merge_dimensions_not_in_file(self):
        time = mock.MagicMock()
        type(time).name = mock.PropertyMock(return_value='time')

        lat = mock.MagicMock()
        type(lat).name = mock.PropertyMock(return_value='lat')

        lon = mock.MagicMock()
        type(lon).name = mock.PropertyMock(return_value='lon')

        domain = mock.MagicMock()
        type(domain).dimensions = mock.PropertyMock(return_value = [
            time, lat, lon
        ])

        context = mock.MagicMock()
        context.domain = domain

        dimensions = ['time', 'lat']

        with self.assertRaises(WPSError):
            preprocess.merge_dimensions(context, dimensions)

    def test_merge_dimensions_not_in_user(self):
        time = mock.MagicMock()
        type(time).name = mock.PropertyMock(return_value='time')

        lat = mock.MagicMock()
        type(lat).name = mock.PropertyMock(return_value='lat')

        domain = mock.MagicMock()
        type(domain).dimensions = mock.PropertyMock(return_value = [
            time, lat
        ])

        context = mock.MagicMock()
        context.domain = domain

        dimensions = ['time', 'lat', 'lon']

        uniq_dim = preprocess.merge_dimensions(context, dimensions)

        self.assertEqual(uniq_dim, set(dimensions))

    def test_merge_dimensions(self):
        time = mock.MagicMock()
        type(time).name = mock.PropertyMock(return_value='time')

        lat = mock.MagicMock()
        type(lat).name = mock.PropertyMock(return_value='lat')

        lon = mock.MagicMock()
        type(lon).name = mock.PropertyMock(return_value='lon')

        domain = mock.MagicMock()
        type(domain).dimensions = mock.PropertyMock(return_value = [
            time, lat, lon
        ])

        context = mock.MagicMock()
        context.domain = domain

        dimensions = ['time', 'lat', 'lon']

        uniq_dim = preprocess.merge_dimensions(context, dimensions)

        self.assertEqual(uniq_dim, set(dimensions))

    @mock.patch('wps.tasks.preprocess.map_axis')
    @mock.patch('wps.tasks.preprocess.merge_dimensions')
    def test_map_domain_map_axis_exception(self, merge_dimensions, map_axis):
        merge_dimensions.return_value =[
            'time',
            'lat',
            'lon',
        ]

        map_axis.side_effect = Exception()

        input1 = mock.MagicMock()
        input1.mapped = {}

        context = mock.MagicMock()
        context.inputs = [input1,]
        context.domain = None

        new_context = preprocess.map_domain(context)

        self.assertIsNone(new_context.inputs[0].mapped)

    @mock.patch('wps.tasks.preprocess.map_axis')
    @mock.patch('wps.tasks.preprocess.merge_dimensions')
    def test_map_domain_map_axis_wpserror(self, merge_dimensions, map_axis):
        merge_dimensions.return_value =[
            'time',
            'lat',
            'lon',
        ]

        map_axis.side_effect = WPSError('')

        input1 = mock.MagicMock()
        input1.mapped = {}

        context = mock.MagicMock()
        context.inputs = [input1,]
        context.domain = None

        new_context = preprocess.map_domain(context)

        self.assertEqual(input1.mapped, None)

    @mock.patch('wps.tasks.preprocess.map_axis')
    @mock.patch('wps.tasks.preprocess.merge_dimensions')
    def test_map_domain_missing_axis(self, merge_dimensions, map_axis):
        merge_dimensions.return_value =[
            'time',
            'lat',
            'lon',
        ]

        var = mock.MagicMock()
        var.getAxisIndex.return_value = -1

        input1 = mock.MagicMock()
        input1.open.return_value.__enter__.return_value = var
        input1.mapped = {}

        context = mock.MagicMock()
        context.inputs = [input1,]
        context.domain = None

        with self.assertRaises(WPSError):
            new_context = preprocess.map_domain(context)

    @mock.patch('wps.tasks.preprocess.map_axis')
    @mock.patch('wps.tasks.preprocess.merge_dimensions')
    def test_map_domain_without_domain(self, merge_dimensions, map_axis):
        merge_dimensions.return_value =[
            'time',
            'lat',
            'lon',
        ]

        map_axis.side_effect = [
            slice(0, 365),
            slice(0, 180),
            slice(0, 360),
        ]

        input1 = mock.MagicMock()
        input1.mapped = {}

        context = mock.MagicMock()
        context.inputs = [input1,]
        context.domain = None

        new_context = preprocess.map_domain(context)

        expected1 = {
            'time': slice(0, 365),
            'lat': slice(0, 180),
            'lon': slice(0, 360),
        }

        self.assertEqual(new_context.inputs[0].mapped, expected1)

    @mock.patch('wps.tasks.preprocess.map_axis')
    @mock.patch('wps.tasks.preprocess.merge_dimensions')
    def test_map_domain(self, merge_dimensions, map_axis):
        merge_dimensions.return_value =[
            'time',
            'lat',
            'lon',
        ]

        map_axis.side_effect = [
            slice(0, 365),
            slice(0, 180),
            slice(0, 360),
        ]

        input1 = mock.MagicMock()
        input1.mapped = {}

        domain = mock.MagicMock()

        context = mock.MagicMock()
        context.inputs = [input1,]
        context.domain = domain

        new_context = preprocess.map_domain(context)

        expected1 = {
            'time': slice(0, 365),
            'lat': slice(0, 180),
            'lon': slice(0, 360),
        }

        domain.get_dimension.assert_any_call('time')
        domain.get_dimension.assert_any_call('lat')
        domain.get_dimension.assert_any_call('lon')

        self.assertEqual(new_context.inputs[0].mapped, expected1)

    def test_base_units_no_time(self):
        var = mock.MagicMock()
        var.getTime.return_value = None

        input = mock.MagicMock()
        input.open.return_value.__enter__.return_value = var
        input.units = None
        input.first = None

        context = mock.MagicMock()
        context.inputs = [input,]
        context.units = None

        new_context = preprocess.base_units(context)

        self.assertEqual(new_context.units, None)

        self.assertEqual(new_context.inputs[0].units, None)
        self.assertEqual(new_context.inputs[0].first, None)

    def test_base_units(self):
        time = mock.MagicMock()
        type(time).units = mock.PropertyMock(return_value='days since 2000')
        time.__getitem__.return_value = 100

        var = mock.MagicMock()
        var.getTime.return_value = time

        input = mock.MagicMock()
        input.open.return_value.__enter__.return_value = var

        context = mock.MagicMock()
        context.inputs = [input,]

        new_context = preprocess.base_units(context)

        self.assertEqual(new_context.units, 'days since 2000')

        self.assertEqual(new_context.inputs[0].units, 'days since 2000')
        self.assertEqual(new_context.inputs[0].first, 100)

    def test_filter_inputs(self):
        expected = [4, 3, 3]

        with self.settings(WORKER_PER_USER=3):
            for index, x in enumerate(range(3)):
                context = mock.MagicMock()        
               
                context.inputs.__len__.return_value = 10

                new_context = preprocess.filter_inputs(context, index)

                self.assertEqual(len(new_context.inputs), expected[index])
