import mock
import random
from django import test
from django.conf import settings
from django.utils import timezone

from . import helpers
from wps import models
from wps.tasks import cache

class CacheTaskTestCase(test.TestCase):

    def setUp(self):
        for _ in range(10):
            models.Cache.objects.create(uid=helpers.random_str(10), size=random.uniform(0.5, 10.0))

    def test_setup_period_tasks(self):
        mock_sender = mock.MagicMock()

        cache.setup_periodic_tasks(mock_sender)

        mock_sender.add_periodic_task.assert_called()

    @mock.patch('wps.tasks.cache.os.remove')
    @mock.patch('wps.tasks.cache.os.path.exists')
    def test_cache_free_space(self, mock_exists, mock_remove):
        mock_exists.return_value = True

        new_size = settings.WPS_CACHE_GB_MAX_SIZE / 10

        cached = models.Cache.objects.all()

        for item in cached:
            item.size = new_size

            item.save()

        # Cache should technically be full we'll add two smaller items and check if they are removed
        for _ in range(2):
            models.Cache.objects.create(uid=helpers.random_str(10), size=new_size/2)

        with self.assertNumQueries(7):
            cache.cache_clean()

        remain_count = models.Cache.objects.count()

        self.assertEqual(remain_count, 9)

    @mock.patch('wps.tasks.cache.os.remove')
    @mock.patch('wps.tasks.cache.os.path.exists')
    def test_cache_delete_expired(self, mock_exists, mock_remove):
        mock_exists.return_value = True

        cached = random.sample(models.Cache.objects.all(), 4)

        for item in cached:
            models.Cache.objects.filter(pk=item.pk).update(accessed_date=timezone.now() - settings.WPS_CACHE_MAX_AGE - timezone.timedelta(days=30))

        with self.assertNumQueries(7):
            cache.cache_clean()

        remain_count = models.Cache.objects.count()

        self.assertEqual(remain_count, 6)

    @mock.patch('wps.tasks.cache.os.remove')
    @mock.patch('wps.tasks.cache.os.path.exists')
    def test_cache_remove_files_not_on_disk(self, mock_exists, mock_remove):
        mock_remove.return_value = True

        mock_exists.side_effect = [
            False, False, False, False, False, False, False, False, False, False,
            False, True, False, True, True, True, False, False, False, False, False
        ]

        with self.assertNumQueries(11):
            cache.cache_clean()

        remain_count = models.Cache.objects.count()

        self.assertEqual(remain_count, 2)
