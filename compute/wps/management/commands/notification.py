from django.core.management.base import BaseCommand, CommandError
from wps import models

class Command(BaseCommand):
    help = 'Creates a new server notification message'

    def add_arguments(self, parser):
        parser.add_argument('action', choices=('create', 'list', 'disable'), type=str)

        parser.add_argument('--username', type=str)
        parser.add_argument('--message', type=str)

        parser.add_argument('--id', type=int)

    def __get_required_option(self, key, options):
        value = options[key]

        if value == None:
            raise CommandError('Option "{}" is required for action "{}"'.format(key, options['action']))

        return value

    def handle(self, *args, **options):
        command_action = options['action']

        if command_action == 'create':
            username = self.__get_required_option('username', options)

            try:
                user = models.User.objects.get(username=username)
            except models.User.DoesNotExist:
                raise CommandError('User "{}" does not exist'.format(username))

            for no in models.Notification.objects.filter(enabled=True):
                no.enabled = False

                no.save()

            models.Notification.objects.create(user=user, message=self.__get_required_option('message', options))

            self.stdout.write(self.style.SUCCESS('Successfully created new notification'))
        elif command_action == 'list':
            notifications = models.Notification.objects.all()

            for no in notifications:
                self.stdout.write(self.style.NOTICE('{}\t{}\t{}\t{}\t"{}"'.format(no.pk, no.enabled, no.user.username, no.created_date.strftime('%x %X'), no.message)))
        elif command_action == 'disable':
            pk = self.__get_required_option('id', options)

            try:
                notification = models.Notification.objects.get(pk=pk)
            except models.Notification.DoesNotExist:
                raise CommandError('Notification with id "{}" does not exist'.format(pk))

            notification.enabled = False;

            notification.save()

            self.stdout.write(self.style.SUCCESS('Successfully disabled notification {} "{}"'.format(pk, notification.message)))
