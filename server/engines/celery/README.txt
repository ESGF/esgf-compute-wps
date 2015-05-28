Installation notes:

pip install -U Celery
brew install rabbitmq
pip install eventlet
pip install librabbitmq
pip install flower

#  Add PATH=$PATH:/usr/local/sbin to your .bash_profile or .profile
#  To adjust the per-user limit for RabbitMQ, there are several options:
#  Invoke ulimit -S -n 4096 before starting RabbitMQ in foreground.
#  Edit the rabbitmq-env.conf to invoke ulimit before the service is started.
#  Configure max open files limit via launchctl limit /etc/launchd.conf.


