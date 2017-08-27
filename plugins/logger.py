import web
import threading
import time
import sys
from pirulo import Handler, LagTrackerHandler

class TopicsHandler:
    def GET(self):
        return Plugin.INSTANCE.topics

class ConsumersHandler:
    def GET(self):
        return Plugin.INSTANCE.consumers

class Plugin(LagTrackerHandler):
    pass

class Plugina(Handler):
    INSTANCE = None

    def __init__(self):
        Handler.__init__(self)
        Plugin.INSTANCE = self
        self.fd = open('/tmp/events', 'w')
        self.topics = []
        self.consumers = []
        self.thread = threading.Thread(target=self.launch_server)
        self.thread.start()

    def handle_initialize(self, offset_store):
        self.subscribe_to_consumers()
        self.subscribe_to_consumer_commits()
        self.subscribe_to_topics()
        self.subscribe_to_topic_message()

    def launch_server(self):
        try:
            urls = (
                '/topics', 'TopicsHandler',
                '/consumers', 'ConsumersHandler',
            )
            sys.argv = []
            app = web.application(urls, globals())
            app.run()
        except Exception as ex:
            print 'Failed running server: ' + str(ex)

    def log_message(self, message):
        self.fd.write(message + '\n')
        self.fd.flush()

    def handle_new_consumer(self, group_id):
        self.log_message('New consumer {0} found'.format(group_id))
        self.consumers.append(group_id)

    def handle_new_topic(self, topic):
        self.log_message('Found topic {0}'.format(topic))
        self.topics.append(topic)

    def handle_consumer_commit(self, group_id, topic, partition, offset):
        self.log_message('Consumer {0} committed to {1}/{2} offset {3}'.format(
            group_id,
            topic,
            partition,
            offset 
        ))

    def handle_topic_message(self, topic, partition, offset):
        self.log_message('New offset for topic {0}/{1} at offset {2}'.format(
            topic,
            partition,
            offset
        ))
