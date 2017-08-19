class Plugin:
    def __init__(self):
        self.fd = open('/tmp/events', 'w')

    def initialize(self, offset_store):
        self.offset_store = offset_store
        self.offset_store.on_new_consumer(self.handle_new_consumer)
        self.offset_store.on_new_topic(self.handle_new_topic)
        for topic in self.offset_store.get_topics():
            self.handle_new_topic(topic)

    def log_message(self, message):
        self.fd.write(message + '\n')
        self.fd.flush()

    def handle_new_consumer(self, group_id):
        self.log_message('New consumer {0} found'.format(group_id))
        self.offset_store.on_consumer_commit(group_id, self.handle_consumer_commit)

    def handle_new_topic(self, topic):
        self.log_message('Found topic {0}'.format(topic))
        self.offset_store.on_topic_message(topic, self.handle_topic_message)

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
