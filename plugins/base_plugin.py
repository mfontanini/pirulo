class BasePlugin:
    def __init__(self):
        self.offset_store = None
        self._track_consumer_commits = False
        self._track_topic_message = False

    def initialize(self, offset_store):
        self.offset_store = offset_store

    def subscribe_to_consumers(self):
        self.offset_store.on_new_consumer(self._handle_new_consumer)
        for group_id in self.offset_store.get_consumers():
            self._handle_new_consumer(group_id)

    def subscribe_to_consumer_commits(self):
        if not self._track_consumer_commits:
            for group_id in self.offset_store.get_consumers():
                self.offset_store.on_consumer_commit(group_id, self._handle_consumer_commit)
        self._track_consumer_commits = True

    def subscribe_to_topics(self):
        self.offset_store.on_new_topic(self._handle_new_topic)
        for topic in self.offset_store.get_topics():
            self._handle_new_topic(topic)

    def subscribe_to_topic_message(self):
        if not self._track_topic_message:
            for topic in self.offset_store.get_topics():
                self.offset_store.on_topic_message(topic, self._handle_topic_message)
        self._track_topic_message = True

    def _handle_new_consumer(self, group_id):
        if self._track_consumer_commits:
            self.offset_store.on_consumer_commit(group_id, self._handle_consumer_commit)
        self.handle_new_consumer(group_id)

    def _handle_consumer_commit(self, group_id, topic, partition, offset):
        self.handle_consumer_commit(group_id, topic, partition, offset)

    def _handle_new_topic(self, topic):
        if self._track_topic_message:
            self.offset_store.on_topic_message(topic, self._handle_topic_message)
        self.handle_new_topic(topic)

    def _handle_topic_message(self, topic, partition, offset):
        self.handle_topic_message(topic, partition, offset)
