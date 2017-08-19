class Plugin:
    def __init__(self):
        self.fd = open('/tmp/events', 'w')

    def initialize(self, offset_store):
        self.offset_store = offset_store
        self.offset_store.on_new_consumer(self.handle_new_consumer)

    def log_message(self, message):
        self.fd.write(message + '\n')
        self.fd.flush()

    def handle_new_consumer(self, group_id):
        self.log_message('New consumer {0} found'.format(group_id))
        self.offset_store.on_consumer_commit(group_id, self.handle_consumer_commit)

    def handle_consumer_commit(self, group_id):
        self.log_message('Consumer {0} committed'.format(group_id))
