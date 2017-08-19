class Plugin:
    def __init__(self):
        self.fd = open('/tmp/events', 'w')

    def initialize(self, offset_store):
        self.offset_store = offset_store
        self.offset_store.on_new_consumer(self.handle_new_consumer)

    def handle_new_consumer(self, group_id):
        self.fd.write('New consumer {0} found\n'.format(group_id))
        self.fd.flush()
