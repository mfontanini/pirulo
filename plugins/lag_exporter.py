from pirulo import LagTrackerHandler

class Plugin(LagTrackerHandler):
    def __init__(self):
        LagTrackerHandler.__init__(self)
        Plugin.INSTANCE = self

    def handle_lag_update(self, topic, partition, group_id, lag):
        print 'Consumer {0} has {1} lag on {2}/{3}'.format(group_id, lag, topic, partition)

def create_plugin():
    return Plugin()
