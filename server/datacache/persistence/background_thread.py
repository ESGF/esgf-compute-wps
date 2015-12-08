import threading, time, re
from manager import getDataPersistenceEngine
from modules.utilities import  *

class PersistenceThread(threading.Thread):
    IDLE = 0
    BUSY = 1

    def __init__(self, group=None, target=None, name='PersistenceThread', args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        self.persistenceManager = getDataPersistenceEngine()
        self.state = PersistenceThread.IDLE
        self.active = True
        self.wake_count = 0
        self.sleep_increment = 600.0
        self.acceptable_wake_count = 2
        self.cache_queue = self.args[0]

    def setOpStateIdle(self):
        self.state = PersistenceThread.IDLE

    def setOpStateBusy(self):
        self.state = PersistenceThread.BUSY
        self.wake_count = 0

    def terminate(self):
        self.active = False

    def run(self):
        while self.active:
            time.sleep(self.sleep_increment)
            if self.state == PersistenceThread.IDLE:
                if self.wake_count >= self.acceptable_wake_count:
                    while( not self.cache_queue.empty() ):
                        if self.state == PersistenceThread.IDLE:
                            ( data, stat ) = self.cache_queue.get()
                            cid = stat.get('cid',None)
                            if cid:
                                stat['persist_id'] = '_'.join([ re.sub("[/:]","_",cid), str(int(10*time.time()))])
                                self.persistenceManager.store(data,stat)
                                wpsLog.debug( " \n ----- Persisting data chunk[ %s ], stat: %s ----- \n " % ( cid, str(stat) ) )
                            else:
                                wpsLog.error( "ERROR, no cid for data chunk: %s" % str( stat ) )
                        else: break
                else:
                    self.wake_count = self.wake_count + 1






#  t = PersistenceThread(args=(q,), kwargs={'a':'A', 'b':'B'})
#  t.start()