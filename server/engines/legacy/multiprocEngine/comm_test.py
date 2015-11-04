from multiprocessing import Process, Pipe
from modules.utilities import *
import cPickle, traceback

if __name__ == "__main__":
    import sys, cdms2
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)
    wid1 = "W-1"
    wid2 = "W-2"

    def worker_exe1( wid, comm ):
        shape = (10,10,1)
        ( task, intercomm ) = comm.recv()
        print "[%s] Received instructions: %s" % ( wid, task )
        if task == "msg.out":
            msg =  [ wid, shape ]
            print "Sending message from worker %s: %s" % ( wid, shape )
            intercomm.send( msg )


    def worker_exe2( wid, comm ):
        ( task, intercomm ) = comm.recv()
        print "[%s] Received instructions: %s" % ( wid, task )
        if task == "msg.in":
            [ rwid, shape ] =  intercomm.recv()
            print "Received message from worker %s: %s" % ( rwid, shape )


    source, destination = Pipe()
    local_comm1, remote_comm1 = Pipe()
    worker_process1 = Process( target=worker_exe1, name=wid1, args=(wid1,remote_comm1) )
    local_comm2, remote_comm2 = Pipe()
    worker_process2 = Process(target=worker_exe2, name=wid2, args=(wid2,remote_comm2) )

    worker_process1.start()
    worker_process2.start()

    local_comm1.send( ( "msg.out", 'destination' ) )
    local_comm2.send( ( "msg.in", 'source' ) )

    worker_process1.join()
