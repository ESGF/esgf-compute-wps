#!/lgm/uvcdat/2015-01-21/bin/python
# EASY-INSTALL-SCRIPT: 'pywps==3.2.2','wps.py'
__requires__ = 'pywps==3.2.2'
import pkg_resources, time, logging
wpsLog = logging.getLogger( 'wps' )
start_time = time.time()
pkg_resources.run_script('pywps==3.2.2', 'wps.py')
end_time = time.time()
wpsLog.debug( " #################  ################# wps.py script run time: %.5f  #################  ################# " % (end_time-start_time) )
