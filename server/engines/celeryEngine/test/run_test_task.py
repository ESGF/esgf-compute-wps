from tasks import simpleTest
from utilities import Profiler

profiler = Profiler()
profiler.mark()

result = simpleTest.delay( range( 10 ) ).get()
profiler.mark('simpleTest')
result = simpleTest.delay( range( 10 ) ).get()
profiler.mark('simpleTest')
result = simpleTest.delay( range( 10 ) ).get()
profiler.mark('simpleTest')
result = simpleTest.delay( range( 10 ) ).get()
profiler.mark('simpleTest')
result = simpleTest.delay( range( 10 ) ).get()
profiler.mark('simpleTest')
result = simpleTest.delay( range( 10 ) ).get()
profiler.mark('simpleTest')


profiler.dump( " Received Result with times:" )
print " Result: %s" %  str( result )


