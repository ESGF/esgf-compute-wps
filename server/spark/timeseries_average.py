import cdutil, cdms2
from pyspark import SparkContext

inputSpec = {'url': '/usr/local/web/data/MERRA/u750/merra_u750.xml', 'id': 'u', 'start': 1979}
location = {'latitude': -40.0, 'longitude': 50.0}
num_years = 3

def loadPartition( partition_index ):
    f=cdms2.open( inputSpec['url'] )
    variable = f[ inputSpec['id'] ]
    start_year = inputSpec['start'] + partition_index
    return variable( time=( '%d-1'%(start_year), '%d-1'%(start_year+1), 'co') )

def timeseries_average( data_slice ):
    lat, lon = location['latitude'], location['longitude']
    timeseries = data_slice(latitude=(lat, lat, "cob"), longitude=(lon, lon, "cob"))
    return cdutil.averager( timeseries, axis='t', weights='equal' ).squeeze().tolist()

sc = SparkContext('local[%d]' % num_years, "cdmsTest")
partitions = sc.parallelize( range(num_years) )
cdmsData = partitions.map(loadPartition)
yearly_averages = cdmsData.map( timeseries_average ).collect()
print " Yearly averages: ", str(yearly_averages)




