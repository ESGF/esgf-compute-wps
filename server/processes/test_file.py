import cdms2, cdutil
import os
cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)

dataset_path = '/usr/local/web/data/MERRA/u750/merra_u750.xml'
varname = 'u'
start_year = 1979
num_years = 3

if __name__ == "__main__":
    end_year = start_year + num_years
    dataset = cdms2.open( dataset_path, "r" )
    variable = dataset(varname,time=( '%d-1'%start_year, '%d-1'%end_year ))
    data_dir = os.path.dirname( dataset_path )
    fout = os.path.join( data_dir, dataset.id + "_%d_%d.nc"%(start_year,end_year) )
    print "Writing %d years of variable %s to output file '%s'" % ( num_years, varname, fout )
    f=cdms2.open(fout,"w")
    f.write(variable)
    f.close()
    print "Done!"
