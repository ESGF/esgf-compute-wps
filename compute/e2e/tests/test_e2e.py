import cwt

def test_cmip5(wps_client):
    files = [
        'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esg_dataroot/AR5/CMIP5/output/CCCma/CanAM4/amipFuture/day/atmos/cl/r1i1p1/cl_cfDay_CanAM4_amipFuture_r1i1p1_19790101-19881231.nc',
        'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esg_dataroot/AR5/CMIP5/output/CCCma/CanAM4/amipFuture/day/atmos/cl/r1i1p1/cl_cfDay_CanAM4_amipFuture_r1i1p1_19890101-19981231.nc',
    ]

    inputs = [cwt.Variable(x, 'cl') for x in files]

    aggregate = wps_client.process_by_name('CDAT.aggregate')

    aggregate.add_inputs(*inputs)

    subset = wps_client.process_by_name('CDAT.subset')

    subset.add_inputs(aggregate)

    subset.set_domain(cwt.Domain(time=('19898-01-01', '1989-01-01')))

    max = wps_client.process_by_name('CDAT.max')

    max.add_inputs(subset)

    max.add_parameters(axes='time')

    workflow = wps_client.process_by_name('CDAT.workflow')

    workflow.add_inputs(max)

    wps_client.execute(workflow)

    assert workflow.wait(timeout=180)


def test_cmip6(wps_client):
    files = [
        'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/DAMIP/NCAR/CESM2/hist-nat/r2i1p1f1/3hr/pr/gn/v20191106/pr_3hr_CESM2_hist-nat_r2i1p1f1_gn_185001010000-185912312100.nc',
        'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/DAMIP/NCAR/CESM2/hist-nat/r2i1p1f1/3hr/pr/gn/v20191106/pr_3hr_CESM2_hist-nat_r2i1p1f1_gn_186001010000-186912312100.nc',
    ]

    inputs = [cwt.Variable(x, 'pr') for x in files]

    aggregate = wps_client.process_by_name('CDAT.aggregate')

    aggregate.add_inputs(*inputs)

    subset = wps_client.process_by_name('CDAT.subset')

    subset.add_inputs(aggregate)

    subset.set_domain(cwt.Domain(time=('1859-01-01', '1860-01-01')))

    max = wps_client.process_by_name('CDAT.max')

    max.add_inputs(subset)

    max.add_parameters(axes='time')

    workflow = wps_client.process_by_name('CDAT.workflow')

    workflow.add_inputs(max)

    wps_client.execute(workflow)

    assert workflow.wait(timeout=180)
