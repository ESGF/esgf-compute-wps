# FAQ

## How can I access the compute service?

The WPS endpoint is available is available [here](https://aims2.llnl.gov/wps/). You can acccess the service through the [web application](https://aims2.llnl.gov/) or through the [end-user api](https://github.com/ESGF/esgf-compute-api/blob/master/examples/getting_started.ipynb). You will need to sign into the web application to retrieve an api key. If you're accessing protected data you will need to authorization the compute service to request a certificate on your behalf, this is covered in the getting started [document](https://github.com/ESGF/esgf-compute-api/blob/master/examples/getting_started.ipynb).

## What type of data can the compute service consume?

The compute service is expecting CF compliant data that is available through an OpenDAP URL.

## I'm not sure how to use the web application.

On the top left of the page you will find a help button that will present a little walk through. If you're still experiecing issues or have a question please open an issue on [GitHub](https://github.com/ESGF/esgf-compute-wps/issues/new)

## I'm trying to add a new DataSet in the web application but nothing is happening!

This input is not expecting a URL (feature that is being worked on), rather it is expecting the "id" of a DataSet found through ESGF search results.

## The output of the operation has missing data.

If your input data is CMIP5, CMIP3 or any data that requires authorization, this may be caused by a known issue and retrying the same operation may result in success. If the following is true you may track the status of the issue [here](https://github.com/ESGF/esgf-compute-wps/issues/212) otherwise report a new [bug](https://github.com/ESGF/esgf-compute-wps/issues/new?template=Bug_report.md).

## I've got a question about the compute service!

Please open an issue using [GitHub](https://github.com/ESGF/esgf-compute-wps/issues/new)

## I've found a bug!

Please report this bug using [GitHub](https://github.com/ESGF/esgf-compute-wps/issues/new?template=Bug_report.md)

## I've got an idea for a feature!

Please create a feature request using [GitHub](https://github.com/ESGF/esgf-compute-wps/issues/new?template=Feature_request.md)
