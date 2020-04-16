# ESGF Compute
The ESGF Compute Service is a containerized application capable of providing compute resources through a web service, using the [Web Processing Service (WPS)](http://www.opengeospatial.org/standards/wps) standard as an interface between the user and the compute resources. Currently version 1.0.0 of the WPS standard is supported, with 2.0.0 in the near future.

The end goal is to provide a federated service for ESGF that brings the computation to the data.

Table of Contents
=================

* [Building](#building)
* [Testing](#testing)
* [Installation](#installation)
* [Contribute](#contribute)
* [Question?](#question)

# Building

These commands will build the production stage of the containers. A buildkit container is used to build the containers unless buildkit is installed locally.

* WPS `make build-wps`
* Tasks `make build-tasks`
* Provisioner `make build-provisioner`
* THREDDS `make build-thredds`

# Provenance

Information about provenance can be found [here](PROVENANCE.md)

# Testing

These commands will build the testing stage of the containers. The unittest results and coverage reports will be located in the `/testing` directory.

* WPS `make build-wps TARGET=testing`
* Tasks `make build-wps TARGET=testing`

The following commands will run the containers, mount the local source and open a shell.

* WPS `make run-wps`
* Tasks `make run-tasks`

# Installation

The deployment instructions and Helm chart can be found at [esgf-compute/charts](https://github.com/esgf-compute/charts).

# Contribute
We welcome contributions to the project, before moving ahead please review the following documents:

* [Contributing Guide](CONTRIBUTING.md)
* [Developers Guide](DEVELOPER.md)

# Question?
Please review the [FAQ](FAQ.md), if you do not find an answer to your question open an issue on [GitHub](https://github.com/ESGF/esgf-compute-wps/issues/new).

# Design documents
Coming soon.
