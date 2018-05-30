import { Http, Headers, URLSearchParams, QueryEncoder } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Axis } from './axis.component';
import { Parameter } from './parameter.component';
import { RegridModel } from './regrid.component';
import { WPSService, WPSResponse } from '../core/wps.service';
import { AuthService } from '../core/auth.service';

export const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
export const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

export class Process {
  uid: string;

  constructor(
    public identifier: string = '',
    public inputs: (Variable | Process)[] = [],
    public domain: Axis[] = [],
    public regrid: RegridModel = new RegridModel(),
    public parameters: any[] = [],
  ) { 
    this.uid = Math.random().toString(16).slice(2); 
  }

  get uuid() {
    return Math.random().toString(16).slice(2);
  }

  newUID() {
    return Math.random().toString(16).slice(2);
  }

  setInputs(inputs: (Variable | Process)[]) {
    this.inputs = inputs;
  }

  validate() { 
    if (this.inputs.length === 0) {
      throw `Process "${this.identifier} - ${this.uid}" must have atleast one input`;
    }

    if (this.regrid.regridType !== 'None') {
      if (this.regrid.regridType === 'Uniform') {
        if (this.regrid.lons === 0) {
          throw `Regrid option "${this.regrid.regridType}" requires Longitude to be set`;
        }
      }

      if (this.regrid.lats === 0) {
        throw `Regrid option "${this.regrid.regridType}" require Latitude to be set`;
      }
    }

    this.parameters.forEach((param: Parameter) => {
      if (param.key === '') {
        throw 'Parameter has an empty key';
      }

      if (param.value === '') {
        throw `Parameter "${param.key}" has an empty value`;
      }
    });
  }

  prepareDataInputs() {
    let operation = {};
    let domain = {};
    let variable = {};

    let regrid = null;

    // define the global domain
    let domainID = this.newUID();
    let gDomain: {[k: string]: any} = { id: domainID };

    this.domain.forEach((axis: Axis) => {
      gDomain[axis.id] = {
        start: axis.start,
        end: axis.stop,
        step: axis.step,
        crs: 'values'
      };
    });

    // defin the global regrid options
    if (this.regrid.regridType !== 'None') {
      regrid = { tool: 'esmf', method: 'linear' };

      if (this.regrid.regridType === 'Gaussian') {
        regrid['grid'] = `gaussian~${this.regrid.lats}`;
      } else if (this.regrid.regridType === 'Uniform') {
        regrid['grid'] = `uniform~${this.regrid.lats}x${this.regrid.lons}`;
      }
    }

    domain[domainID] = gDomain;

    // DFS stack
    let stack: Process[] = [this];

    // Run DFS to build operation, variable list
    while (stack.length > 0) {
      let curr = stack.pop();

      // Check if we've visited this node
      if (operation[curr.uid] === undefined) {
        // Validate node
        curr.validate();

        // Only merge if we're not the root node
        if (curr !== this) {
          // Merge global parameters
          Array.prototype.push.apply(curr.parameters, this.parameters);
        }

        // Define the operation, use {[k: string]: any} to define a very generic,
        // compact type
        let op: {[k: string]: any} = {
          name: curr.identifier,
          result: curr.uid,
          domain: gDomain.id,
          input: [],
        };

        // set the gridder
        if (regrid !== null) {
          op['gridder'] = regrid;
        }

        // Add all parameters
        curr.parameters.forEach((para: Parameter) => {
          op[para.key] = para.value; 
        });

        // Split inputs into variables and processes
        let vars = curr.inputs.filter((item: any) => { return !(item instanceof Process); });
        let procs = curr.inputs.filter((item: any) => { return (item instanceof Process); });

        // Add each input variable to the global variable list
        vars.forEach((item: Variable) => {
          // Each file gets an entry
          item.files.forEach((data: string, i: number) => {
            let uid = `${item.uid}-${i}`;

            // Add string reference to input list
            op.input.push(uid);

            // Add variable to global list if it doesn't already exist
            if (variable[uid] === undefined) {
              variable[uid] = {
                id: `${item.id}|${uid}`,
                uri: data,
              };
            }
          });
        });

        // DFS haven't visited this node, push process inputs onto stack
        procs.forEach((item: Process) => {
          // Add string reference to input list
          op.input.push(item.uid);

          stack.push(item);
        });

        // Add unvisited operation to global list
        operation[op.result] = op;
      }
    }

    let operationJSON = JSON.stringify(Object.keys(operation).map((key: string) => { return operation[key]; }));
    let domainJSON = JSON.stringify(Object.keys(domain).map((key: string) => { return domain[key]; }));
    let variableJSON = JSON.stringify(Object.keys(variable).map((key: string) => { return variable[key]; }));

    return {
      operation: operationJSON,
      domain: domainJSON,
      variable: variableJSON
    };
  }

  prepareDataInputsString() {
    let dataInputs = this.prepareDataInputs();

    return `[operation=${dataInputs.operation}|domain=${dataInputs.domain}|variable=${dataInputs.variable}]`;
  }

  createAttribute(doc: any, node: any, name: string, value: string) {
    let attribute = doc.createAttribute(name);

    attribute.value = value;

    node.setAttributeNode(attribute);
  }

  createAttributeNS(doc: any, node: any, ns: string, name: string, value: string) {
    let attribute = doc.createAttributeNS(ns, name);

    attribute.value = value;

    node.setAttributeNode(attribute);
  }

  createElementNS(doc: any, node: any, ns: string, name: string, value: string = null) {
    let newNode = doc.createElementNS(ns, name);

    if (value != null) {
      newNode.innerHTML = value;
    }

    node.appendChild(newNode);

    return newNode;
  }

  prepareDataInputsXML() {
    const WPS_NS = 'http://www.opengis.net/wps/1.0.0';
    const OWS_NS = 'http://www.opengis.net/ows/1.1';
    const XLINK_NS = 'http://www.w3.org/1999/xlink';
    const XSI_NS = 'http://www.w3.org/2001/XMLSchema-instance';
    const SCHEMA_LOCATION = 'http://www.opengis.net/wps/1.0.0/wpsExecute_request.xsd';

    let dataInputs = this.prepareDataInputs();

    let doc = document.implementation.createDocument(WPS_NS, 'wps:Execute', null);

    let root = doc.documentElement;

    this.createAttribute(doc, root, 'service', 'WPS');
    this.createAttribute(doc, root, 'version', '1.0.0');

    this.createElementNS(doc, root, OWS_NS, 'ows:Identifier', this.identifier);

    let dataInputsElement = this.createElementNS(doc, root, WPS_NS, 'wps:DataInputs');

    for (let key in dataInputs) {
      let inputElement = this.createElementNS(doc, dataInputsElement, WPS_NS, 'wps:Input');

      this.createElementNS(doc, inputElement, OWS_NS, 'ows:Identifier', key);
      
      this.createElementNS(doc, inputElement, OWS_NS, 'ows:Title', key);

      let dataElement = this.createElementNS(doc, inputElement, WPS_NS, 'wps:Data');

      this.createElementNS(doc, dataElement, WPS_NS, 'wps:LiteralData', dataInputs[key]);
    }

    return new XMLSerializer().serializeToString(doc.documentElement);
  }
}

export interface DatasetCollection { 
  [index: string]: Dataset;
}

export class Dataset {
  constructor(
    public id: string,
    public variables: Variable[] = []
  ) { }
}

export class Variable {
  uid: string;

  constructor(
    public id: string,
    public axes: Axis[] = [],
    public files: string[] = [],
    public dataset: string = '',
  ) { 
    this.uid = Math.random().toString(16).slice(2); 
  }
}

export interface VariableCollection {
  [index: string]: Variable;
}

export interface RegridOptions {
  lats: number;
  lons: number;
}

export class Configuration {
  process: Process;
  dataset: Dataset;
  variable: Variable;

  // ESGF search parameters
  datasetID: string;
  indexNode: string;
  query: string;
  shard: string;

  constructor() { 
    this.process = new Process();
  }
}

@Injectable()
export class ConfigureService extends WPSService {
  constructor(
    http: Http,
    private authService: AuthService,
  ) { 
    super(http); 
  }

  processes(): Promise<string[]> {
    return this.get('/wps/processes')
      .then(response => {
        return response.data;
      });
  }

  searchESGF(config: Configuration): Promise<Variable[]> { 
    let params = new URLSearchParams();

    params.append('dataset_id', config.dataset.id);
    params.append('index_node', config.indexNode);
    params.append('query', config.query);
    params.append('shard', config.shard);

    return this.get('/wps/search', params)
      .then(response => {
        return Object.keys(response.data).map((key: string) => {
          let variable = response.data[key];

          return new Variable(variable.id, variable.axes, variable.files);
        });
      });
  }

  searchVariable(config: Configuration): Promise<Axis[]> {
    let params = new URLSearchParams();

    params.append('dataset_id', config.dataset.id);
    params.append('index_node', config.indexNode);
    params.append('query', config.query);
    params.append('shard', config.shard);
    params.append('variable', config.variable.id);

    return this.get('/wps/search/variable', params)
      .then(response => {
        return response.data as Axis[];
      });
  }

  execute(process: Process): Promise<string> {
    let preparedData: string;

    try {
      //preparedData = process.prepareDataInputsString();
      preparedData = process.prepareDataInputsXML();
    } catch (e) {
      return Promise.reject(e);
    }

    let params = new URLSearchParams();

    //params.append('service', 'WPS');
    //params.append('request', 'execute');
    params.append('api_key', this.authService.user.api_key);
    //params.append('identifier', process.identifier);
    //params.append('datainputs', preparedData);

    //return this.getUnmodified('/wps', params)
    //  .then(response => {
    //    return response.text(); 
    //  });

    return this.postCSRFUnmodified('/wps/', preparedData, params=params)
      .then(response => {
        return response.text();
      });
  }

  downloadScript(process: Process): Promise<any> {
    let preparedData: string;
    
    try {
      preparedData = process.prepareDataInputsString();
    } catch (e) {
      return Promise.reject(e);
    }

    let data = `datainputs=${preparedData}`;

    return this.postCSRF('/wps/generate/', data)
      .then(response => {
        return response.data;
      });
  }
}
