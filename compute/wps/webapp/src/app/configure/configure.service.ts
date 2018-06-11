import { Http, Headers, URLSearchParams, QueryEncoder } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Axis } from './axis.component';
import { Parameter } from './parameter.component';
import { RegridModel } from './regrid.component';
import { WPSService, WPSResponse } from '../core/wps.service';
import { AuthService } from '../core/auth.service';
import { ConfigService } from '../core/config.service';

export const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
export const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

export class Process {
  uid: string;

  constructor(
    public identifier: string = '',
    public inputs: (Variable | Process)[] = [],
    public domain: Axis[] = [],
    public domainPreset: string = 'World',
    public regrid: RegridModel = new RegridModel('ESMF', 'Linear', 'None', null, null, 3.0, null, null, 4.0),
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

  addInput(input: Variable) {
    this.inputs.push(input);
  }

  removeInput(input: Variable) {
    this.inputs = this.inputs.filter((x: Variable) => {
      return x.uid != input.uid;
    });
  }

  setInputs(inputs: (Variable | Process)[]) {
    this.inputs = inputs;
  }

  validate() { 
    if (this.inputs.length === 0) {
      throw `Process "${this.identifier} - ${this.uid}" must have atleast one input`;
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

  buildRegrid(regrid: RegridModel) {
    let data = {
      tool: regrid.regridTool,
      method: regrid.regridMethod,
    };

    if (regrid.regridType === 'Gaussian') {
      data['grid'] = `gaussian~${regrid.nLats}`;
    } else if (regrid.regridType === 'Uniform') {
      let lats = '';
      let lons = '';

      if (regrid.startLats != null && regrid.deltaLats != null) {
        lats = `${regrid.startLats}:${regrid.nLats}:${regrid.deltaLats}` 
      } else {
        lats = `${regrid.deltaLats}`
      }

      if (regrid.startLons != null && regrid.deltaLons != null) {
        lons = `${regrid.startLons}:${regrid.nLons}:${regrid.deltaLons}`
      } else {
        lons = `${regrid.deltaLons}`
      }

      data['grid'] = `uniform~${lats}x${lons}`;
    }

    return data;
  }

  buildDomain(axes: Axis[]) {
    let data: {[k: string]: any} = {
      id: this.newUID(),
    };

    axes.forEach((axis: Axis) => {
      if (axis.crs.toLowerCase() === 'indices') {
        // Check that all values are integers
        let check = [axis.start, axis.stop, axis.step]
          .map(x => Number.isInteger(x) && x >= 0)
          .every(x => x);

        if (!check) {
          throw `Axis "${axis.id}" CRS is set to "${axis.crs}", all values must be positive whole numbers`;
        }
      }

      data[axis.id] = {
        start: axis.start,
        end: axis.stop,
        step: axis.step,
        crs: axis.crs.toLowerCase(),
      };
    });

    let sig = Object.keys(data).sort().map((name: string) => {
      if (name === 'id') return '';

      let axis = data[name];

      return `${name}${axis.start}${axis.end}${axis.step}${axis.crs}`;
    });

    data['sig'] = sig.join('');

    return data
  }

  prepareDataInputs(defaults: any = {}) {
    let operation = {};
    let domain = {};
    let variable = {};

    let regrid = null;

    if (defaults.regrid != undefined && defaults.regrid.regridType !== 'None') {
      regrid = this.buildRegrid(defaults.regrid);
    }

    let defaultDomain = null;

    if (defaults.domain != undefined) {
      defaultDomain = this.buildDomain(defaults.domain);
    }

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
          input: [],
        };

        if (curr.regrid.regridType === 'None') {
          if (regrid != null) {
            op['gridder'] = regrid;
          }
        } else {
          op['gridder'] = this.buildRegrid(curr.regrid);
        }

        if (defaultDomain != null && curr.domainPreset === 'Global') {
          op['domain'] = defaultDomain.id;

          if (!(defaultDomain.id in domain)) {
            domain[defaultDomain.id] = defaultDomain;
          }
        } else if (curr.domainPreset === 'Custom' || curr.domainPreset === 'World') {
          let newDomain = this.buildDomain(curr.domain);

          let matched: null | any = null;

          Object.keys(domain).forEach((name: any) => {
            if (domain[name].sig === newDomain.sig) {
              matched = domain[name];
            }
          });

          if (matched == null) {
            domain[newDomain.id] = newDomain; 

            op['domain'] = newDomain.id;
          } else {
            op['domain'] = matched.id;
          }
        }

        if (defaults.parameters != undefined) {
          // Always insert the defaults
          defaults.parameters.forEach((para: Parameter) => {
            op[para.key] = para.value;
          });
        }

        // Allow process specific to overwrite default parameters
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

    Object.keys(domain).forEach((x: any) => { delete domain[x].sig; });

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

  prepareDataInputsXML(defaults: any) {
    const WPS_NS = 'http://www.opengis.net/wps/1.0.0';
    const OWS_NS = 'http://www.opengis.net/ows/1.1';
    const XLINK_NS = 'http://www.w3.org/1999/xlink';
    const XSI_NS = 'http://www.w3.org/2001/XMLSchema-instance';
    const SCHEMA_LOCATION = 'http://www.opengis.net/wps/1.0.0/wpsExecute_request.xsd';

    let dataInputs = this.prepareDataInputs(defaults);

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
    private configService: ConfigService,
  ) { 
    super(http); 
  }

  processes(): Promise<string[]> {
    return this.get(this.configService.processesPath)
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

    return this.get(this.configService.searchPath, params)
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

    return this.get(this.configService.searchVariablePath, params)
      .then(response => {
        return response.data as Axis[];
      });
  }

  execute(process: Process, defaults: any = {}): Promise<string> {
    let preparedData: string;

    try {
      //preparedData = process.prepareDataInputsString();
      preparedData = process.prepareDataInputsXML(defaults);
    } catch (e) {
      return Promise.reject(e);
    }

    let params = new URLSearchParams();

    //params.append('service', 'WPS');
    //params.append('request', 'execute');
    params.append('api_key', this.authService.user.api_key);
    //params.append(.id', process.id);
    //params.append('datainputs', preparedData);

    //return this.getUnmodified('/wps', params)
    //  .then(response => {
    //    return response.text(); 
    //  });

    return this.postCSRFUnmodified(this.configService.wpsPath, preparedData, params=params)
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

    return this.postCSRF(this.configService.generatePath, data)
      .then(response => {
        return response.data;
      });
  }
}
