import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import * as L from 'leaflet';

import { AuthService } from '../core/auth.service';
import { Configuration, SearchResult, ConfigureService } from './configure.service';
import { NotificationService } from '../core/notification.service';

import { Selection } from './selection';
import { Axis, AxisComponent } from './axis.component';
import { Parameter } from './parameter.component';
import { MapComponent } from './map.component';

class Domain {
  constructor(
    public name: string,
    public bounds?: L.LatLngBoundsExpression
  ) { }
}

@Component({
  templateUrl: './configure.component.html',
  styles: [`
  .fill { 
    height: 100%;
  }

  .select-spacer {
    margin-bottom: 10px;
  }

  .list-item-axis {
    margin: 5px 0px 5px 0px;
  }
  `],
  providers: [ConfigureService]
})
export class ConfigureComponent implements OnInit { 
  @ViewChild(MapComponent) map: MapComponent;

  domains = [
    new Domain('World'),
    new Domain('Custom')
  ];

  config: Configuration;
  result: SearchResult;
  selection: Selection;
  variables: string[];
  datasetIDs: string[];

  processes: Promise<string[]>;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private authService: AuthService,
    private configService: ConfigureService,
    private notificationService: NotificationService
  ) { 
    this.config = new Configuration();
  }

  ngOnInit() {
    this.map.domain = 'World'
    
    this.route.queryParams.subscribe(params => {
      this.datasetIDs = (params['dataset_id'] === undefined) ? [] : params['dataset_id'].split(',');

      if (this.datasetIDs.length > 0) {
        this.config.datasetID = this.datasetIDs[0];
      }

      this.config.indexNode = params['index_node'] || '';

      this.config.query = params['query'] || '';

      this.config.shard = params['shard'] || '';

      this.loadDataset();
    });

    this.processes = this.configService.processes()
      .then(data => {
        let p = data.sort();

        this.config.process = p[0];

        return p;
      })
      .catch(error => {
        this.notificationService.error(error); 

        return [];
      });
  }

  addParameter() {
    this.config.params.push({key: '', value: ''} as Parameter);
  }

  removeParameter(param: Parameter) {
    this.config.params = this.config.params.filter((value: Parameter) => {
      return !(param.key === value.key && param.value === value.value);
    });
  }

  domainChange() {
    if (this.map.domain === 'World' && this.config.dataset.axes !== undefined) {
      this.config.dataset.axes.forEach((axis: Axis) => {
        if (this.map.latNames.indexOf(axis.id) >= 0 || this.map.lonNames.indexOf(axis.id) >= 0) {
          let filtered = this.result[this.config.variable].axes.filter((value: Axis) => {
            return axis.id === value.id;
          });

          if (filtered.length > 0) {
            axis.start = filtered[0].start;

            axis.stop = filtered[0].stop;
          }
        }
      });
    }

    this.map.domainChange();
  }

  loadDataset() {
    this.configService.searchESGF(this.config)
      .then(data => {
        this.result = data;

        this.variables = Object.keys(this.result).sort();

        this.config.variable = this.variables[0];

        this.loadVariable();
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  loadVariable() {
    if (this.result[this.config.variable].axes === undefined) {
      this.configService.searchVariable(this.config)
        .then(data => {
          this.result[this.config.variable]['axes'] = data;

          this.setDataset();
        })
        .catch(error => {
          this.notificationService.error(error);
        });
    } else {
      this.setDataset();
    }
  }

  setDataset() {
    this.config.dataset = Object.assign({}, this.result[this.config.variable]);

    this.config.dataset.axes = this.config.dataset.axes.map((axis: Axis) => {
      return {step: 1, ...axis}; 
    });
  }

  onDownload() {
    this.config.dataset = this.result[this.config.variable];

    this.configService.downloadScript(this.config)
      .then(data => {
          let url = URL.createObjectURL(new Blob([data.text]));

          let a = document.createElement('a');

          a.href = url;
          a.target = '_blank';
          a.download = data.filename;

          a.click();
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  onExecute() {
    this.config.dataset = this.result[this.config.variable];

    this.configService.execute(this.config)
      .then((data: any) => {
        let parser = new DOMParser();
        let xml = parser.parseFromString(data.report, 'text/xml');
        let el = xml.getElementsByTagName('wps:ExecuteResponse');
        let link = '';

        if (el.length > 0) {
          let statusLocation = el[0].attributes.getNamedItem('statusLocation').value;

          let jobID = statusLocation.substring(statusLocation.lastIndexOf('/')+1);

          link = `/wps/home/user/jobs`;
        }
        
        this.notificationService.message('Succesfully submitted job', link);
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }
}
