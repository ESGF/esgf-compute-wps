import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import * as L from 'leaflet';

import { AuthService } from '../core/auth.service';
import {
  Configuration,
  DatasetCollection, Dataset,
  VariableCollection,
  ConfigureService
} from './configure.service';
import { NotificationService } from '../core/notification.service';

import { Selection } from './selection';
import { Axis, AxisComponent } from './axis.component';
import { Parameter } from './parameter.component';
import { MapComponent } from './map.component';
import { GeneralConfigComponent } from './general-config.component';

class Domain {
  constructor(
    public name: string,
    public bounds?: L.LatLngBoundsExpression
  ) { }
}

@Component({
  templateUrl: './configure.component.html',
  styles: [`
  .pane {
    padding: 1em;
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
  @ViewChild(GeneralConfigComponent) general: GeneralConfigComponent;

  domains = [
    new Domain('World'),
    new Domain('Custom')
  ];

  config: Configuration;
  datasetIDs: string[];
  processes: any[];

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private authService: AuthService,
    private configService: ConfigureService,
    private notificationService: NotificationService
  ) { 
    this.config = new Configuration();

    this.datasetIDs = [];
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
    });

    this.configService.processes()
      .then(data => {
        this.processes = data.sort((a: any, b: any) => {
          if (a.identifier < b.identifier) { return -1; }
          if (a.identifier > b.identifier) { return 1; }

          return 0; 
        });

        this.config.process.identifier = this.processes[0].identifier;
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  onFilesModified() {
    this.config.process.domain.forEach((item: any) => {
      this.updateAxis(item);
    });
  }

  onCRSChange(name: any) {
    let axis = this.config.process.domain.find((item: any) => {
      return item.id == name;
    });

    this.updateAxis(axis);
  }

  updateAxis(axis: Axis) {
    if (axis.type == 'temporal') {
      let data = axis.data;

      let start = Infinity;
      let stop = 0;

      for (let file of this.config.variable.files) {
        if (file.included) {
          let fileData = data[file.url];

          if (axis.crs == 'Indices') {
            start = 0;

            stop += fileData.length;
          } else if (axis.crs == 'Values') {
            if (fileData.start < start) {
              start = fileData.start;
            }

            if (fileData.stop > stop) {
              stop = fileData.stop;
            }
          }
        }
      }

      axis.start = start;

      axis.stop = stop;
    } else {
      if (axis.crs == 'Indices') {
        axis.start = 0;

        axis.stop = axis.length;
      } else if (axis.crs == 'Values') {
        axis.start = axis._start;

        axis.stop = axis._stop;
      }
    }
  }

  addParameter() {
    this.config.process.parameters.push(new Parameter());
  }

  removeParameter(param: Parameter) {
    this.config.process.parameters = this.config.process.parameters.filter((value: Parameter) => {
      return param.uid !== value.uid;
    });
  }

  domainChange() {
    if (this.map.domain === 'World') {
      this.general.resetDomain();
    }

    this.map.domainChange();
  }
}
