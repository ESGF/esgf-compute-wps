import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import * as L from 'leaflet';

import { AuthService } from '../core/auth.service';
import { Configuration, VariableCollection, ConfigureService } from './configure.service';
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
  @ViewChild(GeneralConfigComponent) general: GeneralConfigComponent;

  domains = [
    new Domain('World'),
    new Domain('Custom')
  ];

  config: Configuration;
  datasetIDs: string[];

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
    if (this.map.domain === 'World') {
      this.general.resetDomain();
    }

    this.map.domainChange();
  }
}
