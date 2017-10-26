import { Input, Component, OnInit, ViewChild, ViewChildren, QueryList, AfterViewInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import * as L from 'leaflet';

require('leaflet/dist/leaflet.css');

import { AuthService } from '../core/auth.service';
import { ConfigureService } from './configure.service';
import { NotificationService } from '../core/notification.service';

import { Selection } from './selection';
import { Axis, AxisComponent } from './axis.component';

interface Dataset {
  axes: Axis[];
  files: string[];
}

interface SearchResult {
  [index: string]: Dataset;
}

class RegridOptions {
  lats: number;
  lons: number;
}

class Configuration {
  process: string;
  axes: Axis[];
  files: string[];
  variable: string;
  regrid: string;
  regridOptions: RegridOptions;

  constructor() {
    this.regridOptions = new RegridOptions();
  }
}

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

  .map-container {
    min-height: calc(100vh - 100px);
  }
  `],
  providers: [ConfigureService]
})
export class ConfigureComponent implements OnInit, AfterViewInit { 
  @ViewChild('mapContainer') mapContainer: any;
  @ViewChildren(AxisComponent) axes: QueryList<AxisComponent>;

  lngNames = ['x', 'lon', 'longitude'];
  latNames = ['y', 'lat', 'latitude'];

  domains = [
    new Domain('World'),
    new Domain('Custom')
  ];

  map: L.Map;
  domainModel = {value: 'World'};
  config: Configuration;
  result: SearchResult;
  selection: Selection;
  variables: string[];

  processes: Promise<any>;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private authService: AuthService,
    private configService: ConfigureService,
    private notificationService: NotificationService
  ) { 
    this.config = new Configuration();

    this.config.regrid = 'None';
  }

  ngOnInit() {
    this.route.queryParams.subscribe(params => {
      this.configService.searchESGF(params)
        .then(response => {
          if (response.status === 'success') {
            this.result = response.data as SearchResult;

            this.variables = Object.keys(this.result);

            this.config.variable = this.variables[0];

            this.config.axes = this.result[this.config.variable].axes.map((axis: Axis) => {
              return {step: 1, ...axis};
            });
          } else {
            this.notificationService.error(response.error);
          }
        });
    });

    this.processes = this.configService.processes()
      .then(response => {
        if (response.status === 'success') {
          let p = response.data.sort((a: string, b: string) => {
            if (a > b) {
              return 1;
            } else if (a < b) {
              return -1;
            } else {
              return 0;
            }
          });

          this.config.process = p[0];

          return p;
        } else {
          this.notificationService.error(response.error);

          return [];
        }
      });

    this.map = L.map(this.mapContainer.nativeElement).setView(L.latLng(0.0, 0.0), 1);

    L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(this.map);

    this.selection = new Selection(this.map, [[0, 0], [20, 20]], {color: '#4db8ff'});

    this.selection.on('updatedomain', (data: any) => {
      let nw = data.getNorthWest(),
        se = data.getSouthEast();

      this.axes.forEach((axisComponent: AxisComponent) => {
        let axis = axisComponent.axis;

        if (this.lngNames.some((x: string) => x === axis.id)) {
          axis.start = nw.lng;

          axis.stop = se.lng;
        } else if (this.latNames.some((x: string) => x === axis.id)) {
          axis.start = nw.lat;

          axis.stop = se.lat;
        }
      });
    });
  }

  ngAfterViewInit() {
    this.map.invalidateSize();
  }

  domainChange() {
    switch (this.domainModel.value) {
      case 'World':
        if (this.map.hasLayer(this.selection)) {
          this.selection.removeFrom(this.map);
        }

        const timeAxis = ['time', 't'];

        this.axes.forEach((axisComponent: AxisComponent) => {
          if (timeAxis.indexOf(axisComponent.axis.id) < 0) {
            let filtered = this.result[this.config.variable].axes.filter((axis: Axis) => {
              return axis.id === axisComponent.axis.id;
            });

            if (filtered.length > 0) {
              axisComponent.axis.start = filtered[0].start;

              axisComponent.axis.stop = filtered[0].stop;
            }
          }
        });

        break;
      case 'Custom':
        if (!this.map.hasLayer(this.selection)) {
          this.selection.addTo(this.map);
        }

        break;
    }
  }

  prepareData(): string {
    let data = '';
    let numberPattern = /\d+\.?\d+/;

    data += `process=${this.config.process}&`;

    data += `variable=${this.config.variable}&`;

    data += `regrid=${this.config.regrid}&`;

    if (this.config.regrid !== 'None') {
      if (this.config.regrid === 'Uniform') {
        if (this.config.regridOptions.lons === undefined) {
          this.notificationService.error('Regrid longitudes must have a value set');

          return null;
        }
      }

      if (this.config.regridOptions.lats === undefined) {
        this.notificationService.error('Regrid latitudes must have a value set');

        return null;
      }

      data += `longitudes=${this.config.regridOptions.lons}&`;

      data += `latitudes=${this.config.regridOptions.lats}&`;
    }

    let files = this.result[this.config.variable].files;

    data += `files=${files}&`;

    data += 'dimensions=' + JSON.stringify(this.axes.map((axis: any) => { return axis.axis; }));

    return data;
  }

  onDownload() {
    let data = this.prepareData();

    if (data !== null) {
      this.configService.downloadScript(data)
        .then(response => {
          if (response.status === 'success') {
            let url = URL.createObjectURL(new Blob([response.data.text]));

            let a = document.createElement('a');

            a.href = url;
            a.target = '_blank';
            a.download = response.data.filename;

            a.click();
          } else {
            this.notificationService.error(`Failed to download script: ${response.error}`);
          }
        });
    }
  }

  onExecute() {
    let data = this.prepareData();

    if (data !== null) {
      this.configService.execute(data)
        .then(response => {
          if (response.status === 'success') {

          } else {
            this.notificationService.error(`Failed to execute process: ${response.error}`);
          }
        });
    }
  }
}
