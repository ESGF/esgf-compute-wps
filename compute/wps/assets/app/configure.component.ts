import { Input, Component, OnInit, ViewChild, ViewChildren, QueryList, AfterViewInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import * as L from 'leaflet';

require('leaflet/dist/leaflet.css');

import { AuthService } from './auth.service';
import { ConfigureService } from './configure.service';
import { NotificationService } from './notification.service';

interface Axis {
  id: string;
  id_alt: string;
  start: number;
  stop: number;
  step: number;
}

interface SearchResult {
  axes: Axis[];
  files: string[];
  variables: string[];
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

@Component({
  selector: 'axis',
  template: `
  <br>
  <div class="panel panel-default">
    <div class="panel-heading">
      <div class="panel-title">
        <a role="button" data-toggle="collapse" data-parent="#accordionAxis" href="#collapse{{axisIndex}}">
          <span>{{axis.id}} ({{axis.units}})</span>
        </a>
      </div>
    </div>
    <div id="collapse{{axisIndex}}" class="panel-collapse collapse">
      <div class="panel-body">
        <form #dimForm{{axisIndex}}="ngForm">
          <label for="start{{axisIndex}}">Start</label>     
          <input [(ngModel)]="axis.start" name="start" class="form-control" type="string" id="start{{axisIndex}}" value="{{axis.start}}">
          <label for="stop{{axisIndex}}">Stop</label> 
          <input [(ngModel)]="axis.stop" name="stop" class="form-control" type="string" id="stop{{axisIndex}}" value="{{axis.stop}}">
          <label for="step{{axisIndex}}">Step</label> 
          <input [(ngModel)]="axis.step" name="step" class="form-control" type="string" id="step{{axisIndex}}" value="{{axis.step}}">
        </form>
      </div>
    </div>
  </div>
  `
})
export class AxisComponent {
  @Input() axis: Axis;
  @Input() axisIndex: number;
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

  map: L.Map;
  config: Configuration;
  result: SearchResult;

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

            // Set default step value
            this.result.axes.map((axis: Axis) => {
              axis.step = 1;
            });

            this.config.variable = this.result.variables[0];
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
  }

  ngAfterViewInit() {
    this.map.invalidateSize();
  }

  prepareData(): string {
    let data = '';

    data += `process=${this.config.process}&`;

    data += `variable=${this.config.variable}&`;

    data += `regrid=${this.config.regrid}&`;

    if (this.config.regrid !== 'None') {
      data += `longitudes=${this.config.regridOptions.lons}&`;

      data += `latitudes=${this.config.regridOptions.lats}&`;
    }

    let files = this.result.files.filter((value: string) => {
      return value.indexOf(`/${this.config.variable}/`) >= 0;
    });

    data += `files=${files}&`;

    data += 'dimensions=' + JSON.stringify(this.axes.map((axis: any) => { return axis.axis; }));

    return data;
  }

  onDownload() {
    this.configService.downloadScript(this.prepareData())
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

  onExecute() {
    this.configService.execute(this.prepareData())
      .then(response => {
        if (response.status === 'success') {

        } else {
          this.notificationService.error(`Failed to execute process: ${response.error}`);
        }
      });
  }
}
