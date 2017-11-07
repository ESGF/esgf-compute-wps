import { Input, Component, OnInit, ViewChild, ViewChildren, QueryList, AfterViewInit } from '@angular/core';
import { Router, ActivatedRoute, Params } from '@angular/router';

import * as L from 'leaflet';

require('leaflet/dist/leaflet.css');

import { AuthService } from '../core/auth.service';
import { Configuration, SearchResult, ConfigureService } from './configure.service';
import { NotificationService } from '../core/notification.service';

import { Selection } from './selection';
import { Axis, AxisComponent } from './axis.component';

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
  domainModel = { value: this.domains[0].name };

  map: L.Map;
  config: Configuration;
  result: SearchResult;
  selection: Selection;
  variables: string[];

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
    this.route.queryParams.subscribe(params => {
      this.config.params = params;

      this.configService.searchESGF(params)
        .then(data => {
          this.result = data;

          this.variables = Object.keys(this.result);

          this.config.variable = this.variables[0];

          this.config.dataset = this.result[this.config.variable];
          
          this.config.dataset.axes = this.config.dataset.axes.map((axis: Axis) => {
            return {step: 1, ...axis};
          });
        })
        .catch(error => {
          this.notificationService.error(error); 
        });
    });

    this.processes = this.configService.processes()
      .then(data => {
        let p = data.sort((a: string, b: string) => {
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
      })
      .catch(error => {
        this.notificationService.error(error); 

        return [];
      });

    if (this.map === undefined) {
      this.map = L.map(this.mapContainer.nativeElement).setView(L.latLng(0.0, 0.0), 1);

      L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
        attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
      }).addTo(this.map);
    }

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

  variableChange() {
    this.config.dataset = this.result[this.config.variable];

    if (this.config.dataset.axes === undefined) {
      this.configService.searchVariable(this.config)
        .then(data => {
          this.config.dataset.axes = data;

          this.config.dataset.axes = this.config.dataset.axes.map((axis: Axis) => {
            return {step: 1, ...axis}; 
          });
        })
        .catch(error => {
          this.notificationService.error(error);
        });
    }
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

          link = `/wps/home/user/jobs?selected=${jobID}`;
        }
        
        this.notificationService.message('Succesfully submitted job', link);
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }
}
