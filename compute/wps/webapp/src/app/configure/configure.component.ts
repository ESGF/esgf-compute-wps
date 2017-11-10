import { Input, Component, OnInit, ViewChild, ViewChildren, QueryList, AfterViewInit } from '@angular/core';
import { Router, ActivatedRoute, Params } from '@angular/router';

import * as L from 'leaflet';

require('leaflet/dist/leaflet.css');

import { AuthService } from '../core/auth.service';
import { Configuration, SearchResult, ConfigureService } from './configure.service';
import { NotificationService } from '../core/notification.service';

import { Selection } from './selection';
import { Axis, AxisComponent } from './axis.component';
import { Parameter } from './parameter.component';

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

  lonNames = ['x', 'lon', 'longitude'];
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

    if (this.map === undefined) {
      this.map = L.map(this.mapContainer.nativeElement).setView(L.latLng(0.0, 0.0), 1);

      L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
        attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
      }).addTo(this.map);
    }

    this.selection = new Selection(this.map, [[0, 0], [20, 20]], {color: '#4db8ff'});

    this.selection.on('updatedomain', (data: any) => this.updateDomain(data));
  }

  ngAfterViewInit() {
    this.map.invalidateSize();
  }

  addParameter() {
    this.config.params.push({key: '', value: ''} as Parameter);
  }

  removeParameter(param: Parameter) {
    this.config.params = this.config.params.filter((value: Parameter) => {
      return !(param.key === value.key && param.value === value.value);
    });
  }

  updateDomain(data: any) {
    let nw = data.getNorthWest(),
      se = data.getSouthEast();

    this.axes.forEach((axisComponent: AxisComponent) => {
      let axis = axisComponent.axis;

      if (this.lonNames.some((x: string) => x === axis.id)) {
        axis.start = nw.lng;

        axis.stop = se.lng;
      } else if (this.latNames.some((x: string) => x === axis.id)) {
        axis.start = se.lat;

        axis.stop = nw.lat;
      }
    });
  }

  onAxisChange(id: string) {
    if (this.lonNames.indexOf(id) === -1 && this.latNames.indexOf(id) === -1) {
      return;
    }
    
    if (this.domainModel.value !== 'Custom') {
      this.domainModel.value = 'Custom';
    }

    let lon = this.axes.find((axis: AxisComponent) => this.lonNames.indexOf(axis.axis.id) >= 0);
    let lat = this.axes.find((axis: AxisComponent) => this.latNames.indexOf(axis.axis.id) >= 0);

    this.selection.off('updatedomain');

    if (!this.map.hasLayer(this.selection)) {
      this.selection.addTo(this.map);
    }

    this.selection.updateBounds([[lat.axis.stop, lon.axis.start], [lat.axis.start, lon.axis.stop]]);

    this.selection.on('updatedomain', (data: any) => this.updateDomain(data));
  }

  loadDataset() {
    this.configService.searchESGF(this.config)
      .then(data => {
        this.result = data;

        this.variables = Object.keys(this.result).sort();

        this.config.variable = this.variables[0];

        this.loadVariable();
        //this.configService.searchVariable(this.config)
        //  .then(data => {
        //    this.result[this.config.variable]['axes'] = data;

        //    this.setDataset();
        //  })
        //  .catch(error => {
        //    this.notificationService.error(error);
        //  });
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

          link = `/wps/home/user/jobs`;
        }
        
        this.notificationService.message('Succesfully submitted job', link);
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }
}
