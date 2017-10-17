import { Input, Component, OnInit, ViewChild, ViewChildren, QueryList, AfterViewInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import * as L from 'leaflet';

require('leaflet/dist/leaflet.css');

import { AuthService } from './auth.service';
import { ConfigureService } from './configure.service';
import { NotificationService } from './notification.service';

class CornerMarker extends L.Marker {
  constructor(
    public index: number,
    latlng: L.LatLngExpression,
    options?: L.MarkerOptions
  ) {
    super(latlng, options);
  }
}

class Selection extends L.Rectangle {
  icon: L.DivIcon;

  originPoint: L.Point;
  boundsOrigin: L.Point[];

  moveMarker: L.Marker;
  resizeMarkers: CornerMarker[];

  constructor(private map: L.Map, latLngBounds: L.LatLngBoundsExpression, options?: L.PolylineOptions) { 
    super(latLngBounds, options);

    this.icon = L.divIcon({
      iconSize: [10, 10]
    });

    this.initMoveMarker();

    this.initResizeMarkers();
  }

  initMoveMarker() {
    let bounds = this.getBounds(),
      center = bounds.getCenter();

    this.moveMarker = L.marker(center, {
      icon: this.icon,
      draggable: true
    })
      .on('dragstart', (e: any) => this.onMarkerDragStart(e))
      .on('drag', (e: any) => this.onMarkerDrag(e))
      .on('dragend', (e: any) => this.onMarkerDragEnd(e))
  }

  initResizeMarkers() {
    this.resizeMarkers = this.boundsToArray().map((latlng: L.LatLng, i: number) => {
      return new CornerMarker(i, latlng, {
        icon: this.icon,
        draggable: true
      })
        .on('drag', (e: any) => this.onResizeDrag(e))
        .on('dragend', (e: any) => this.onMarkerDragEnd(e));
    });
  }

  onAdd(map: L.Map): any {
    super.onAdd(map); 

    this.moveMarker.addTo(map);

    this.resizeMarkers.forEach((marker: CornerMarker) => {
      marker.addTo(map);
    });

    this.fire('updatedomain', this.getBounds());

    return this;
  }

  onRemove(map: L.Map): any {
    super.onRemove(map);

    this.moveMarker.removeFrom(map);

    this.resizeMarkers.forEach((marker: CornerMarker) => {
      marker.removeFrom(map);
    });

    return this;
  }

  boundsToArray() {
    let bounds = this.getBounds(),
      sw = bounds.getSouthWest(),
      nw = bounds.getNorthWest(),
      ne = bounds.getNorthEast(),
      se = bounds.getSouthEast();

    return [sw, nw, ne, se];
  }

  onResizeDrag(e: any) {
    let marker = e.target,
      index = marker.index,
      bounds = this.boundsToArray(),
      opposite = bounds[(index+2)%4];

    this.setBounds(L.latLngBounds(marker.getLatLng(), opposite));

    this.repositionResizeMarkers();

    bounds = this.boundsToArray();

    this.moveMarker.setLatLng(this.getCenter());
  }

  onMarkerDragStart(e: any) {
    this.originPoint = this.map.latLngToLayerPoint(e.target.getLatLng());

    this.boundsOrigin = this.boundsToArray().map((latlng: L.LatLng) => {
      return this.map.latLngToLayerPoint(latlng);
    });
  }

  onMarkerDrag(e: any) {
    let marker = e.target,
      position = this.map.latLngToLayerPoint(marker.getLatLng()),
      offset = position.subtract(this.originPoint);

    let newBounds = this.boundsOrigin.map((point: L.Point) => {
      return this.map.layerPointToLatLng(point.add(offset));
    });

    this.setLatLngs(newBounds);

    this.repositionResizeMarkers();
  }

  onMarkerDragEnd(e: any) {
    this.fire('updatedomain', this.getBounds());
  }

  repositionResizeMarkers() {
    let bounds = this.boundsToArray();

    this.resizeMarkers.forEach((marker: CornerMarker, i: number) => {
      marker.setLatLng(bounds[i]);
    });
  }
}

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

class Domain {
  constructor(
    public name: string,
    public bounds?: L.LatLngBoundsExpression
  ) { }
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
          <input [(ngModel)]="axis.start" name="start" class="form-control" type="string" id="start{{axisIndex}}">
          <label for="stop{{axisIndex}}">Stop</label> 
          <input [(ngModel)]="axis.stop" name="stop" class="form-control" type="string" id="stop{{axisIndex}}">
          <label for="step{{axisIndex}}">Step</label> 
          <input [(ngModel)]="axis.step" name="step" class="form-control" type="string" id="step{{axisIndex}}">
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

            // clone each axis so search result holds original values
            this.config.axes = this.result.axes.map((axis: Axis) => {
              return {...axis}; 
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

        this.axes.forEach((axisComponent: AxisComponent) => {
          let filtered = this.result.axes.filter((axis: Axis) => {
            return axis.id == axisComponent.axis.id;
          });

          if (filtered.length > 0) {
            axisComponent.axis.start = filtered[0].start;

            axisComponent.axis.stop = filtered[0].stop;
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

    let files = this.result.files.filter((value: string) => {
      return value.indexOf(`/${this.config.variable}/`) >= 0;
    });

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
