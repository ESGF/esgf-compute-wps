import { Component, Input, OnInit, ViewChild } from '@angular/core';

import { Axis } from './axis.component';
import { Selection } from './selection';
import { Domain } from './domain.component';

import * as L from 'leaflet';

require('leaflet/dist/leaflet.css');

const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

@Component({
  selector: 'domain-map',
  styles: [`
    .map-container {
      height: 85vh;
    }
  `],
  template: `
  <div #mapContainer class="map-container"></div>
  `
})
export class MapComponent implements OnInit {
  @ViewChild('mapContainer') mapContainer: any;

  domain: Domain;
  map: L.Map;
  selection: Selection;

  ngOnInit() {
    this.map = L.map(this.mapContainer.nativeElement).setView(L.latLng(0.0, 0.0), 1);

    L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
      crossOrigin: true,
      attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(this.map);

    this.selection = new Selection(this.map, [[90, -180], [-90, 180]], {color: '#4db8ff'});

    this.selection.on('updatedomain', (data: any) => this.selectionUpdated(data));

    this.selection.addTo(this.map);
  }

  selectionUpdated(data: any) {
    if (this.domain == undefined) { return; }

    let lng = this.domain.axes.find((axis: Axis) => { return LNG_NAMES.indexOf(axis.id) > -1; });
    let lat = this.domain.axes.find((axis: Axis) => { return LAT_NAMES.indexOf(axis.id) > -1; });

    lng.start = data.longitude[0];
    lng.stop = data.longitude[1];

    lat.start = data.latitude[0];
    lat.stop = data.latitude[1];
  }

  getBounds() {
    let lng = this.domain.axes.find((axis: Axis) => { return LNG_NAMES.indexOf(axis.id) > -1; });
    let lat = this.domain.axes.find((axis: Axis) => { return LAT_NAMES.indexOf(axis.id) > -1; });

    let sw = new L.LatLng(lat.stop, lng.start);
    let ne = new L.LatLng(lat.start, lng.stop);
    let bounds = new L.LatLngBounds(sw, ne);

    return bounds;
  }

  updateDomain(domain: Domain) {
    this.domain = domain;

    this.selection.updateBounds(this.getBounds());
  }
}
