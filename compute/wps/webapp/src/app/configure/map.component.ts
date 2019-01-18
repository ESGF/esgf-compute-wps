import { Component, Input, OnInit, ViewChild } from '@angular/core';

import { Selection } from './selection';

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
  }

  getBounds(): L.LatLngBoundsExpression {
    return null;
  }
}
