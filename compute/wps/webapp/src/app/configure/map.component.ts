import { Component, Input, OnInit, ViewChild } from '@angular/core';

import { Axis } from './axis.component';
import { Selection } from './selection';

import * as L from 'leaflet';

require('leaflet/dist/leaflet.css');

@Component({
  selector: 'domain-map',
  styles: [`
  .map-container {
    min-height: calc(100vh - 100px);
  }
  `],
  template: `
  <div #mapContainer class="map-container"></div>
  `
})
export class MapComponent implements OnInit {
  @ViewChild('mapContainer') mapContainer: any;

  @Input() axes: Axis[];

  lonNames = ['x', 'lon', 'longitude'];
  latNames = ['y', 'lat', 'latitude'];

  domain: string;
  map: L.Map;
  selection: Selection;

  ngOnInit() {
    this.map = L.map(this.mapContainer.nativeElement).setView(L.latLng(0.0, 0.0), 1);

    L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(this.map);

    this.selection = new Selection(this.map, [[0, 0], [20, 20]], {color: '#4db8ff'});

    this.selection.on('updatedomain', (data: any) => this.updateDomain(data));
  }

  updateDomain(data: any) {
    let nw = data.getNorthWest(),
      se = data.getSouthEast();

    this.axes.forEach((axis: Axis) => {
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
    
    if (this.domain !== 'Custom') {
      this.domain = 'Custom';
    }

    let lon = this.axes.find((axis: Axis) => this.lonNames.indexOf(axis.id) >= 0);
    let lat = this.axes.find((axis: Axis) => this.latNames.indexOf(axis.id) >= 0);

    this.selection.off('updatedomain');

    if (!this.map.hasLayer(this.selection)) {
      this.selection.addTo(this.map);
    }

    this.selection.updateBounds([[lat.stop, lon.start], [lat.start, lon.stop]]);

    this.selection.on('updatedomain', (data: any) => this.updateDomain(data));
  }

  domainChange() {
    switch (this.domain) {
      case 'World':
        if (this.map.hasLayer(this.selection)) {
          this.selection.removeFrom(this.map);
        }

        break;
      case 'Custom':
        if (!this.map.hasLayer(this.selection)) {
          this.selection.addTo(this.map);
        }

        break;
    }
  }
}
