import * as L from 'leaflet';

import { CornerMarker } from './corner-marker';

export class Selection extends L.Rectangle {
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

  updateBounds(newBounds: L.LatLngBoundsExpression) {
    this.setBounds(newBounds);

    this.moveMarker.setLatLng(this.getCenter());

    this.repositionResizeMarkers();
  }

  onAdd(map: L.Map): any {
    super.onAdd(map); 

    this.moveMarker.addTo(map);

    this.resizeMarkers.forEach((marker: CornerMarker) => {
      marker.addTo(map);
    });

    let bounds = this.getBounds(),
      nw = bounds.getNorthWest(),
      se = bounds.getSouthEast();

    let data = {
      latitude: [nw.lat, se.lat],
      longitude: [nw.lng, se.lng],
    };

    this.fire('updatedomain', data);

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
    let bounds = this.getBounds(),
      nw = bounds.getNorthWest(),
      se = bounds.getSouthEast();

    let data = {
      latitude: [nw.lat, se.lat],
      longitude: [nw.lng, se.lng],
    };

    this.fire('updatedomain', data);
  }

  repositionResizeMarkers() {
    let bounds = this.boundsToArray();

    this.resizeMarkers.forEach((marker: CornerMarker, i: number) => {
      marker.setLatLng(bounds[i]);
    });
  }
}
