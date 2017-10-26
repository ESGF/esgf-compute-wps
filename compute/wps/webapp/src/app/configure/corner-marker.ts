import * as L from 'leaflet';

export class CornerMarker extends L.Marker {
  constructor(
    public index: number,
    latlng: L.LatLngExpression,
    options?: L.MarkerOptions
  ) {
    super(latlng, options);
  }
}
