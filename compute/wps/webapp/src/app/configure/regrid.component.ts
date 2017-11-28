import { Component, Input } from '@angular/core';

export class RegridModel {
  constructor(
    public regridType: string = 'None',
    public lats: number = 0,
    public lons: number = 0,
  ) { }
}

@Component({
  selector: 'regrid',
  styles: [`
  .select-spacer {
    margin-bottom: 5px;
  }
  `],
  template: `
  <select [(ngModel)]="model.regridType" class="form-control select-spacer" id="regridType" name="regridType">
    <option>None</option>
    <option>Gaussian</option>
    <option>Uniform</option>
  </select>
  <div *ngIf="model.regridType !== 'None'" class="panel panel-default">
    <div class="panel-body">
      <div class="form-group">
        <label for="lats">Latitudes</label>
        <input [(ngModel)]="model.lats" type="number" class="form-control" id="lats" placeholder="Latitudes" name="lats">
      </div>
      <div *ngIf="model.regridType === 'Uniform'" class="form-group">
        <label for="lons">Longituds</label>
        <input [(ngModel)]="model.lons" type="number" class="form-control" id="lons" placeholder="Longitudes" name="lons">
      </div>
    </div>
  </div>
  `
})
export class RegridComponent { 
  @Input() model: RegridModel;
}
