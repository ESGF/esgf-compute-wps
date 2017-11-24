import { Component, Input } from '@angular/core';

import { Configuration } from './configure.service';

@Component({
  selector: 'regrid',
  styles: [`
  .select-spacer {
    margin-bottom: 5px;
  }
  `],
  template: `
  <select [(ngModel)]="config.process.regrid" class="form-control select-spacer" id="regrid" name="regrid">
    <option>None</option>
    <option>Gaussian</option>
    <option>Uniform</option>
  </select>
  <div *ngIf="config.process.regrid !== 'None'" class="panel panel-default">
    <div class="panel-body">
      <div class="form-group">
        <label for="lats">Latitudes</label>
        <input [(ngModel)]="config.process.regridOptions.lats" type="number" class="form-control" id="lats" placeholder="Latitudes" name="lats">
      </div>
      <div *ngIf="config.process.regrid === 'Uniform'" class="form-group">
        <label for="lons">Longituds</label>
        <input [(ngModel)]="config.process.regridOptions.lons" type="number" class="form-control" id="lons" placeholder="Longitudes" name="lons">
      </div>
    </div>
  </div>
  `
})
export class RegridComponent { 
  @Input() config: Configuration;
}
