import { Component, Input } from '@angular/core';

export class RegridModel {
  constructor(
    public regridType: string = 'None',
    public startLats: number,
    public nLats: number,
    public deltaLats: number,
    public startLons: number,
    public nLons: number,
    public deltaLons: number
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
  <select [(ngModel)]="model.regridType" (ngModelChange)="modelChange($event)" class="form-control select-spacer" id="regridType" name="regridType">
    <option>None</option>
    <option>Gaussian</option>
    <option>Uniform</option>
  </select>
  <div [ngSwitch]="model.regridType">
    <div *ngSwitchCase="'Gaussian'" class="panel panel-default">
      <div class="panel-body">
        <div class="form-group">
          <label for="nlats">Latitude</label>
          <input [(ngModel)]="model.nLats" type="number" class="form-control" id="lats" placeholder="# Latitudes" name="nlats">
        </div>
      </div>
    </div>
    <div *ngSwitchCase="'Uniform'" class="panel panel-default">
      <div class="panel-body">
        <div class="form-group">
          <label for="startlats">Start Latitudes</label>
          <input [(ngModel)]="model.startLats" type="number" class="form-control" id="lats" placeholder="Start Latitudes" name="startlats">
          <label for="nlats"># Latitudes</label>
          <input [(ngModel)]="model.nLats" type="number" class="form-control" id="lats" placeholder="# Latitudes" name="nlats">
          <label for="deltalats">Delta Latitude</label>
          <input [(ngModel)]="model.deltaLats" type="number" class="form-control" id="lats" placeholder="Delta Latitudes" name="deltalats">
          <label for="startlons">Start Longitude</label>
          <input [(ngModel)]="model.startLons" type="number" class="form-control" id="lats" placeholder="Start Longitudes" name="startlons">
          <label for="nlons"># Longitudes</label>
          <input [(ngModel)]="model.nLons" type="number" class="form-control" id="lats" placeholder="# Longitudes" name="nlons">
          <label for="deltalons">Delta Longitude</label>
          <input [(ngModel)]="model.deltaLons" type="number" class="form-control" id="lats" placeholder="Delta Longitudes" name="deltalons">
        </div>
      </div>
    </div>
    <div *ngSwitchDefault>
    </div>
  </div>
  `
})
export class RegridComponent { 
  @Input() model: RegridModel;

  modelChange(data: string) {
    if (data == 'Gaussian') {
      this.model.nLats = 32;
    } else if (data == 'Uniform' && this.model.nLats != null) {
      this.model.nLats = null;
    }
  }
}
