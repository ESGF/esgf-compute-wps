import { Component, Input, Output, EventEmitter, OnInit } from '@angular/core';

import { Axis } from './axis';
import { CRS } from './crs.enum';

@Component({
  selector: 'axis-config',
  template: `
  <div class="row">
    <div class="col-md-12">
      <div class="row form-horizontal">
        <div class="col-md-1">
          <label for="name" class="form-control-static">Name</label>
        </div>
        <div class="col-md-3">
          <input 
            [(ngModel)]="axis.id"
            *ngIf="axis.custom; else staticID"
            class="form-control"
            id="name" 
            type="text">
          <ng-template #staticID>
            <div class="form-control-static" id="name">
              {{axis.id}}
            </div>
          </ng-template>
        </div>
        <div class="col-md-1">
          <label for="start" class="form-control-static">Start</label>
        </div>
        <div class="col-md-2">
          <input 
            [(ngModel)]="axis.start"
            class="form-control" 
            id="start" 
            type="number">
        </div>
        <div class="col-md-1">
          <label for="crs" class="form-control-static">CRS</label>
        </div>
        <div class="col-md-2">
          <select 
            [(ngModel)]="axis.crs"
            (change)="axis.reset(axis.crs)"
            class="form-control" 
            id="crs">
            <option *ngFor="let x of crs | enum" value="{{x.value}}">{{x.key}}</option>
          </select>
        </div>
        <div class="col-md-2">
          <button (click)="onRemove.emit(axis)" type="button" class="btn btn-danger pull-right">&times;</button>
        </div>
      </div>
      <br />
      <div class="row form-horizontal">
        <div class="col-md-1">
          <label *ngIf="!axis.custom" for="units" class="form-control-static">Units</label>
        </div>
        <div class="col-md-3">
          <div class="form-control-static" id="units">
            {{axis.units}}
          </div>
        </div>
        <div class="col-md-1">
          <label for="stop" class="form-control-static">Stop</label>
        </div>
        <div class="col-md-2">
          <input
            [(ngModel)]="axis.stop"
            class="form-control" 
            id="stop" 
            type="number">
        </div>
        <div class="col-md-1">
          <label for="step" class="form-control-static">Step</label>
        </div>
        <div class="col-md-2">
          <input 
            [(ngModel)]="axis.step"
            class="form-control" 
            id="step" 
            type="number">
        </div>
      </div>
    </div>
  </div>
  `
})
export class AxisComponent {
  @Input() axis: Axis;

  @Output() onRemove = new EventEmitter<Axis>();

  crs = CRS;

  constructor() {

  }

  changeCRS(crs: CRS) {
    this.axis.reset(crs);
  }
}
