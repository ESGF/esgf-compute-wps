import { 
  Component,
  Input,
  Output,
  EventEmitter,
  OnChanges,
  SimpleChanges 
} from '@angular/core';

import { Subject } from 'rxjs/Subject';

export interface Axis {
  id: string;
  id_alt: string;
  units: string;
  start: number;
  stop: number;
  step: number;
  crs: string;
}

@Component({
  selector: 'axis',
  template: `
  <div class="panel panel-default">
    <div class="panel-heading">
      <div class="panel-title">
        <a role="button" data-toggle="collapse" data-parent="#accordionAxis" href="#collapse{{id}}{{axisIndex}}">
          <div class="container-fluid">
            <div class="row">
              <div class="col-md-10">
                <span id="title">{{axis.id}} ({{axis.units}})</span>
              </div>
              <div *ngIf="canRemove" class="col-md-2">
                <button (click)="axisRemove.emit(axis.id)" class="close">&times;</button>
              </div>
            </div>
          </div>
        </a>
      </div>
    </div>
    <div id="collapse{{id}}{{axisIndex}}" class="panel-collapse collapse">
      <div class="panel-body">
        <form #dimForm{{axisIndex}}="ngForm">
          <div>
            <label for="crs{{axisIndex}}">CRS</label>
            <br />
            <select [(ngModel)]="axis.crs" name="crs" id="crs{{axisIndex}}" class="form-control">
              <option>Values</option>
              <option>Indices</option>
            </select>
          </div>
          <div>
            <label for="start{{axisIndex}}">Start</label>     
            <input [ngModel]="axis.start" (ngModelChange)="start.next($event)" name="start" class="form-control" type="string" id="start{{axisIndex}}">
          </div>
          <div>
            <label for="stop{{axisIndex}}">Stop</label> 
            <input [ngModel]="axis.stop" (ngModelChange)="stop.next($event)" name="stop" class="form-control" type="string" id="stop{{axisIndex}}">
          </div>
          <div>
            <label for="step{{axisIndex}}">Step</label> 
            <input [ngModel]="axis.step" (ngModelChange)="step.next($event)" name="step" class="form-control" type="string" id="step{{axisIndex}}">
          </div>
        </form>
      </div>
    </div>
  </div>
  `
})
export class AxisComponent {
  @Input() axis: Axis;
  @Input() axisIndex: number;
  @Input() canRemove: boolean = false;
  @Output() axisChange: EventEmitter<string> = new EventEmitter<string>();
  @Output() axisRemove: EventEmitter<string> = new EventEmitter<string>();

  id: string = Math.random().toString(16).slice(2);
  start: Subject<number> = new Subject<number>();
  stop: Subject<number> = new Subject<number>();
  step: Subject<number> = new Subject<number>();

  constructor() {
    this.start
      .debounceTime(1000)
      .distinctUntilChanged()
      .subscribe(value => {
        this.axis.start = +value;

        this.axisChange.emit(this.axis.id);
      });

    this.stop
      .debounceTime(1000)
      .distinctUntilChanged()
      .subscribe(value => {
        this.axis.stop = +value;

        this.axisChange.emit(this.axis.id);
      });

    this.step
      .debounceTime(1000)
      .distinctUntilChanged()
      .subscribe(value => {
        this.axis.step = +value;

        this.axisChange.emit(this.axis.id);
      });
  }
}
