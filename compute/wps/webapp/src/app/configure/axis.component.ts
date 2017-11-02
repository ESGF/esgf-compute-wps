import { Component, Input } from '@angular/core';

export interface Axis {
  id: string;
  id_alt: string;
  units: string;
  start: number;
  stop: number;
  step: number;
}

@Component({
  selector: 'axis',
  template: `
  <br>
  <div class="panel panel-default">
    <div class="panel-heading">
      <div class="panel-title">
        <a role="button" data-toggle="collapse" data-parent="#accordionAxis" href="#collapse{{axisIndex}}">
          <span id="title">{{axis.id}} ({{axis.units}})</span>
        </a>
      </div>
    </div>
    <div id="collapse{{axisIndex}}" class="panel-collapse collapse">
      <div class="panel-body">
        <form #dimForm{{axisIndex}}="ngForm">
          <label for="start{{axisIndex}}">Start</label>     
          <input [(ngModel)]="axis.start" name="start" class="form-control" type="string" id="start{{axisIndex}}">
          <label for="stop{{axisIndex}}">Stop</label> 
          <input [(ngModel)]="axis.stop" name="stop" class="form-control" type="string" id="stop{{axisIndex}}">
          <label for="step{{axisIndex}}">Step</label> 
          <input [(ngModel)]="axis.step" name="step" class="form-control" type="string" id="step{{axisIndex}}">
        </form>
      </div>
    </div>
  </div>
  `
})
export class AxisComponent {
  @Input() axis: Axis;
  @Input() axisIndex: number;
}
