import { 
  Component,
  Input,
  Output,
  EventEmitter,
  OnChanges,
  SimpleChanges,
  OnInit,
  Pipe,
  PipeTransform,
} from '@angular/core';

import { Subject } from 'rxjs/Subject';

import { File } from './file';

enum CRS {
  Values = 'Values',
  Indices = 'Indices',
}

@Pipe({name: 'enumToArray'})
export class EnumToArrayPipe implements PipeTransform {
  transform(data: Object) {
    return Object.keys(data);
  }
}

export class Axis {
  constructor(
    public id: string,
    public units: string,
    public _start: number,
    public _stop: number,
    public length: number,
    public type: string,
    public start = 0,
    public stop = 0,
    public step = 1,
    public crs = 'Values',
    public data: any = null,
    public custom = false,
  ) { 
    this.start = this._start;

    this.stop = this._stop;
  }
}

export class Domain {
  constructor(
    public id: string,
    public axes: Axis[],
  ) { }
}

@Component({
  selector: 'domain',
  styles: [`
  .panel-group {
    margin-bottom: 3px;   
  }
  `],
  template: `
  <div [class.input-group]="selectedDomain?.id == 'Custom'" style="padding-bottom: 5px;">
    <select [(ngModel)]="selectedDomain" (ngModelChange)="domainChanged($event)" class="form-control">
      <option *ngFor="let domain of domains" [ngValue]="domain">{{domain.id}}</option>
    </select>
    <span *ngIf="selectedDomain?.id == 'Custom'" class="input-group-btn">
      <button (click)="addAxis()" class="btn btn-default" type="button">Add</button>
    </span>
  </div>
  <div class="panel-group" id="domainAccordion" role="tablist">
    <div *ngFor="let axis of selectedDomain?.axes" class="panel panel-default">
      <div class="panel-heading">
        <div class="panel-title">
          <a role="button" data-toggle="collapse" data-parent="#domainAccordion" href="#collapse{{axis.id}}">
            <div class="container-fluid">
              <div class="row">
                <div class="col-md-10">
                  <span *ngIf="!axis.custom" id="title">{{axis.id}}</span>
                  <input *ngIf="axis.custom" [(ngModel)]="axis.id" type="string" class="form-control"/>
                  <span id="title">({{axis.units}})</span>
                </div>
                <div *ngIf="axis.custom" class="col-md-2">
                  <button (click)="removeAxis(axis)" class="close">&times;</button>
                </div>
              </div>
            </div>
          </a>
        </div>
      </div>
      <div id="collapse{{axis.id}}" class="panel-collapse collapse">
        <div class="panel-body">
            <div>
            <label for="crs{{axisIndex}}">CRS</label>
            <br />
            <select [(ngModel)]="axis.crs" (ngModelChange)="crsChanged(axis, $event)" name="crs" id="crs{{axis.id}}" class="form-control">
              <option *ngFor="let x of crsEnum | enumToArray" [ngValue]="x">{{x}}</option>
            </select>
          </div>
          <div>
            <label for="start{{axis.id}}">Start</label>     
            <input [ngModel]="axis.start" (ngModelChange)="startChanged(axis, $event)" name="start" class="form-control" type="string" id="start{{axis.id}}">
          </div>
          <div>
            <label for="stop{{axis.id}}">Stop</label> 
            <input [ngModel]="axis.stop" (ngModelChange)="stopChanged(axis, $event)" name="stop" class="form-control" type="string" id="stop{{axis.id}}">
          </div>
          <div>
            <label for="step{{axis.id}}">Step</label> 
            <input [ngModel]="axis.step" (ngModelChange)="stepChanged(axis, $event)" name="step" class="form-control" type="string" id="step{{axis.id}}">
          </div>
        </div>
      </div>
    </div>
  </div>
  `
})
export class DomainComponent implements OnInit {
  @Output() modified = new EventEmitter<Domain>();

  crsEnum = CRS;

  id: string = Math.random().toString(16).slice(2);

  start = new Subject<any>();
  stop = new Subject<any>();
  step = new Subject<any>();

  domains = [
    new Domain('World', []),
    new Domain('Custom', []),
  ];

  selectedDomain: Domain;

  constructor() {
    this.start
      .debounceTime(1000)
      .distinctUntilChanged()
      .subscribe(data => {
        data.axis.start = data.value;

        this.modified.emit(this.selectedDomain);
      });

    this.stop
      .debounceTime(1000)
      .distinctUntilChanged()
      .subscribe(data => {
        data.axis.stop = data.value;

        this.modified.emit(this.selectedDomain);
      });

    this.step
      .debounceTime(1000)
      .distinctUntilChanged()
      .subscribe(data => {
        data.axis.step = data.value;

        this.modified.emit(this.selectedDomain);
      });
  }

  ngOnInit() {
    this.selectedDomain = this.domains[0];
  }

  @Input()
  set axes(axes: Axis[]) {
    let world = this.domains.find((domain: Domain) => {
      return domain.id == 'World';
    });

    world.axes = axes;

    this.modified.emit(world);
  }

  get axes() {
    return this.selectedDomain.axes;
  }

  crsChanged(axis: Axis, value: CRS) {
    if (axis.crs == CRS.Values) {
      if (axis.type == 'temporal') {
        axis.start = Infinity;

        axis.data.forEach((file: File) => {
          if (file.temporal.start < axis.start) {
            axis.start = file.temporal.start;
          }

          if (file.temporal.stop > axis.stop) {
            axis.stop = file.temporal.stop;
          }
        });
      } else {
        axis.start = axis.data.start;

        axis.stop = axis.data.stop;
      }
    } else if (axis.crs == CRS.Indices) {
      axis.start = 0;

      if (axis.type == 'temporal') {
        let axisLengths = axis.data.map((file: File) => { return file.temporal.length; });

        axis.stop = axisLengths.reduce((a: number, b: number) => { return a + b; });
      } else {
        axis.stop = axis.data.length;
      }
    }

    this.modified.emit(this.selectedDomain);
  }

  domainChanged(value: Domain) {
    if (value.id == 'Custom' && value.axes.length == 0) {
      let world = this.domains.find((item: Domain) => { return item.id == 'World' });

      value.axes = world.axes.map((item: Axis) => { return {...item}; });
    }

    this.modified.emit(this.selectedDomain);
  }

  addAxis() {
    let id = Math.random().toString(16).slice(2);
    let axisId = `axis${id}`;
    let axis = new Axis(axisId, 'custom', 0, 0, 0, 'unknown');

    axis.custom = true;

    this.selectedDomain.axes.push(axis);

    this.modified.emit(this.selectedDomain);
  }

  removeAxis(axis: Axis) {
    this.selectedDomain.axes = this.selectedDomain.axes.filter((item: Axis) => {
      return axis.id != item.id;
    });

    this.modified.emit(this.selectedDomain);
  }

  startChanged(axis: Axis, value: number) {
    this.start.next({ axis: axis, value: value });
  }

  stopChanged(axis: Axis, value: number) {
    this.stop.next({ axis: axis, value: value });
  }

  stepChanged(axis: Axis, value: number) {
    this.step.next({ axis: axis, value: value });
  }
}
