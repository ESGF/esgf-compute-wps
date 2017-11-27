import { Component, Input, Output, EventEmitter } from '@angular/core';

export interface Parameter {
  key: string;
  value: string;
}

@Component({
  selector: 'parameter',
  styles: [`
  .half {
    width: 42%;
  }
  .parameter {
    padding: 2px;
  }
  `],
  template: `
    <div class="form-inline list-group-item clearfix">
      <input [(ngModel)]="param.key" name="key" class="form-control" type="text" placeholder="Key">
      <input [(ngModel)]="param.value" name="value" class="form-control" type="text" placeholder="Value">
      <span>
        <span (click)="onRemove()" class="btn btn-xs btn-default">
          <span class="glyphicon glyphicon-remove"></span>
        </span>
      </span>
    </div>
  `
})
export class ParameterComponent {
  @Input() param: Parameter = {key: '', value: ''} as Parameter;

  @Output() remove: EventEmitter<Parameter> = new EventEmitter<Parameter>();

  onRemove() {
    this.remove.emit(this.param);
  }
}
