import { Component, Input, Output, EventEmitter } from '@angular/core';

export class Parameter {
  uid: string;

  constructor(
    public key: string = '',
    public value: string = ''
  ) { 
    this.uid = Math.random().toString(16).slice(2);
  }

  validate() {
    if (this.key == '') {
      throw `Parameter must have a key`;
    } else if (this.value == '') {
      throw `Parameter "${this.key}" must have a value`;
    }
  }
}

@Component({
  selector: 'parameter',
  styles: [`
  .split {
    max-width: 5.5vw;
  }
  `],
  template: `
    <div class="form-inline clearfix">
      <input [(ngModel)]="param.key" name="key" class="form-control split" type="text" placeholder="Key">
      <input [(ngModel)]="param.value" name="value" class="form-control split" type="text" placeholder="Value">
      <span>
        <span (click)="onRemove()" class="btn btn-xs btn-default">
          <span class="glyphicon glyphicon-remove"></span>
        </span>
      </span>
    </div>
  `
})
export class ParameterComponent {
  @Input() param: Parameter;

  @Output() remove: EventEmitter<Parameter> = new EventEmitter<Parameter>();

  onRemove() {
    this.remove.emit(this.param);
  }
}
