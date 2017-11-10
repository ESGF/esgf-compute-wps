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
    <div class="form-inline parameter">
      <input [(ngModel)]="param.key" name="key" class="form-control half" type="text" placeholder="Key">
      <input [(ngModel)]="param.value" name="value" class="form-control half" type="text" placeholder="Value">
      <button (click)="onRemove()" type="button" aria-labels="Close">&times;</button>
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
