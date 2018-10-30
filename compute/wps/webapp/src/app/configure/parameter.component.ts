import { Component, Input, Output, EventEmitter } from '@angular/core';

import { Parameter } from './parameter';
import { NotificationService } from '../core/notification.service';

@Component({
  selector: 'parameter-config',
  styles: [`
  .split {
    max-width: 5.5vw;
  }
  `],
  template: `
  <ng-container>
    <li class="list-group-item">
      <div class="container-fluid">
        <div class="row">
          <div class="col-md-5"><input #keyInput class="form-control" placeholder="Key" type="text"></div>
          <div class="col-md-5"><input #valueInput class="form-control" placeholder="Value" type="text"></div>
          <div class="col-md-2"><button (click)="addParameter(keyInput.value, valueInput.value)" class="btn btn-default" type="button">Add</button></div>
        </div>
      </div>
    </li>
    <li class="list-group-item" *ngFor="let x of params">
      <div class="container-fluid">
        <div class="row">
          <div class="col-md-5"><input disabled class="form-control" type="text" value="{{x.key}}"></div>
          <div class="col-md-5"><input disabled class="form-control" type="text" value="{{x.value}}"></div>
          <div class="col-md-2"><button (click)="removeParameter(x)" class="btn btn-default" type="button">Remove</button></div>
        </div>
      </div>
    </li>
  </ng-container>
  `
})
export class ParameterComponent {
  @Input() params: Parameter[];

  key = '';
  value = '';

  constructor(
    private notificationService: NotificationService,
  ) { }

  removeParameter(param: Parameter) {
    this.params = this.params.filter((item: Parameter) => {
      return item.uid != param.uid;
    });
  }

  addParameter(key: string, value: string) {
    if (!key) {
      this.notificationService.error('Missing key');

      return;
    }

    if (!value) {
      this.notificationService.error('Missing value');

      return;
    }

    let match = this.params.find((item: Parameter) => item.key === key);

    if (match != undefined) {
      this.notificationService.error(`Duplicate parameter key "${key}"`);

      return;
    }

    this.params.push(new Parameter(key, value));
  }
}
