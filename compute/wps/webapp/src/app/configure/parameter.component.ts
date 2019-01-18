import { Component, Input, Output, EventEmitter, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';

import { Process } from './process';
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
    <li class="list-group-item" [formGroup]="form">
      <div class="container-fluid">
        <div class="row">
          <div class="col-md-5" [class.has-error]="keyControl.invalid">
            <input formControlName="key" #keyInput class="form-control" placeholder="Key" type="text">
          </div>
          <div class="col-md-5" [class.has-error]="valueControl.invalid">
            <input formControlName="value" #valueInput class="form-control" placeholder="Value" type="text">
          </div>
          <div class="col-md-2"><button (click)="addParameter(keyInput.value, valueInput.value)" class="btn btn-default" type="button">Add</button></div>
        </div>
      </div>
    </li>
    <li class="list-group-item" *ngFor="let x of process?.parameters">
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
export class ParameterComponent implements OnInit {
  @Input() process: Process;

  form: FormGroup;
  keyControl: FormControl;
  valueControl: FormControl;

  constructor(
    private notificationService: NotificationService,
  ) { }

  ngOnInit() {
    this.keyControl = new FormControl('', [
      Validators.required,
    ]);

    this.valueControl = new FormControl('', [
      Validators.required,
    ]);

    this.form = new FormGroup({
      key: this.keyControl,
      value: this.valueControl,
    });
  }

  addParameter(key: string, value: string) {
    if (this.form.invalid) {
      let error = Object.keys(this.form.controls).map((key: string) => {
        return [key, this.form.controls[key]];
      }).find((item: any[]) => {
        if (item[1].invalid) {
          return true;
        }

        return false;
      });

      let errorMessage = Object.keys((<FormControl>error[1]).errors).join(', ');

      this.notificationService.error(`Parameter error: ${error[0]} ${errorMessage}`);

      return;
    }

    let match = this.process.parameters.find((item: Parameter) => item.key === key);

    if (match != undefined) {
      this.notificationService.error(`Duplicate parameter key "${key}"`);

      return;
    }

    this.process.parameters.push(new Parameter(key, value));
  }

  removeParameter(item: Parameter) {
    this.process.parameters = this.process.parameters.filter((x: Parameter) => {
      if (item.uid === x.uid) {
        return false;
      }

      return true;
    });
  }
}
