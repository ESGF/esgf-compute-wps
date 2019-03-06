import { Component, Input, Output, EventEmitter, OnInit, ViewChildren, QueryList } from '@angular/core';

import { Domain } from './domain';
import { Axis } from './axis';
import { AxisComponent } from './axis.component';

@Component({
  selector: 'domain-config',
  styles: [`
  `],
  template: `
  <ng-container>
    <li class="list-group-item">
      <div class="dropdown">
        <button class="btn btn-default dropdown-toggle" id="axesDropdown" type="button" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
          Axes
          <span class="caret"></span>
        </button>
        <ul class="dropdown-menu" aria-labelledby="axesDropdown">
          <li>
            <a (click)="addCustomAxis()">Custom</a>
          </li>
          <li role="separator" class="divider"></li>
          <li *ngIf="candidateDomain?.temporal">
            <a (click)="addAxis(candidateDomain.temporal, true)">{{candidateDomain.temporal.id}}</a>
          </li>
          <li *ngFor="let x of candidateDomain?.spatial">
            <a (click)="addAxis(x)">{{x.id}}</a>
          </li>
        </ul>
      </div>
    </li>
    <li *ngIf="domain?.temporal" class="list-group-item">
      <axis-config (onRemove)="domain.remove($event)" [axis]="domain?.temporal"></axis-config>
    </li>
    <li *ngFor="let x of domain?.spatial" class="list-group-item">
      <axis-config (onRemove)="domain.remove($event)" [axis]="x"></axis-config>
    </li>
  </ng-container>
  `
})
export class DomainComponent implements OnInit {
  @Input() domain: Domain;
  @Input() candidateDomain: Domain;

  @ViewChildren(AxisComponent)
  private axes: QueryList<AxisComponent>;

  constructor() { }

  ngOnInit() { }

  errors() {
    return this.axes.find((item: AxisComponent) => {
      if (item.form.invalid) {
        return true;
      }

      return false;
    }) != undefined;
  }

  addCustomAxis() {
    let axis = new Axis();

    axis.custom = true;

    this.domain.spatial.push(axis);
  }

  addAxis(axis: Axis, isTime = false) {
    if (isTime && this.domain.temporal != null) {
      return;
    } else {
      let match = this.domain.spatial.findIndex((item: Axis) => {
        if (item.id == axis.id) {
          return true;
        }

        return false;
      });

      if (match != -1) {
        return;
      }
    }

    let newAxis = new Axis();

    Object.assign(newAxis, axis, {step: 1.0});

    if (isTime) {
      this.domain.temporal = newAxis;
    } else {
      this.domain.spatial.push(newAxis);
    }
  }
}
