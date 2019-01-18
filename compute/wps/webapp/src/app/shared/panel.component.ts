import { Component, Input } from '@angular/core';

@Component({
  selector: 'panel',
  styles: [`
  .scrollable {
    max-height: 50vh;
    overflow-y: scroll;
  }

  .panel {
    margin-top: 5px;
  }
  `],
  template: `
  <div class="panel panel-default">
    <div class="panel-heading" role="tab">
      <h4 class="panel-title">
        <a *ngIf="collapse==true; else noCollapse" class="collapsed" role="button" data-toggle="collapse" href="#{{uid}}">
          {{title}}
        </a>
        <ng-template #noCollapse>
          {{title}}
        </ng-template>
      </h4>
    </div>
    <div *ngIf="collapse==true; else innerWrap" class="panel-collapse collapse" id="{{uid}}">
      <ng-container *ngTemplateOutlet="innerWrap"></ng-container>
    </div>
    <ng-template #innerWrap> 
      <ul *ngIf="listGroup==true" class="list-group" [class.scrollable]="scrollable">
        <ng-container *ngTemplateOutlet="content"></ng-container>
      </ul>
      <div *ngIf="listGroup==false" class="panel-body" [class.scrollable]="scrollable">
        <ng-container *ngTemplateOutlet="content"></ng-container>
      </div>
      <ng-template #content>
        <ng-content></ng-content>
      </ng-template>
    </ng-template>
  </div>
  `
})
export class PanelComponent {
  @Input() title: string;
  @Input() listGroup = false;
  @Input() scrollable = false;
  @Input() collapse = true;
  @Input() uid: string;
}
