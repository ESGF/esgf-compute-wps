import { Component, Input } from '@angular/core';

@Component({
  selector: 'panel',
  styles: [`
  .scrollable {
    max-height: 50vh;
    overflow-y: scroll;
  }
  `],
  template: `
  <div class="panel panel-default">
    <div class="panel-heading" role="tab">
      <h4 class="panel-title">
        <a class="collapsed" role="button" data-toggle="collapse" href="#{{uid}}">
          {{title}}
        </a>
      </h4>
    </div>
    <div class="panel-collapse collapse" id="{{uid}}">
      <ul *ngIf="listGroup==true" class="list-group" [class.scrollable]="scrollable">
        <ng-container *ngTemplateOutlet="content"></ng-container>
      </ul>
      <div *ngIf="listGroup==false" class="panel-body" [class.scrollable]="scrollable">
        <ng-container *ngTemplateOutlet="content"></ng-container>
      </div>
      <ng-template #content>
        <ng-content></ng-content>
      </ng-template>
    </div>
  </div>
  `
})
export class PanelComponent {
  @Input() title: string;
  @Input() listGroup = false;
  @Input() scrollable = false;

  uid: string;
  
  constructor() { 
    this.uid = Math.random().toString(16).slice(2);
  }
}
