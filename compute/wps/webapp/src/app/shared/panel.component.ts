import { Component, Input, Output, EventEmitter, ContentChildren, QueryList, AfterContentInit } from '@angular/core';

declare var $: any;

@Component({
  selector: 'panel',
  styles: [`
  .panel {
    margin-top: 5px;
  }
  `],
  template: `
  <div class="panel panel-default">
    <div class="panel-heading" role="tab" id="{{heading}}">
      <h4 class="panel-title">
        <a (click)="onToggle.emit(this)" role="button" data-toggle="collapse">
          {{title}}
        </a>
      </h4>
    </div>
    <div id="{{collapse}}" class="panel-collapse collapse" role="tabpanel">
      <div class="panel-body">
        <ng-content></ng-content>
      </div>
    </div>
  </div>
  `
})
export class PanelComponent {
  @Input() title: string;
  @Output() onToggle = new EventEmitter<PanelComponent>();

  uid: string;
  
  constructor() {
    this.uid = Math.random().toString(16).slice(2);
  }

  get collapse() {
    return `collapse${this.uid}`;
  }

  get heading() {
    return `heading${this.uid}`;
  }
}

@Component({
  selector: 'panel-group',
  template: `
  <div class="panel-group" role="tablist">
    <ng-content></ng-content>
  </div>
  `
})
export class PanelGroupComponent implements AfterContentInit {
  @ContentChildren(PanelComponent) panels: QueryList<PanelComponent>;

  ngAfterContentInit() {
    this.panels.forEach((panel: PanelComponent) => {
      panel.onToggle.subscribe((panel: PanelComponent) => { this.onToggle(panel); });
    });
  }

  onToggle(panel: PanelComponent) {
    $(`#${panel.collapse}`).collapse('toggle');

    this.panels.forEach((item: PanelComponent) => {
      if (panel !== item) {
        $(`#${item.collapse}`).collapse('hide');
      }
    });
  }
}
