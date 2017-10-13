import { 
  AfterContentInit, 
  Component, 
  ContentChild, 
  ContentChildren, 
  QueryList, 
  Input } from '@angular/core';

@Component({
  selector: 'tab',
  template: `
    <div role="tabpanel" class="tab-pane" [hidden]="!active" id="{{title}}">
      <ng-content></ng-content> 
    </div>
  `
})
export class TabComponent {
  @Input('tabTitle') title: string;
  @Input() active: boolean;
}

@Component({
  selector: 'tabs',
  template: `
    <ul class="nav nav-tabs" role="tablist">
      <li *ngFor="let tab of tabs" role="presentation" (click)="selectTab(tab)" [class.active]="tab.active">
        <a href="#{{tab.title}}" role="tab" data-toggle="tab">{{tab.title}}</a>
      </li>
    </ul>
    <div class="tab-content">
      <ng-content></ng-content>
    </div>
  `
})
export class TabsComponent implements AfterContentInit {
  @ContentChildren(TabComponent) tabs: QueryList<TabComponent>;

  ngAfterContentInit() {
    let activeTabs = this.tabs.filter((tab) => tab.active);

    if (activeTabs.length === 0) {
      this.selectTab(this.tabs.first);
    }
  }

  selectTab(tab: TabComponent) {
    this.tabs.toArray().forEach((tab) => tab.active = false);

    tab.active = true;
  }
}
