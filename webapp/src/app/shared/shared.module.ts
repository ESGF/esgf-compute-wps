import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { TabComponent, TabsComponent } from './tab.component';
import { PaginationComponent, PaginationTableComponent } from './pagination.component';
import { PanelComponent } from './panel.component';

import { StatsService } from './stats.service';

import { ThreddsPipe } from './thredds.pipe';

@NgModule({
  imports: [ CommonModule ],
  declarations: [
    TabComponent,
    TabsComponent,
    PaginationComponent,
    PaginationTableComponent,
    PanelComponent,
    ThreddsPipe
  ],
  exports: [
    CommonModule,
    TabComponent,
    TabsComponent,
    PaginationComponent,
    PaginationTableComponent,
    PanelComponent,
    ThreddsPipe
  ],
  providers: [ StatsService ]
})
export class SharedModule { }
