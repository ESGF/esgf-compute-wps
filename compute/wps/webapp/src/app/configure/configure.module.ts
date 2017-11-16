import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { SharedModule } from '../shared/shared.module';
import { DndModule } from 'ng2-dnd';

import { AxisComponent } from './axis.component';
import { ParameterComponent } from './parameter.component';
import { MapComponent } from './map.component';
import { GeneralConfigComponent } from './general-config.component';
import { RegridComponent } from './regrid.component';
import { ConfigureComponent } from './configure.component';
import { WorkflowComponent } from './workflow.component';

import { ConfigureService } from './configure.service';

import { ConfigureRoutingModule } from './configure-routing.module';

@NgModule({
  imports: [ 
    CommonModule,
    FormsModule,
    ConfigureRoutingModule,
    SharedModule,
    DndModule,
  ],
  declarations: [
    ConfigureComponent,
    AxisComponent,
    ParameterComponent,
    MapComponent,
    GeneralConfigComponent,
    RegridComponent,
    WorkflowComponent,
  ],
  exports: [],
  providers: [
    ConfigureService,
  ]
})
export class ConfigureModule { }
