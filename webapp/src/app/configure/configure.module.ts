import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ReactiveFormsModule } from '@angular/forms';
import { SharedModule } from '../shared/shared.module';
import { DndModule } from 'ng2-dnd';
import { CoreModule } from '../core/core.module';

import { JoyrideModule } from 'ngx-joyride';

import { DomainComponent } from './domain.component';
import { AxisComponent } from './axis.component';
import { ParameterComponent } from './parameter.component';
import { RegridComponent } from './regrid.component';
import { ConfigureComponent } from './configure.component';
import { FilterPipe } from './filter.pipe';
import { FilenamePipe } from './filename.pipe';
import { EnumPipe } from './enum.pipe';
import { WorkflowComponent } from './workflow.component';
import { ProcessConfigureComponent } from './process-configure.component';
import { ProcessAbstractComponent } from './process-abstract.component';

import { ConfigureService } from './configure.service';

import { ConfigureRoutingModule } from './configure-routing.module';

@NgModule({
  imports: [ 
    JoyrideModule.forChild(),
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    ConfigureRoutingModule,
    SharedModule,
    CoreModule,
    DndModule,
  ],
  declarations: [
    DomainComponent,
    ConfigureComponent,
    AxisComponent,
    ParameterComponent,
    RegridComponent,
    FilterPipe,
    FilenamePipe,
    EnumPipe,
    WorkflowComponent,
    ProcessConfigureComponent,
    ProcessAbstractComponent,
  ],
  exports: [],
  providers: [
    ConfigureService,
    FilterPipe,
    FilenamePipe,
    EnumPipe,
  ]
})
export class ConfigureModule { }
