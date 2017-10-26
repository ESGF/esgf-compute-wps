import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';

import { AdminComponent } from './admin.component';
import { AdminFilesComponent } from './admin-files.component';
import { AdminProcessesComponent } from './admin-processes.component';

import { AdminRoutingModule } from './admin-routing.module';

@NgModule({
  imports: [ 
    CommonModule,
    SharedModule,
    AdminRoutingModule
  ],
  declarations: [
    AdminComponent,
    AdminFilesComponent,
    AdminProcessesComponent
  ],
  exports: [],
  providers: []
})
export class AdminModule { }
