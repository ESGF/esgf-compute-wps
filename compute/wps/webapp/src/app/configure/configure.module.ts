import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { AxisComponent } from './axis.component';
import { ParameterComponent } from './parameter.component';
import { ConfigureComponent } from './configure.component';

import { ConfigureService } from './configure.service';

import { ConfigureRoutingModule } from './configure-routing.module';

@NgModule({
  imports: [ 
    CommonModule,
    FormsModule,
    ConfigureRoutingModule,
  ],
  declarations: [
    ConfigureComponent,
    AxisComponent,
    ParameterComponent,
  ],
  exports: [],
  providers: [
    ConfigureService,
  ]
})
export class ConfigureModule { }
