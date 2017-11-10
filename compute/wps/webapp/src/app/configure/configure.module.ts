import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { AxisComponent } from './axis.component';
import { ParameterComponent } from './parameter.component';
import { MapComponent } from './map.component';
import { GeneralConfigComponent } from './general-config.component';

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
    MapComponent,
    GeneralConfigComponent,
  ],
  exports: [],
  providers: [
    ConfigureService,
  ]
})
export class ConfigureModule { }
