import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpModule } from '@angular/http';
import { DndModule } from 'ng2-dnd';
import { RouterModule } from '@angular/router';

import { AuthService } from './auth.service';
import { ConfigService } from './config.service';
import { NotificationService } from './notification.service';
import { WPSService } from './wps.service';

@NgModule({
  imports: [
    RouterModule,
    CommonModule,
    HttpModule,
    DndModule.forRoot(),
  ],
  declarations: [],
  exports: [],
  providers: [
    AuthService,
    ConfigService,
    NotificationService,
    WPSService
  ]
})
export class CoreModule { }
