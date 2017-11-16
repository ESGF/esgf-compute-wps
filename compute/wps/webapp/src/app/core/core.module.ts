import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpModule } from '@angular/http';
import { DndModule } from 'ng2-dnd';

import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';
import { WPSService } from './wps.service';

@NgModule({
  imports: [
    CommonModule,
    HttpModule,
    DndModule.forRoot(),
  ],
  declarations: [],
  exports: [],
  providers: [
    AuthService,
    NotificationService,
    WPSService
  ]
})
export class CoreModule { }
