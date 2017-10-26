import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpModule } from '@angular/http';

import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';
import { WPSService } from './wps.service';

@NgModule({
  imports: [ CommonModule, HttpModule ],
  declarations: [],
  exports: [],
  providers: [
    AuthService,
    NotificationService,
    WPSService
  ]
})
export class CoreModule { }
