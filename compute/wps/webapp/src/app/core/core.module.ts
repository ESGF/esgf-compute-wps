import { NgModule, ModuleWithProviders, Optional, SkipSelf } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpModule } from '@angular/http';
import { DndModule } from 'ng2-dnd';
import { RouterModule } from '@angular/router';

import { AuthService } from './auth.service';
import { ConfigService } from './config.service';
import { NotificationComponent } from './notification.component';
import { NotificationService } from './notification.service';
import { WPSService } from './wps.service';

@NgModule({
  imports: [
    RouterModule,
    CommonModule,
    HttpModule,
    DndModule.forRoot(),
  ],
  declarations: [
    NotificationComponent,
  ],
  exports: [
    NotificationComponent,
  ],
  providers: [
    ConfigService,
    WPSService
  ]
})
export class CoreModule { 
  static forRoot(): ModuleWithProviders {
    return {
      ngModule: CoreModule,
      providers: [
        NotificationService,
      ]
    };
  }
}
