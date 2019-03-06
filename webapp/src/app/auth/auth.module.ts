import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { LoginCallbackComponent } from './login-callback.component';
import { LoginOpenIDComponent } from './login-openid.component';
import { LogoutComponent } from './logout.component';

import { AuthGuard } from './auth-guard.service';

import { AuthRoutingModule } from './auth-routing.module';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    AuthRoutingModule
  ],
  declarations: [
    LoginCallbackComponent,
    LoginOpenIDComponent,
    LogoutComponent,
  ],
  exports: [],
  providers: [ AuthGuard ]
})
export class AuthModule { }
