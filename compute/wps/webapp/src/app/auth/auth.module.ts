import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { CreateUserComponent } from './create-user.component';
import { LoginComponent, LoginCallbackComponent } from './login.component';
import { LoginOpenIDComponent } from './login-openid.component';
import { LogoutComponent } from './logout.component';
import { ForgotUsernameComponent } from './forgot-username.component';
import { ForgotPasswordComponent } from './forgot-password.component';
import { ResetPasswordComponent } from './reset-password.component';

import { AuthGuard } from './auth-guard.service';

import { AuthRoutingModule } from './auth-routing.module';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    AuthRoutingModule
  ],
  declarations: [
    CreateUserComponent,
    LoginComponent,
    LoginCallbackComponent,
    LoginOpenIDComponent,
    LogoutComponent,
    ForgotUsernameComponent,
    ForgotPasswordComponent,
    ResetPasswordComponent,
  ],
  exports: [],
  providers: [ AuthGuard ]
})
export class AuthModule { }
