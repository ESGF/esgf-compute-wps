import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Routes, RouterModule } from '@angular/router';

import { CreateUserComponent } from './create-user.component';
import { LoginComponent } from './login.component';
import { LoginCallbackComponent } from './login-callback.component';
import { LoginOpenIDComponent } from './login-openid.component';
import { LogoutComponent } from './logout.component';
import { ForgotUsernameComponent } from './forgot-username.component';
import { ForgotPasswordComponent } from './forgot-password.component';
import { ResetPasswordComponent } from './reset-password.component';

import { AuthGuard } from '../auth/auth-guard.service';

const routes: Routes = [
  { path: 'create', component: CreateUserComponent },
  { path: 'login', component: LoginComponent },
  { path: 'login/callback', component: LoginCallbackComponent },
  { path: 'login/openid', component: LoginOpenIDComponent },
  { path: 'logout', canActivate: [AuthGuard], component: LogoutComponent },
  { path: 'reset', component: ResetPasswordComponent },
  { path: 'forgot/username', component: ForgotUsernameComponent },
  { path: 'forgot/password', component: ForgotPasswordComponent },
  { path: '', redirectTo: '/wps/home', pathMatch: 'full' },
];

@NgModule({
  imports: [ RouterModule.forChild(routes) ],
  declarations: [],
  exports: [ RouterModule ],
  providers: []
})
export class AuthRoutingModule { }
