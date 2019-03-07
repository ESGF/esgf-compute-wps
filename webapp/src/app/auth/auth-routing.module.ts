import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Routes, RouterModule } from '@angular/router';

import { LoginCallbackComponent } from './login-callback.component';
import { LoginOpenIDComponent } from './login-openid.component';
import { LogoutComponent } from './logout.component';

import { AuthGuard } from '../auth/auth-guard.service';

const routes: Routes = [
  { path: 'login/callback', component: LoginCallbackComponent },
  { path: 'login', component: LoginOpenIDComponent },
  { path: 'logout', canActivate: [AuthGuard], component: LogoutComponent },
  //{ path: '', redirectTo: '/wps/home', pathMatch: 'full' },
];

@NgModule({
  imports: [ RouterModule.forChild(routes) ],
  declarations: [],
  exports: [ RouterModule ],
  providers: []
})
export class AuthRoutingModule { }
