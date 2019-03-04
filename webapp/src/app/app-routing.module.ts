import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HomeComponent } from './home.component';

import { AuthGuard } from './auth/auth-guard.service';

const routes: Routes = [
  { 
    path: '', children: [
      { path: '', component: HomeComponent },
      { path: 'auth', loadChildren: './auth/auth.module#AuthModule' },
      { path: 'user', canActivateChild: [AuthGuard], loadChildren: './user/user.module#UserModule' },
      { path: 'configure', canActivateChild: [AuthGuard], loadChildren: './configure/configure.module#ConfigureModule' },
    ]
  },
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  declarations: [],
  exports: [ RouterModule ],
  providers: []
})
export class AppRoutingModule { }
