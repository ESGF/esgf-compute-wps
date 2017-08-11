import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { RouterModule } from '@angular/router';

import { AppComponent } from './app.component';
import { CreateUserFormComponent } from './create-user-form.component';
import { UpdateUserFormComponent } from './update-user-form.component';
import { LoginFormComponent } from './login-form.component';
import { LogoutComponent } from './logout.component';
import { ConfigureComponent } from './configure.component';
import { DimensionComponent } from './dimension.component';

import { AuthService } from './auth.service';
import { AuthGuard } from './auth-guard.service';

@NgModule({
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,
    RouterModule.forRoot([
      {
        path: 'wps/home',
        children: [
          {
            path: 'create',
            component: CreateUserFormComponent
          },
          {
            path: 'login',
            component: LoginFormComponent
          }
        ]
      },
      {
        path: 'wps/home',
        canActivate: [AuthGuard],
        children: [
          {
            path: 'profile',
            component: UpdateUserFormComponent
          },
          {
            path: 'logout',
            component: LogoutComponent
          },
          {
            path: 'configure',
            component: ConfigureComponent
          }
        ]
      }
    ])
  ],
  declarations: [
    AppComponent,
    CreateUserFormComponent,
    UpdateUserFormComponent,
    LoginFormComponent,
    LogoutComponent,
    ConfigureComponent,
    DimensionComponent
  ],
  providers: [
    AuthService,
    AuthGuard
  ],
  bootstrap: [ AppComponent ]
})

export class AppModule { }
