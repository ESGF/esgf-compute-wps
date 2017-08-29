import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { RouterModule } from '@angular/router';

import { AppComponent } from './app.component';
import { HomeComponent } from './home.component';
import { CreateUserFormComponent } from './create-user-form.component';
import { UpdateUserFormComponent } from './update-user-form.component';
import { LoginFormComponent } from './login-form.component';
import { LogoutComponent } from './logout.component';
import { ConfigureComponent } from './configure.component';
import { DimensionComponent } from './dimension.component';
import { JobsComponent } from './jobs.component';

import { AuthService } from './auth.service';
import { AuthGuard } from './auth-guard.service';
import { NotificationService } from './notification.service';

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
            path: '',
            component: HomeComponent
          },
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
            path: 'jobs',
            component: JobsComponent
          },
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
    HomeComponent,
    CreateUserFormComponent,
    UpdateUserFormComponent,
    LoginFormComponent,
    LogoutComponent,
    ConfigureComponent,
    DimensionComponent,
    JobsComponent
  ],
  providers: [
    AuthService,
    AuthGuard,
    NotificationService
  ],
  bootstrap: [ AppComponent ]
})

export class AppModule { }
