import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { RouterModule } from '@angular/router';

import { AppComponent } from './app.component';
import { HomeComponent } from './home.component';
import { CreateUserComponent } from './create-user.component';
import { UserProfileComponent } from './user-profile.component';
import { UserDetailsComponent } from './user-details.component';
import { UserFilesComponent } from './user-files.component';
import { UserProcessesComponent } from './user-processes.component';
import { LoginComponent, LoginCallbackComponent } from './login.component';
import { LoginOpenIDComponent } from './login-openid.component';
import { LogoutComponent } from './logout.component';
import { ConfigureComponent, AxisComponent } from './configure.component';
import { JobsComponent } from './jobs.component';
import { TabComponent, TabsComponent } from './tab.component';
import { AdminFilesComponent } from './admin-files.component';
import { AdminProcessesComponent } from './admin-processes.component';
import { ForgotUsernameComponent } from './forgot-username.component';
import { ForgotPasswordComponent, ResetPasswordComponent } from './forgot-password.component';
import { PaginationComponent, PaginationTableComponent } from './pagination.component';
import { AdminComponent } from './admin.component';

import { AuthService } from './auth.service';
import { AuthGuard } from './auth-guard.service';
import { StatsService } from './stats.service';
import { NotificationService } from './notification.service';

import { ThreddsPipe } from './user-files.component';

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
            component: CreateUserComponent
          },
          {
            path: 'login',
            component: LoginComponent
          },
          {
            path: 'login/openid',
            component: LoginOpenIDComponent
          },
          {
            path: 'login/callback',
            component: LoginCallbackComponent
          },
          {
            path: 'login/forgot/username',
            component: ForgotUsernameComponent
          },
          {
            path: 'login/forgot/password',
            component: ForgotPasswordComponent
          },
          {
            path: 'login/reset',
            component: ResetPasswordComponent
          }
        ]
      },
      {
        path: 'wps/home',
        canActivate: [AuthGuard],
        children: [
          {
            path: 'admin',
            component: AdminComponent
          },
          {
            path: 'jobs',
            component: JobsComponent
          },
          {
            path: 'profile',
            component: UserProfileComponent
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
    CreateUserComponent,
    UserProfileComponent,
    UserDetailsComponent,
    UserFilesComponent,
    UserProcessesComponent,
    LoginComponent,
    LoginCallbackComponent,
    LoginOpenIDComponent,
    LogoutComponent,
    ConfigureComponent,
    AxisComponent,
    JobsComponent,
    TabComponent,
    TabsComponent,
    ThreddsPipe,
    AdminFilesComponent,
    AdminProcessesComponent,
    ForgotUsernameComponent,
    ForgotPasswordComponent,
    ResetPasswordComponent,
    PaginationComponent,
    PaginationTableComponent,
    AdminComponent
  ],
  providers: [
    AuthService,
    AuthGuard,
    NotificationService,
    StatsService
  ],
  bootstrap: [ AppComponent ]
})

export class AppModule { }
