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

import { AuthService } from './auth.service';

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
            path: 'profile',
            component: UpdateUserFormComponent
          },
          {
            path: 'login',
            component: LoginFormComponent
          },
          {
            path: 'logout',
            component: LogoutComponent
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
  ],
  providers: [
    AuthService 
  ],
  bootstrap: [ AppComponent ]
})

export class AppModule { }
