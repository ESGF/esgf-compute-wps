import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { RouterModule } from '@angular/router';

import { AppComponent } from './app.component';
import { CreateUserFormComponent } from './create-user-form.component';

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
          }
        ]
      }
    ])
  ],
  declarations: [
    AppComponent,
    CreateUserFormComponent
  ],
  bootstrap: [ AppComponent ]
})

export class AppModule { }
