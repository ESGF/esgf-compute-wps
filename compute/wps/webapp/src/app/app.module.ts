import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from './app.component';
import { HomeComponent } from './home.component';

import { AdminModule } from './admin/admin.module';
import { AuthModule } from './auth/auth.module';
import { CoreModule } from './core/core.module';
import { UserModule } from './user/user.module';
import { SharedModule } from './shared/shared.module';
import { ConfigureModule } from './configure/configure.module';

import { AppRoutingModule } from './app-routing.module';

@NgModule({
  imports: [
    BrowserModule,
    CoreModule,
    SharedModule,
    AdminModule,
    AuthModule,
    ConfigureModule,
    UserModule,
    AppRoutingModule
  ],
  declarations: [ AppComponent, HomeComponent ],
  bootstrap: [ AppComponent ]
})

export class AppModule { }
