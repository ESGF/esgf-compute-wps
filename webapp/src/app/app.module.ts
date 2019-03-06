import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { JoyrideModule } from 'ngx-joyride';

import { AppComponent } from './app.component';
import { HomeComponent } from './home.component';

import { AuthService } from './core/auth.service';
import { AuthModule } from './auth/auth.module';
import { CoreModule } from './core/core.module';
import { UserModule } from './user/user.module';
import { SharedModule } from './shared/shared.module';
import { ConfigureModule } from './configure/configure.module';

import { AppRoutingModule } from './app-routing.module';

@NgModule({
  imports: [
    JoyrideModule.forRoot(),
    BrowserModule,
    CoreModule.forRoot(),
    SharedModule,
    AuthModule,
    ConfigureModule,
    UserModule,
    AppRoutingModule
  ],
  declarations: [ AppComponent, HomeComponent ],
  bootstrap: [ AppComponent ],
  providers: [ AuthService ],
})

export class AppModule { }
