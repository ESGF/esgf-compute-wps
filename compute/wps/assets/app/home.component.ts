import { Component } from '@angular/core';

@Component({
  template: `
  <div class="container">
    <h1 class="text-center">Welcome to LLNL's CWT WPS server</h1>
    <div class="row">
      <div class="col-md-12">
        <br>
        <p>
          To get started either create an <a [routerLink]="['/wps/home/create']">account</a> or login into using your <a [routerLink]="['/wps/home/login/openid']">IDP</a>.
        </p>
      </div>
    </div>
  </div>
  `
})
export class HomeComponent {}
