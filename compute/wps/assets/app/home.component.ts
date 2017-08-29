import { Component } from '@angular/core';

@Component({
  template: `
  <div class="container">
    <div class="row">
      <div class="col-md-12">
        <p>
          Welcome to LLNL's CWT WPS server. 
        </p>
        <p>
          To get started you must create an <a [routerLink]="['/wps/home/create']">account</a>.
        </p>
      </div>
    </div>
  </div>
  `
})
export class HomeComponent {}
