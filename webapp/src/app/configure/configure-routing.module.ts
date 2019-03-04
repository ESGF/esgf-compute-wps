import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { ConfigureComponent } from './configure.component';

const routes: Routes = [
  { path: '', component: ConfigureComponent },
];

@NgModule({
  imports: [ RouterModule.forChild(routes) ],
  declarations: [],
  exports: [ RouterModule ],
  providers: [],
})
export class ConfigureRoutingModule { }
