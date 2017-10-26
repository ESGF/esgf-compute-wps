import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { AdminComponent } from './admin.component';

const routes: Routes = [
  { path: '', component: AdminComponent }, 
];

@NgModule({
  imports: [ RouterModule.forChild(routes) ],
  declarations: [],
  exports: [ RouterModule ],
  providers: []
})
export class AdminRoutingModule { }
