import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { UserProfileComponent } from './user-profile.component';
import { JobsComponent } from './jobs.component';

const routes: Routes = [
  { path: 'profile', component: UserProfileComponent }, 
  { path: 'jobs', component: JobsComponent },
  { path: '', redirectTo: 'profile', pathMatch: 'full' },
];

@NgModule({
  imports: [ RouterModule.forChild(routes) ],
  declarations: [],
  exports: [ RouterModule ],
  providers: []
})
export class UserRoutingModule { }
