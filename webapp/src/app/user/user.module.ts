import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { SharedModule } from '../shared/shared.module';

import { UserProfileComponent } from './user-profile.component';
import { UserDetailsComponent } from './user-details.component';
import { UserFilesComponent } from './user-files.component';
import { UserProcessesComponent } from './user-processes.component';
import { JobsComponent } from './jobs.component';

import { UserService }from './user.service';

import { UserRoutingModule } from './user-routing.module';

@NgModule({
  imports: [ 
    CommonModule,
    FormsModule,
    SharedModule,
    UserRoutingModule,
  ],
  declarations: [
    UserProfileComponent,
    UserDetailsComponent,
    UserFilesComponent,
    UserProcessesComponent,
    JobsComponent,
  ],
  exports: [],
  providers: [ UserService ]
})
export class UserModule { }
