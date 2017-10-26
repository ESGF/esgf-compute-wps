import { Component } from '@angular/core';

@Component({
  template: `
  <div class="container">
    <tabs>
      <tab tabTitle="Files">
        <stats-files></stats-files>
      </tab>
      <tab tabTitle="Processes">
        <stats-processes></stats-processes>
      </tab>
    </tabs>
  </div>
  `
})
export class AdminComponent { }
