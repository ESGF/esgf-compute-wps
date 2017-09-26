import { Component, OnInit } from '@angular/core';

import { WPSService } from './wps.service';

@Component({
  selector: 'stats-files',
  styleUrls: ['./forms.css'],
  templateUrl: './stats-files.component.html'
})
export class StatsFilesComponent implements OnInit {
  files: Array<any> = null;

  constructor(
    private wpsService: WPSService
  ) { }

  ngOnInit() {
    this.wpsService.statsFiles()
      .then(response => {
        this.files = response.data.files;
      });
  }
}
