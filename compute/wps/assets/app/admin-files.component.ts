import { Component, OnInit } from '@angular/core';

import { Header } from './pagination.component';
import { WPSService } from './wps.service';

@Component({
  selector: 'stats-files',
  styleUrls: ['./forms.css'],
  templateUrl: './admin-files.component.html'
})
export class AdminFilesComponent implements OnInit {
  files: Promise<any[]>;
  headers = [
    new Header('Name', 'name'),
    new Header('Host', 'host'),
    new Header('Requested', 'requested'),
    new Header('Variable', 'variable')
  ];

  constructor(
    private wpsService: WPSService
  ) { }

  ngOnInit() {
    this.files = this.wpsService.statsFiles()
      .then(response => response.data.files);
  }
}
