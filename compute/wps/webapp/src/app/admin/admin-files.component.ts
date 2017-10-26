import { Component, OnInit } from '@angular/core';

import { Header } from '../shared/pagination.component';
import { FileStat, StatsService } from '../shared/stats.service';

@Component({
  selector: 'stats-files',
  styleUrls: ['../forms.css'],
  templateUrl: './admin-files.component.html'
})
export class AdminFilesComponent implements OnInit {
  headers = [
    new Header('Name', 'name'),
    new Header('Host', 'host'),
    new Header('Requested', 'requested'),
    new Header('Variable', 'variable')
  ];

  files: Promise<FileStat[]>;

  constructor(
    private statsService: StatsService
  ) { }

  ngOnInit() {
    this.files = this.statsService.files();
  }
}
