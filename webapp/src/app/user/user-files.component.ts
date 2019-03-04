import { Component, OnInit, Pipe, PipeTransform } from '@angular/core';

import { Header } from '../shared/pagination.component';
import { FileStat, StatsService } from '../shared/stats.service';

@Component({
  selector: 'user-files',
  styleUrls: ['../forms.css'],
  templateUrl: './user-files.component.html', 
})
export class UserFilesComponent implements OnInit {
  headers = [
    new Header('Name', 'name'), 
    new Header('Host', 'host'),
    new Header('Requested', 'requested'),
    new Header('Variable', 'variable'),
    new Header('Last Requested', 'requested_date')
  ];

  files: Promise<FileStat[]>;

  constructor(
    private statsService: StatsService
  ) { }

  ngOnInit() {
    this.files = this.statsService.userFiles();
  }
}
