import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { ConfigureService } from './configure.service';

class Config {
  process: string;
  variable: string;
  files: string;
}

@Component({
  templateUrl: './configure.component.html',
  providers: [ConfigureService]
})
export class ConfigureComponent implements OnInit  { 
  PROCESSES = ['CDAT.aggregate', 'CDAT.subset'];

  config: Config = new Config();
  variables: string[] = [];
  files: string[] = [];

  constructor(
    private route: ActivatedRoute,
    private configService: ConfigureService
  ) { }

  ngOnInit() {
    this.route.queryParams.subscribe(params => this.loadData(params));

    this.config.process = this.PROCESSES[0];
  }

  onSubmit(): void {
    this.config.files = this.files.filter((x) => { return (x.indexOf(`/${this.config.variable}_`) > 0); }).join(',');

    this.configService.downloadScript(this.config);
  }

  loadData(params: any): void {
    this.configService.searchESGF(params)
      .then(response => this.handleLoadData(response));
  }

  handleLoadData(response: any): void {
    if (response.status && response.status === 'success') {
      this.files = response.data.files;

      this.variables = response.data.variables;

      this.config.variable = this.variables[0];
    }
  }
}
