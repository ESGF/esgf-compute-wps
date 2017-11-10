import { Component, EventEmitter, Input, Output, OnInit } from '@angular/core';

import { Axis } from './axis.component';
import { LAT_NAMES, LNG_NAMES, Configuration, SearchResult, ConfigureService } from './configure.service';
import { NotificationService } from '../core/notification.service';

@Component({
  selector: 'general-config',
  template: `
  <div>
    <div class="form-group">
      <label for="process">Dataset</label>
      <select [(ngModel)]="config.datasetID" (change)="loadDataset()" class="form-control" id="datasetID" name="datasetID">
        <option *ngFor="let dataset of datasetIDs">{{dataset}}</option>
      </select>
    </div>
    <div class="form-group">
      <label for="process">Process</label>
      <select [(ngModel)]="config.process" class="form-control" id="process" name="process">
        <option *ngFor="let proc of processes | async">{{proc}}</option>
      </select>
    </div>
    <div class="form-group">
      <label for="variable">Variable</label>
      <select [(ngModel)]="config.variable" (change)="loadVariable()" class="form-control" id="variable" name="variable">
        <option *ngFor="let v of variables" [value]="v">{{v}}</option>
      </select>
    </div>
    <div>
      <button (click)="onDownload()" type="submit" class="btn btn-default">Script</button>
      <button (click)="onExecute()" type="submit" class="btn btn-default">Execute</button>
    </div>
  </div>
  `
})
export class GeneralConfigComponent implements OnInit { 
  @Input() config: Configuration;

  datasetIDs: string[];
  variables: string[];

  processes: Promise<string[]>;
  result: SearchResult;

  constructor(
    private configService: ConfigureService,
    private notificationService: NotificationService,
  ) { }

  ngOnInit() {
    this.processes = this.configService.processes()
      .then(data => {
        let p = data.sort();

        this.config.process = p[0];

        return p;
      })
      .catch(error => {
        this.notificationService.error(error); 

        return [];
      });

    this.loadDataset();
  }
  
  loadDataset() {
    this.configService.searchESGF(this.config)
      .then(data => {
        this.result = data;

        this.variables = Object.keys(this.result).sort();

        this.config.variable = this.variables[0];

        this.loadVariable();
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  loadVariable() {
    if (this.result[this.config.variable].axes === undefined) {
      this.configService.searchVariable(this.config)
        .then(data => {
          this.result[this.config.variable]['axes'] = data;

          this.setDataset();
        })
        .catch(error => {
          this.notificationService.error(error);
        });
    } else {
      this.setDataset();
    }
  }

  setDataset() {
    this.config.dataset = Object.assign({}, this.result[this.config.variable]);

    this.config.dataset.axes = this.config.dataset.axes.map((axis: Axis) => {
      return {step: 1, ...axis}; 
    });
  }

  resetDomain() {
    if (this.config.dataset.axes !== undefined) {
      this.config.dataset.axes.forEach((axis: Axis) => {
        if (LAT_NAMES.indexOf(axis.id) >= 0 || LNG_NAMES.indexOf(axis.id) >= 0) {
          let filtered = this.result[this.config.variable].axes.filter((value: Axis) => {
            return axis.id === value.id;
          });

          if (filtered.length > 0) {
            axis.start = filtered[0].start;

            axis.stop = filtered[0].stop;
          }
        }
      });
    }
  }

  onDownload() {
    this.configService.downloadScript(this.config)
      .then(data => {
          let url = URL.createObjectURL(new Blob([data.text]));

          let a = document.createElement('a');

          a.href = url;
          a.target = '_blank';
          a.download = data.filename;

          a.click();
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  onExecute() {
    this.configService.execute(this.config)
      .then((data: any) => {
        let parser = new DOMParser();
        let xml = parser.parseFromString(data.report, 'text/xml');
        let el = xml.getElementsByTagName('wps:ExecuteResponse');
        let link = '';

        if (el.length > 0) {
          let statusLocation = el[0].attributes.getNamedItem('statusLocation').value;

          let jobID = statusLocation.substring(statusLocation.lastIndexOf('/')+1);

          link = `/wps/home/user/jobs`;
        }
        
        this.notificationService.message('Succesfully submitted job', link);
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }
}
