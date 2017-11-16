import { Component, EventEmitter, Input, Output, OnInit } from '@angular/core';

import { Axis } from './axis.component';
import { LAT_NAMES, LNG_NAMES, Configuration, DatasetCollection, VariableCollection, ConfigureService } from './configure.service';
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
        <option *ngFor="let proc of processes">{{proc}}</option>
      </select>
    </div>
    <div class="form-group">
      <label for="variable">Variable</label>
      <select [(ngModel)]="config.variableID" (change)="loadVariable()" class="form-control" id="variable" name="variable">
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
  @Input() processes: string[];
  @Input() datasetIDs: string[];

  variables: string[];

  datasets: DatasetCollection;

  constructor(
    private configService: ConfigureService,
    private notificationService: NotificationService,
  ) { }

  ngOnInit() {
    this.datasets = {} as DatasetCollection;

    this.datasetIDs.forEach((id: string) => {
      this.datasets[id] = {id: id, variables: {} as VariableCollection};
    });

    this.loadDataset();
  }
  
  loadDataset() {
    this.configService.searchESGF(this.config)
      .then(data => {
        this.datasets[this.config.datasetID].variables = data;

        this.variables = Object.keys(data).sort();

        this.config.variableID = this.variables[0];

        this.loadVariable();
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  loadVariable() {
    let dataset = this.datasets[this.config.datasetID];

    if (dataset.variables[this.config.variableID].axes === null) {
      this.configService.searchVariable(this.config)
        .then(data => {
          dataset.variables[this.config.variableID].axes = data.map((axis: Axis) => {
            return {step: 1, ...axis}; 
          });

          this.setVariable();
        })
        .catch(error => {
          this.notificationService.error(error);
        });
    } else {
      this.setVariable();
    }
  }

  setVariable() {
    let dataset = this.datasets[this.config.datasetID];

    let variable = dataset.variables[this.config.variableID];

    this.config.variable = {...variable};

    this.config.variable.axes = this.config.variable.axes.map((axis: Axis) => {
      return {...axis};
    });
  }

  resetDomain() {
    if (this.config.variable.axes !== null) {
      this.config.variable.axes.forEach((axis: Axis) => {
        if (LAT_NAMES.indexOf(axis.id) >= 0 || LNG_NAMES.indexOf(axis.id) >= 0) {
          let dataset = this.datasets[this.config.datasetID];

          let filtered = dataset.variables[this.config.variableID].axes.filter((value: Axis) => {
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
