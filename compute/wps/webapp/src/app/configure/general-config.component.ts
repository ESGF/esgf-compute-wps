import { Component, EventEmitter, Input, Output, OnInit } from '@angular/core';

import { Axis } from './axis.component';
import { LAT_NAMES, LNG_NAMES, Configuration, Dataset, Variable, DatasetCollection, VariableCollection, ConfigureService } from './configure.service';
import { NotificationService } from '../core/notification.service';
import { ConfigService } from '../core/config.service';

declare var $: any;

@Component({
  selector: 'general-config',
  template: `
  <div>
    <div class="form-group">
      <label for="datasetID">Dataset</label>
      <select [(ngModel)]="config.dataset" (change)="loadDataset()" class="form-control" id="datasetID" name="datasetID">
        <option *ngFor="let d of datasets" [ngValue]="d">{{d.id}}</option>
      </select>
    </div>
    <div class="form-group">
      <label for="variable">Variable</label>
      <select [(ngModel)]="config.variable" (change)="loadVariable()" class="form-control" id="variable" name="variable">
        <option *ngFor="let v of variables" [ngValue]="v">{{v.id}}</option>
      </select>
    </div>
    <div class="form-group">
      <label for="process">Process</label>
      <div class="input-group">
        <select [(ngModel)]="config.process.identifier" class="form-control" id="process" name="process">
          <option *ngFor="let proc of processes">{{proc.identifier}}</option>
        </select>
        <span class="input-group-btn">
          <button (click)="showAbstract()" type="button" class="btn btn-default" data-toggle="modal" data-target="#abstractModal">Abstract</button>
        </span>
      </div>
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
  @Input() processes: any[];
  @Input() datasetIDs: string[];

  variables: Variable[];
  datasets: Dataset[];

  constructor(
    private configureService: ConfigureService,
    private configService: ConfigService,
    private notificationService: NotificationService,
  ) { }

  ngOnInit() {
    this.datasets = [];

    this.datasetIDs.forEach((id: string) => {
      this.datasets.push({id: id, variables: []});
    });

    if (this.datasets.length > 0) {
      this.config.dataset = this.datasets[0];

      this.loadDataset();
    }
  }

  showAbstract() {
    let proc = this.processes.filter((item: any) => {
      return this.config.process.identifier === item.identifier;
    });

    if (proc.length > 0) {
      // Really ugly way to parse XML
      // TODO replace with better parsing
      let parser = new DOMParser();

      let xmlDoc = parser.parseFromString(proc[0].description, 'text/xml');

      let description = xmlDoc.children[0].children[0];

      let abstractText = '';
      let titleText = '';

      Array.from(description.children).forEach((item: any) => {
        if (item.localName === 'Identifier') {
          titleText = item.innerHTML;
        } else if (item.localName === 'Abstract') {
          abstractText = item.innerHTML;
        }
      });

      let modal = $('#abstractModal');

      modal.find('.modal-title').html(`"${titleText}" Abstract`);

      if (abstractText === '') { abstractText = 'No abstract available'; }

      modal.find('#abstract').html(abstractText);
    }
  }
  
  loadDataset() {
    this.configureService.searchESGF(this.config)
      .then(data => {
        this.variables = this.config.dataset.variables = data;

        this.config.variable = this.variables[0];

        this.loadVariable();
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  loadVariable() {
    if (this.config.variable.axes === null) {
      this.configureService.searchVariable(this.config)
        .then(data => {
          // cached copy of the axes
          this.config.variable.axes = data.map((axis: Axis) => {
            return {step: 1, crs: 'Values', ...axis}; 
          });

          // create a copy for editing
          this.config.process.domain = data.map((axis: Axis) => {
            return {step: 1, crs: 'Values', ...axis};
          });
        })
        .catch(error => {
          this.notificationService.error(error); 
        });
    } else {
      // create a copy for editing
      this.config.process.domain = this.config.process.domain.map((axis: Axis) => {
        return {step: 1, crs: 'Values', ...axis};
      });
    }
  }

  resetDomain() {
    this.config.process.domain.forEach((axis: Axis) => {
      if (LAT_NAMES.indexOf(axis.id) >= 0 || LNG_NAMES.indexOf(axis.id) >= 0) {
        let filtered = this.config.variable.axes.filter((value: Axis) => {
          return axis.id === value.id;
        });

        if (filtered.length > 0) {
          axis.start = filtered[0].start;

          axis.stop = filtered[0].stop;
        }
      }
    });
  }

  onDownload() {
    this.config.process.setInputs([this.config.variable]);

    this.configureService.downloadScript(this.config.process)
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
    this.config.process.setInputs([this.config.variable]);

    this.configureService.execute(this.config.process)
      .then((data: any) => {
        let parser = new DOMParser();
        let xml = parser.parseFromString(data, 'text/xml');
        let el = xml.getElementsByTagName('wps:ExecuteResponse');
        let link = '';

        if (el.length > 0) {
          let statusLocation = el[0].attributes.getNamedItem('statusLocation').value;

          let jobID = statusLocation.substring(statusLocation.lastIndexOf('/')+1);

          link = this.configService.userJobPath;
        }
        
        this.notificationService.message('Succesfully submitted job', link);
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }
}
