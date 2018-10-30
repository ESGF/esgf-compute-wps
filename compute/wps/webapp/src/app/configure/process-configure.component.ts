import { Component, Input, Output, EventEmitter } from '@angular/core';

import { Process } from './process';
import { ConfigureService } from './configure.service';
import { Variable } from './variable';
import { Dataset } from './dataset';

@Component({
  selector: 'process-configure',
  styles: [`
  .scrollable {
    max-height: 50vh;
    overflow-y: scroll;
  }
  `],
  template: `
  <div id="processConfigureModal" class="modal fade" tabindex="-1" role="dialog">
    <div class="modal-dialog modal-lg" role="document">
      <div class="modal-content">
        <div class="modal-header">
          <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
          <h4 class="modal-title">Configure "{{process?.identifier}}"</h4>
        </div>
        <div class="modal-body">
          <panel title="Inputs">
            <div class="container-fluid">
              <div class="row">
                <div class="col-md-2">
                  <div class="dropdown">
                    <button class="btn btn-default dropdown-toggle" type="button" id="datasetDropdown" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true">
                      Dataset
                      <span class="caret"></span>
                    </button>
                    <ul class="dropdown-menu" aria-labelledby="datasetDropdown">
                      <li *ngFor="let x of datasetID"><a (click)="selectDataset(x)">{{x}}</a></li>
                    </ul>
                  </div>
                </div>
                <div class="col-md-10">
                  {{selectedDataset}}
                </div>
              </div>
              <div class="row">
                <div class="col-md-2">
                  <div class="dropdown">
                    <button class="btn btn-default dropdown-toggle" type="button" id="variableDropdown" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true">
                      Variable
                      <span class="caret"></span>
                    </button>
                    <ul class="dropdown-menu scrollable" aria-labelledby="varibaleDropdown">
                      <li *ngFor="let x of dataset?.variables"><a (click)="selectedVariable=x">{{x.name}}</a></li>
                    </ul>
                  </div>
                </div>
                <div class="col-md-10">
                  {{selectedVariable?.name}}
                </div>
              </div>
              <div class="row">
                <div class="col-md-12">
                  <div class="container-fluid">
                    <div *ngFor="let x of selectedVariable?.files" class="row">
                      <div class="col-md-10"><a (click)="onAddFile(selectedVariable, x)">{{x | filename}}</a></div> 
                    </div>
                  </div>
                </div>
              </div>
              <div class="row">
                <div class="col-md-12">
                  <div class="container-fluid">
                    <div *ngFor="let x of process?.inputs" class="row">
                      <div class="col-md-10"><a>{{x.display()}}</a></div> 
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </panel>
          <panel title="Regrid">
            <regrid-config [model]=process?.regrid></regrid-config>
          </panel>
          <panel title="Parameters" [listGroup]="true">
            <parameter-config [params]=process?.parameters></parameter-config>
          </panel>
          <panel title="Domain">
          </panel>
        </div>
      </div>
    </div>
  </div>
  `
})
export class ProcessConfigureComponent {
  @Input() datasetID: string[];
  @Input() process: Process;
  @Input() params: any;

  selectedDataset: string;
  selectedVariable: Variable;

  dataset: Dataset;

  constructor(
    private configureService: ConfigureService,
  ) {
  }

  selectDataset(dataset: string) {
    this.selectedDataset = dataset;

    this.configureService.searchESGF(dataset, this.params)
      .then((data: Dataset) => this.dataset = data);
  }

  onAddFile(variable: Variable, file: string) {
    this.process.inputs.push(new Variable(variable.name, [file]));
  }
}
