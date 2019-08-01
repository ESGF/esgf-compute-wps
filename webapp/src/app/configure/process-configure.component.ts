import { Component, Input, Output, EventEmitter, ViewChild, AfterViewInit } from '@angular/core';

import { JoyrideService } from 'ngx-joyride';

import { Process } from './process';
import { ProcessWrapper } from './process-wrapper';
import { ConfigureService } from './configure.service';
import { Variable } from './variable';
import { Dataset } from './dataset';
import { NotificationService } from '../core/notification.service';
import { Domain } from './domain';
import { DomainComponent } from './domain.component';
import { NotificationComponent } from '../core/notification.component';
import { Axis } from './axis';

declare var $: any;

@Component({
  selector: 'process-configure',
  styles: [`
  .scrollable {
    max-height: 50vh;
    overflow-y: scroll;
  }

  .loader-background {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 9999;
    opacity: 0.5;
    border-radius: 6px;
    background-color: #000;
  }

  .loader {
    border: 12px solid #dddddd;
    border-top: 12px solid #3498db;
    border-radius: 50%;
    width: 120px;
    height: 120px;
    animation: spin 1s linear infinite;
    z-index: 10000;
    position: fixed;
    top: -100%;
    bottom: -100%;
    left: -100%;
    right: -100%;
    margin: auto;
  }

  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
  `],
  template: `
  <div id="processConfigureModal" class="modal fade" tabindex="-1" role="dialog">
    <div [class.loader]="loading"></div>
    <div class="modal-dialog modal-lg" role="document">
      <div class="modal-content">
        <div class="modal-header">
          <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
          <h4 class="modal-title">Configure "{{process?.identifier}}"</h4>
        </div>
        <div class="modal-body panel-group">
          <notification></notification>
          <panel title="Inputs" uid="inputPanel">
            <div class="container-fluid">
              <div class="row">
                <div class="col-md-2">
                  <div class="dropdown">
                    <button class="btn btn-default dropdown-toggle" type="button" id="datasetDropdown" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true">
                      Dataset ID
                      <span class="caret"></span>
                    </button>
                    <ul class="dropdown-menu" aria-labelledby="datasetDropdown">
                      <li *ngFor="let x of datasetID"><a (click)="selectDataset(x)">{{x}}</a></li>
                      <li><a (click)="newDataset = true">Add Dataset ID</a></li>
                    </ul>
                  </div>
                </div>
                <div class="col-md-10">
                  <div *ngIf="!newDataset; else customDataset" class="form-control-static">
                    {{processWrapper?.selectedDataset}}
                  </div>
                  <ng-template #customDataset>
                    <div class="row">
                      <div class="col-md-8">
                        <input type="text" #dataset class="form-control" placeholder="Dataset ID">
                      </div>
                      <div class="col-md-2">
                        <button (click)="addDataset(dataset.value)" type="button" class="btn btn-default">Add</button>
                      </div>
                    </div>
                  </ng-template>
                </div>
              </div>
              <br/>
              <div class="row">
                <div class="col-md-2">
                  <div class="dropdown">
                    <button class="btn btn-default dropdown-toggle" type="button" id="variableDropdown" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true">
                      Variable
                      <span class="caret"></span>
                    </button>
                    <ul class="dropdown-menu scrollable" aria-labelledby="varibaleDropdown">
                      <li *ngFor="let x of processWrapper?.dataset?.variableNames"><a (click)="processWrapper?.setSelectedVariable(x)">{{x}}</a></li>
                    </ul>
                  </div>
                </div>
                <div class="col-md-10">
                  <div class="form-control-static">
                    {{processWrapper?.selectedVariable}}
                  </div>
                </div>
              </div>
              <br/>
              <div *ngIf="processWrapper?.selectedVariable" class="row">
                <div class="col-md-12">
                  <panel title="Files" [listGroup]="true" [collapse]="false" [scrollable]="true">
                    <li class="list-group-item">
                      <div class="row">
                        <div class="col-md-10">
                          <input #filterText type="text" class="form-control">
                        </div>
                        <div class="col-md-2">
                          <button type="button" class="btn btn-default" (click)="addAllInputFiles()">Add All</button>
                        </div>
                      </div>
                    </li>
                    <li 
                      *ngFor="let x of processWrapper?.dataset?.getVariables(processWrapper?.selectedVariable) | filter:filterText.value"
                      class="list-group-item"
                      [class.list-group-item-success]="isInput(x)"
                      (click)="addInputFile(x)">
                      {{x.file | filename}}
                    </li>
                  </panel>
                </div>
              </div>
              <div class="row">
                <div class="col-md-12">
                  <panel 
                    title="Process Inputs"
                    [listGroup]="true"
                    [collapse]="false"
                    [scrollable]="true"
                    dnd-sortable-container
                    [sortableData]="process?.inputs">
                    <li class="list-group-item">
                      <div class="row">
                        <div class="col-md-10">
                        </div>
                        <div class="col-md-2">
                          <button type="button" class="btn btn-default" (click)="removeAllInputs()">Remove All</button>
                        </div>
                      </div>
                    </li>
                    <li *ngFor="let x of process?.inputs; let i = index" class="list-group-item" dnd-sortable [sortableIndex]="i">
                      <div class="row">
                        <div class="col-md-10">
                          {{x.display()}}
                        </div>
                        <div class="col-md-2">
                          <button type="button" class="close" (click)="removeInput(x)">
                            <span aria-hidden="true">&times;</span>
                          </button>
                        </div>
                      </div>
                    </li>
                  </panel>
                </div>
              </div>
            </div>
          </panel>
          <panel title="Regrid" uid="regridPanel">
            <regrid-config [model]=process?.regrid></regrid-config>
          </panel>
          <panel title="Parameters" [listGroup]="true" uid="parameterPanel">
            <parameter-config 
              [process]="process">
              </parameter-config>
          </panel>
          <panel title="Domain" [listGroup]="true" uid="domainPanel">
            <domain-config 
              [domain]="processWrapper?.process?.domain"
              [candidateDomain]="processWrapper?.domain">
            </domain-config>
          </panel>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-danger pull-left" (click)="removeProcess()">Remove</button>
          <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
        </div>
        <div [class.loader-background]="loading"></div>
      </div>
    </div>
  </div>
  `
})
export class ProcessConfigureComponent implements AfterViewInit {
  @Input() datasetID: string[];
  @Input() processWrapper: ProcessWrapper;
  @Input() params: any;

  @Output() inputRemoved = new EventEmitter<Variable|Process>();
  @Output() removed = new EventEmitter<Process>();

  @ViewChild(DomainComponent)
  private domainComponent: DomainComponent;

  @ViewChild(NotificationComponent)
  private notificationComponent: NotificationComponent;

  private newDataset = false;

  private loading = false;

  private datasetIDRegex = /^.*\|.*$/g;

  constructor(
    private configureService: ConfigureService,
    private notificationService: NotificationService,
    private joyrideService: JoyrideService,
  ) { }

  ngAfterViewInit() {
    $('#processConfigureModal').on('show.bs.modal', (e: any) => {
      this.reset();

      this.notificationComponent.subscribe();
    });

    $('#processConfigureModal').on('hidden.bs.modal', (e: any) => {
      this.notificationComponent.unsubscribe();

      this.processWrapper.errors = this.domainComponent.errors();
    });
  }

  addDataset(value: string) {
    if (value === '') {
      this.notificationService.error('Please enter a Dataset ID e.g. CMIP6.CFMIP.NCAR.CESM2.amip-4xCO2.r1i1p1f1.Amon.tas.gn.v20190408|esgf-data.ucar.edu, this can be found through a CoG search.');
    } else {
      let exists = this.datasetID.findIndex((item: string) => {
        return item === value;
      });

      if (exists != -1) {
        let matchDatasetID = this.datasetIDRegex.test(value);

        if (matchDatasetID) {
          this.selectDataset(value); 
        } else {
          this.notificationService.error(`"${value}" did not match the expected format: <Master ID>|<Data node>. You can find Dataset IDs by search CoG.`);
        }
      } else {
        this.selectDataset(value);
      }
    }
  }

  reset() {
  }

  get process() {
    return (this.processWrapper == null) ? null : this.processWrapper.process;
  }

  removeProcess() {
    this.removed.emit(this.process);

    $('#processConfigureModal').modal('hide');
  }

  selectDataset(dataset: string) {
    if (!this.loading) {
      this.loading = true;
    }

    this.newDataset = false;

    this.configureService.searchESGF(dataset, this.params)
      .then((data: Dataset) => {
        this.processWrapper.selectedDataset = dataset;

        this.datasetID.push(dataset);

        this.processWrapper.dataset = data

        this.loading = false;
      })
      .catch((error: string) => {
        this.notificationService.error(error)

        this.loading = false;
      });
  }

  getIndex(variable: Variable) {
    return this.process.inputs.findIndex((item: Variable|Process) => {
      if (item instanceof Variable) {
        return (<Variable>item).file === variable.file;
      }

      return false;
    });
  }

  isInput(variable: Variable) {
    return this.getIndex(variable) != -1;
  }

  getFileDomain(variable: Variable, disableLoading=false) {
    if (variable.domain != null) {
      return;
    }

    if (!this.loading && !disableLoading) {
      this.loading = true;
    }

    return new Promise((resolve, reject) => {
      this.configureService.searchVariable(
        this.processWrapper.selectedVariable, 
        this.processWrapper.selectedDataset, 
        [variable.index], 
        this.params)
        .then((data: Domain[]) => {
          variable.domain = data[0].clone();

          if (this.processWrapper.domain == null) {
            this.processWrapper.domain = data[0].clone();
          }

          if (!disableLoading) {
            this.loading = false;
          }

          resolve();
        })
        .catch((error: string) => {
          this.process.removeInput(variable);

          this.notificationService.error(`Removed ${variable.display()}, failed to retrieve metadata: ${error}`);

          if (!disableLoading) {
            this.loading = false;
          }

          reject();
        });
    });
  }

  removeInput(item: Variable|Process) {
    if (item instanceof Variable) {
      this.process.inputs = this.process.inputs.filter((x: Variable) => {
        if ((<Variable>item).name == x.name && (<Variable>item).file == x.file) {
          return false;
        }

        return true;
      });

      this.updateDomain();
    } else if (item instanceof Process) {
      this.inputRemoved.emit(item);

      this.process.removeInput(item);
    }
  }

  removeAllInputs() {
    this.process.inputs.forEach((item: Variable|Process) => {
      if (item instanceof Process) {
        this.inputRemoved.emit(item);
      }
    });

    this.process.clearInputs();
  }

  updateDomain() {
    let process = this.processWrapper.process;

    if (process.inputs.length === 0) {
      return;
    }

    let temporal = process.inputs
      .filter((item: Variable|Process) => {
        return item instanceof Variable ? true : false;
      })
      .map((item: Variable) => {
        let temporal = item.domain.temporal;

        return {
          units: temporal.units,
          start: temporal.start,
          stop: temporal.stop,
          length: temporal.length,
        };
      });

    let length = temporal
      .map((x: any) => x.length)
      .reduce((x: number, y: number) => x + y);

    this.configureService
      .combineTemporal(temporal)
      .then((item: any) => {
        if (process.domain.temporal != null) {
          process.domain.temporal.updateValues(item);

          process.domain.temporal.updateLength(length);
        }

        if (this.processWrapper.domain.temporal != null) {
          this.processWrapper.domain.temporal.updateValues(item);

          this.processWrapper.domain.temporal.updateLength(length);
        }
      })
      .catch((error: string) => {
        this.notificationService.error(`Failed to combine the temporal axes: ${error}`);
      });

  }

  addInputFiles(variables: Variable[]) {
    if (!this.loading) {
      this.loading = true;
    }

    let allowed = this.process.description.metadata.inputs;

    if ((this.process.inputs.length + variables.length) > allowed) {
      this.notificationService.error(`Cannot add input, exceeding maximum allowed (${allowed}).`);
    }

    let items = [];

    for (let variable of variables) {
      items.push(this.getFileDomain(variable, true));
    }

    Promise.all(items)
      .then((res: any) => {
        this.process.inputs = this.process.inputs.concat(variables);

        this.updateDomain();

        this.loading = false;
      })
      .catch((err: any) => {
        console.info(err);

        this.loading = false;
      });
  }

  addInputFile(variable: Variable) {
    let allowed = this.process.description.metadata.inputs;

    if ((this.process.inputs.length + 1) > allowed) {
      this.notificationService.error(`Cannot add input, exceeding maximum allowed (${allowed}).`);

      return;
    }

    let match = this.process.inputs.findIndex((item: Variable|Process) => {
      if (item instanceof Variable) {
        return (<Variable>item).file === variable.file;
      }

      return false;
    });

    if (match == -1) {
      this.process.inputs.push(variable);

      if (variable.domain == null) {
        this.getFileDomain(variable)
          .then(() => {
            this.updateDomain();
          });
      } else {
        this.updateDomain();
      }
    }
  }

  addAllInputFiles() {
    let variables = this.processWrapper.dataset.getVariables(this.processWrapper.selectedVariable);

    this.addInputFiles(variables);
  }
}
