import { Component, Input, Output, EventEmitter, ViewChild, AfterViewInit } from '@angular/core';

import { Process } from './process';
import { ProcessWrapper } from './process-wrapper';
import { ConfigureService } from './configure.service';
import { Variable } from './variable';
import { Dataset } from './dataset';
import { NotificationService } from '../core/notification.service';
import { Domain } from './domain';
import { DomainComponent } from './domain.component';

declare var $: any;

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
        <div class="modal-body panel-group">
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
                  <div class="form-control-static">
                    {{processWrapper?.selectedDataset}}
                  </div>
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
          <panel title="Regrid">
            <regrid-config [model]=process?.regrid></regrid-config>
          </panel>
          <panel title="Parameters" [listGroup]="true">
            <parameter-config [params]=process?.parameters></parameter-config>
          </panel>
          <panel title="Domain" [listGroup]="true">
            <domain-config [candidateDomain]="processWrapper?.domain"></domain-config>
          </panel>
        </div>
      </div>
    </div>
  </div>
  `
})
export class ProcessConfigureComponent implements AfterViewInit {
  @Input() datasetID: string[];
  @Input() processWrapper: ProcessWrapper;
  @Input() params: any;

  @Output() removeProcessInput = new EventEmitter<Process>();

  @ViewChild(DomainComponent)
  private domainComponent: DomainComponent;

  constructor(
    private configureService: ConfigureService,
    private notificationService: NotificationService,
  ) { }

  ngAfterViewInit() {
    $('#processConfigureModal').on('hidden.bs.modal', (e: any) => {
      this.processWrapper.errors = this.domainComponent.errors();
    });
  }

  get process() {
    return (this.processWrapper == null) ? null : this.processWrapper.process;
  }

  selectDataset(dataset: string) {
    this.processWrapper.selectedDataset = dataset;

    this.configureService.searchESGF(dataset, this.params)
      .then((data: Dataset) => this.processWrapper.dataset = data);
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

  getFileDomain(variable: Variable) {
    if (variable.domain != null) {
      return;
    }

    return this.configureService.searchVariable(
      this.processWrapper.selectedVariable, 
      this.processWrapper.selectedDataset, 
      [variable.index], 
      this.params)
      .then((data: Domain[]) => {
        if (this.processWrapper.domain == null) {
          this.processWrapper.domain = Object.create(Domain.prototype);

          Object.assign(this.processWrapper.domain, data[0]);
        }

        variable.domain = data[0];
      })
      .catch((text: string) => this.notificationService.error(text));
  }

  removeInput(item: Variable|Process) {
    if (item instanceof Variable) {
      this.process.inputs = this.process.inputs.filter((x: Variable) => {
        if ((<Variable>item).name == x.name && (<Variable>item).file == x.file) {
          return false;
        }

        return true;
      });
    } else if (item instanceof Process) {
      this.removeProcessInput.emit(item);
    }
  }

  removeAllInputs() {
    this.process.inputs.forEach((item: Variable|Process) => {
      if (item instanceof Process) {
        this.removeProcessInput.emit(item);
      }
    });

    this.process.clearInputs();
  }

  addAllInputFiles() {
    this.process.inputs.concat(this.processWrapper.dataset.getVariables(this.processWrapper.selectedVariable));
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

    if (match != -1) {
      this.process.inputs.splice(match, 1);
    } else {
      this.getFileDomain(variable).then(() => {
        this.process.inputs.push(variable);
      });
    }
  }
}
