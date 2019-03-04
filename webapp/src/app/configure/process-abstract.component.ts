import { Component, Input, Output, EventEmitter, ViewChild, AfterViewInit } from '@angular/core';

import { Process } from './process';

@Component({
  selector: 'process-abstract',
  styles: [`
  .scrollable {
    max-height: 50vh;
    overflow-y: scroll;
  }
  `],
  template: `
  <div id="processAbstractModal" class="modal fade" tabindex="-1" role="dialog">
    <div class="modal-dialog modal-lg" role="document">
      <div class="modal-content">
        <div class="modal-header">
          <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
          <h4 class="modal-title">{{process?.identifier}} abstract</h4>
        </div>
        <div class="modal-body panel-group">
          <ng-container *ngIf="process != null && process.description != null; else notAvailable">
            {{process?.description?.abstract}}          
          </ng-container>
          <ng-template #notAvailable>
            <h4>Abstract not available</h4>
          </ng-template>
        </div>
      </div>
    </div>
  </div>
  `,
})
export class ProcessAbstractComponent {
  @Input() process: Process;
}
