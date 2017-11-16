import { Component, Input, OnInit } from '@angular/core';

import * as d3 from 'd3';

@Component({
  selector: 'workflow',
  styles: [`
  .pane {
    padding: 1em;
  }

  .graph-container {
    width: 100%;
    min-height: calc(100vh - 100px);
  }

  .fill {
    min-height: calc(100vh - 100px);
  }
  `],
  templateUrl: './workflow.component.html'
})
export class WorkflowComponent implements OnInit{
  @Input() processes: string[];

  svg: d3.Selection<any, any, any, any>;

  ngOnInit() {
    this.svg = d3.select('svg')
      .append('g')
        .attr('class', 'nodes');
  }

  dropped(data: any) {
    let color = d3.scaleOrdinal(d3.schemeCategory20);
  }
}
