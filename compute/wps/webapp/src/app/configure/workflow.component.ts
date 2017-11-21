import { Component, Input, OnInit } from '@angular/core';

import { DatasetCollection, Dataset } from './configure.service';

import * as d3 from 'd3';

class WorkflowModel { 
  selectedInput: Process;
  availableInputs: Process[] = [];
}

class Process {
  inputs: Process[];

  constructor(
    public id: string,
    public identifier: string,
  ) { 
    this.inputs = [];
  }

  get uid() {
    return this.identifier + '-' + this.id;
  }
}

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

  svg {
    border: 1px solid #ddd;
  }
  `],
  templateUrl: './workflow.component.html'
})
export class WorkflowComponent implements OnInit{
  @Input() processes: string[];
  @Input() datasets: DatasetCollection;

  model: WorkflowModel = new WorkflowModel();

  drag: boolean;
  dragValue: string;

  nodes: Process[];
  selectedNode: Process;

  ngOnInit() {
    this.nodes = [];

    d3.select('svg')
      .on('mouseover', () => {
        if (this.drag) {
          let origin = d3.mouse(d3.event.target);

          this.nodes.push(new Process(Math.random().toString(16).slice(2), this.dragValue));

          let g = d3.select('svg')
            .selectAll('g')
            .data(this.nodes)
            .enter()
              .append('g')
              .attr('transform', `translate(${origin[0]}, ${origin[1]})`)
              .attr('data-toggle', 'modal')
              .attr('data-target', '#configure');

          g.append('circle')
            .attr('r', 60)
            .attr('stroke', 'black')
            .attr('fill', 'white');

          g.append('text')
            .attr('text-anchor', 'middle')
            .text((d) => { return d.identifier; });

          d3.select('svg')
            .selectAll('g')
            .on('click', () => {
              this.selectedNode = <Process>d3.select(d3.event.target).datum();

              this.model.availableInputs = this.nodes.filter((value: Process) => {
                return this.selectedNode !== value;
              });

              let datasets = Object.keys(this.datasets).map((value: string) => {
                return new Process(value, value);
              });

              this.model.availableInputs = this.model.availableInputs.concat(datasets);

              if (this.model.availableInputs.length > 0) {
                this.model.selectedInput = this.model.availableInputs[0];
              }
            })
            .call(d3.drag()
              .on('drag', function() {
                d3.select(this)
                  .attr('transform', `translate(${d3.event.x}, ${d3.event.y})`);
              })
            );

          this.drag = false;
        }
      });
  }

  addInput() {
    this.selectedNode.inputs.push(this.model.selectedInput);
  }

  removeInput(value: Process) {
    let index = this.selectedNode.inputs.indexOf(value);

    this.selectedNode.inputs.splice(index, 1);
  }

  dropped(event: any) {
    this.drag = true;
    this.dragValue = event.dragData;
  }
}
