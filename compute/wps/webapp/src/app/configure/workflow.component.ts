import { Component, Input, OnInit } from '@angular/core';

import { DatasetCollection, Dataset } from './configure.service';

import * as d3 from 'd3';

class WorkflowModel { 
  selectedInput: Displayable;
  availableInputs: Displayable[] = [];
}

interface Displayable { 
  display(): string;
}

class DatasetWrapper implements Displayable {
  constructor(
    public dataset: string
  ) { }

  display() {
    return this.dataset;
  }
}

class Process implements Displayable {
  inputs: Displayable[];

  constructor(
    public id: string,
    public identifier: string,
    public x: number,
    public y: number
  ) { 
    this.inputs = [];
  }

  get uid() {
    return this.identifier + '-' + this.id;
  }

  display() {
    return this.uid;
  }
}

class Link {
  constructor(
    public src: Process,
    public dst: Process
  ) { }
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
  links: Link[];
  selectedNode: Process;

  svg: any;
  svgLinks: any;
  svgNodes: any;

  ngOnInit() {
    this.nodes = [];

    this.links = [];

    this.svg = d3.select('svg')
      .on('mouseover', () => this.processMouseOver());

    this.svgLinks = this.svg.append('g');

    this.svgNodes = this.svg.selectAll('.node');
  }

  processMouseOver() {
    if (this.drag) {
      let origin = d3.mouse(d3.event.target);

      this.nodes.push(new Process(
        Math.random().toString(16).slice(2),
        this.dragValue,
        origin[0],
        origin[1]
      ));

      this.update();

      this.drag = false;
    }
  }

  addInput() {
    this.links.push(new Link(<Process>this.model.selectedInput, this.selectedNode));

    this.selectedNode.inputs.push(this.model.selectedInput);

    this.update();
  }

  removeInput(value: Process) {
    let index = this.selectedNode.inputs.indexOf(value);

    this.selectedNode.inputs.splice(index, 1);
  }

  dropped(event: any) {
    this.drag = true;
    this.dragValue = event.dragData;
  }

  update() {
    d3.select('svg')
      .selectAll('.link')
      .data(this.links)
      .enter()
      .append('path')
      .classed('link', true)
      .style('stroke', '#333')
      .style('stroke-width', '4px')
      .attr('d', function(d) {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      });

    let newNodes = this.svgNodes
      .selectAll('g')
      .data(this.nodes)
      .enter()
      .append('g')
      .on('click', () => this.clickNode())
      .call(d3.drag()
        .on('drag', function() {
          d3.select(this)
            .attr('transform', `translate(${d3.event.x}, ${d3.event.y})`);
        })
      )
      .classed('node', true)
      .attr('data-toggle', 'modal')
      .attr('data-target', '#configure');

    newNodes.classed('node', true)
      .attr('transform', function(d: any) { return 'translate(' + d.x + ',' + d.y + ')'; });

    newNodes.append('circle')
      .attr('r', '60')
      .attr('stroke', '#333')
      .attr('stroke-width', '1px')
      .attr('fill', 'none');

    newNodes.append('text')
      .attr('text-anchor', 'middle')
      .text(function(d: any) { return d.identifier; });
  }

  clickNode() {
    this.selectedNode = <Process>d3.select(d3.event.target).datum();

    this.model.availableInputs = this.nodes.filter((value: Process) => {
      return this.selectedNode !== value;
    });

    let datasets = Object.keys(this.datasets).map((value: string) => {
      return new DatasetWrapper(value);
    });

    this.model.availableInputs = this.model.availableInputs.concat(datasets);

    if (this.model.availableInputs.length > 0) {
      this.model.selectedInput = this.model.availableInputs[0];
    }
  }
}
