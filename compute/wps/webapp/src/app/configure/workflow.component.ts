import { Component, Input, OnInit } from '@angular/core';

import { 
  ConfigureService, 
  Configuration,
  Process, 
  Variable, 
  Dataset, 
  DatasetCollection 
} from './configure.service';

import * as d3 from 'd3';

declare var jQuery:any;

class WorkflowModel { 
  selectedVariable: Variable;

  selectedDataset: DatasetWrapper;
  availableDatasets: DatasetWrapper[] = [];
}

interface Displayable { 
  display(): string;
  uid(): string;
}

class DatasetWrapper implements Displayable {
  constructor(
    public dataset: Dataset,
  ) { }

  display() {
    return this.dataset.id;
  }

  uid() {
    return this.display();
  }
}

class ProcessWrapper implements Displayable {
  constructor(
    public process: Process,
    public x: number,
    public y: number
  ) { }

  display() {
    return this.process.identifier;
  }

  uid() {
    return this.process.uid;
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
  @Input() datasets: string[];
  @Input() config: Configuration;

  model: WorkflowModel = new WorkflowModel();

  drag: boolean;
  dragValue: string;

  nodes: ProcessWrapper[];
  links: Link[];
  selectedNode: ProcessWrapper;

  svg: any;
  svgLinks: any;
  svgNodes: any;

  constructor(
    private configService: ConfigureService
  ) { }

  ngOnInit() {
    this.nodes = [];

    this.links = [];

    this.svg = d3.select('svg')
      .on('mouseover', () => this.processMouseOver());

    this.svg.append('svg:defs')
      .append('svg:marker')
      .attr('id', 'end-arrow')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 60)
      .attr('refY', 0)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M0,-5L10,0L0,5');

    let graph = this.svg.append('g')
      .classed('graph', true);

    this.svgLinks = graph.append('g')
      .classed('links', true);

    this.svgNodes = graph.append('g')
      .classed('nodes', true);
  }

  processMouseOver() {
    if (this.drag) {
      let origin = d3.mouse(d3.event.target);

      let process = new Process(this.dragValue);

      this.nodes.push(new ProcessWrapper(process, origin[0], origin[1]));

      this.update();

      this.drag = false;
    }
  }

  addInput(value: Variable) {
    this.selectedNode.process.inputs.push(value);
  }

  removeInput(value: Variable) {
    this.selectedNode.process.inputs = this.selectedNode.process.inputs.filter((data: Variable) => {
      return value.id !== data.id;
    });
  }

  removeNode(node: ProcessWrapper) {
    jQuery('#configure').modal('hide');

    this.nodes = this.nodes.filter((value: ProcessWrapper) => { 
      return node.uid() !== value.uid();
    });

    this.update();
  }

  dropped(event: any) {
    this.drag = true;
    this.dragValue = event.dragData;
  }

  clickNode() {
    this.selectedNode = <ProcessWrapper>d3.select(d3.event.target).datum();

    this.model.availableDatasets = [];

    let datasets = this.datasets.map((value: string) => { 
      let dataset = new Dataset(value);

      return new DatasetWrapper(dataset); 
    });
    
    this.model.availableDatasets = this.model.availableDatasets.concat(datasets);

    if (this.model.availableDatasets.length > 0) {
      this.model.selectedDataset = this.model.availableDatasets[0];

      this.config.datasetID = this.model.selectedDataset.dataset.id;

      this.configService.searchESGF(this.config)
        .then(data => {
          data.forEach((value: Variable) => {
            value.dataset = this.config.datasetID;
          });

          this.model.selectedDataset.dataset.variables = data;

          if (data.length > 0) {
            this.model.selectedVariable = data[0];
          }
        });
    } else {
      // needs to be undefined to selected the default option
      this.model.selectedDataset = undefined;
    }
  }

  update() {
    let links = this.svgLinks
      .selectAll('path')
      .attr('d', (d: any) => {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      })
      .data(this.links);

    links.exit().remove();

    links.enter()
      .append('path')
      .style('stroke', 'black')
      .style('stroke-width', '2px')
      .attr('marker-end', 'url(#end-arrow)')
      .attr('d', (d: any) => {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      });

    let nodes = this.svgNodes
      .selectAll('g')
      .attr('transform', (d: any) => { return `translate(${d.x}, ${d.y})`; })
      .data(this.nodes);

    nodes.exit().remove();

    let newNodes = nodes.enter()
      .append('g')
      .attr('data-toggle', 'modal')
      .attr('data-target', '#configure')
      .attr('transform', (d: any) => { return `translate(${d.x}, ${d.y})`; })
      .on('click', () => this.clickNode())
      .call(d3.drag()
        .on('drag', () => {
          let node = d3.event.subject;

          node.x += d3.event.dx;

          node.y += d3.event.dy;

          this.update();
        })
      );

    newNodes.append('circle')
      .attr('r', '60')
      .style('stroke', 'white')
      .style('stroke-width', '2px')
      .style('fill', '#ddd');

    newNodes.append('text')
      .attr('text-anchor', 'middle')
      .text((d: any) => { return d.display(); });
  }
}
