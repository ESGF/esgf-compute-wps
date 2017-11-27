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

  inputDatasets() {
    return this.process.inputs.filter((value: any) => {
      return !(value instanceof Process);
    });
  }
}

class Link {
  constructor(
    public src: ProcessWrapper,
    public dst?: ProcessWrapper
  ) { }
}

enum EditorState {
  None,
  Dropped,
  Connecting,
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

  nodes: ProcessWrapper[];
  links: Link[];
  rootNode: ProcessWrapper;
  selectedNode: ProcessWrapper;

  state: EditorState;
  stateData: any;

  svg: any;
  svgLinks: any;
  svgNodes: any;
  svgDrag: any;

  constructor(
    private configService: ConfigureService
  ) { 
    this.nodes = [];

    this.links = [];

    this.state = EditorState.None;
  }

  ngOnInit() {
    this.svg = d3.select('svg')
      .on('mouseover', () => this.svgMouseOver());

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

    this.svg.append('svg:defs')
      .append('svg:marker')
      .attr('id', 'drag-end-arrow')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 0)
      .attr('refY', 0)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('svg:path')
      .attr('d', 'M0,-5L10,0L0,5');

    let graph = this.svg.append('g')
      .classed('graph', true);

    this.svgDrag = graph.append('svg:path')
      .attr('class', 'link hidden')
      .attr('d', 'M0,0L0,0')
      .style('stroke', 'black')
      .style('stroke-width', '2px')
      .style('marker-end', 'url(#drag-end-arrow)');

    this.svgLinks = graph.append('g')
      .classed('links', true);

    this.svgNodes = graph.append('g')
      .classed('nodes', true);
  }

  determineRootNode() {
    if (this.nodes.length === 1) {
      this.rootNode = this.nodes[0];
    } else {
      let noInputs = this.nodes.filter((value: ProcessWrapper) => {
        return value.process.inputs.length === 0;
      });

      if (noInputs.length === 1) {
        this.rootNode = noInputs[0]; 
      } else {
        this.rootNode = undefined;
      }
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

    this.determineRootNode();

    this.update();
  }

  dropped(event: any) {
    this.state = EditorState.Dropped;

    this.stateData = event.dragData;
  }

  svgMouseOver() {
    if (this.state === EditorState.Dropped) {
      this.state = EditorState.None;

      let origin = d3.mouse(d3.event.target);

      let process = new Process(this.stateData);

      this.nodes.push(new ProcessWrapper(process, origin[0], origin[1]));

      this.determineRootNode();

      this.update();
    }
  }

  nodeClick() {
    jQuery('#configure').modal('show');

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

  nodeMouseEnter() {
    if (this.state === EditorState.Connecting) {
      this.stateData.dst = d3.select(d3.event.target).datum();
    }
  }

  nodeMouseLeave() {
    if (this.state === EditorState.Connecting) {
      this.stateData.dst = null;
    }
  }

  drag() {
    let node = d3.event.subject;

    if (this.state === EditorState.Connecting) {
      this.svgDrag
        .attr('d', `M${node.x},${node.y}L${d3.event.x},${d3.event.y}`);
    } else {
      node.x += d3.event.dx;

      node.y += d3.event.dy;

      this.update();
    }
  }

  dragStart() {
    if (d3.event.sourceEvent.shiftKey) {
      let node = d3.event.sourceEvent;
      let process = <ProcessWrapper>d3.select(node.target).datum();

      this.state = EditorState.Connecting;

      this.stateData = new Link(process);

      this.svgDrag
        .attr('d', `M${node.x},${node.y}L${node.x},${node.y}`)
        .classed('hidden', false);
    }
  }

  dragEnd() {
    if (this.state === EditorState.Connecting) {
      this.svgDrag.classed('hidden', true);

      this.state = EditorState.None;

      if (this.stateData !== null && this.stateData.dst !== null) {
        let exists = this.links.findIndex((link: Link) => {
          return link.src === this.stateData.src && link.dst === this.stateData.dst;
        });

        if (exists === -1) {
          let src = this.stateData.src,
            dst = this.stateData.dst;

          dst.process.inputs.push(src.process);

          this.links.push(this.stateData);

          this.update();
        }
      }

      this.stateData = null;
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
      .attr('transform', (d: any) => { return `translate(${d.x}, ${d.y})`; })
      .on('click', () => this.nodeClick())
      .on('mouseenter', () => this.nodeMouseEnter())
      .on('mouseleave', () => this.nodeMouseLeave())
      .call(d3.drag()
        .on('start', () => this.dragStart())
        .on('drag', () => this.drag())
        .on('end', () => this.dragEnd())
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
