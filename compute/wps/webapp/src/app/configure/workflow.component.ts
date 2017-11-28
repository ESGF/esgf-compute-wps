import { Component, Input, OnInit, ViewEncapsulation } from '@angular/core';

import { Parameter } from './parameter.component';
import { 
  ConfigureService, 
  Configuration,
  Process, 
  Variable, 
  Dataset, 
  DatasetCollection 
} from './configure.service';

import * as d3 from 'd3';

declare var jQuery: any;

class WorkflowModel { 
  process: Process;
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
  encapsulation: ViewEncapsulation.None,
  styles: [`
  svg {
    border: 1px solid #ddd;
  }

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

  .node {
    fill: #ddd;
    stroke: white;
    stroke-width: 2px;
  }

  .node-select {
    fill: #abd8ff;
  }

  .node-connect {
    fill: #ccc!important;
  }

  .link {
    stroke: black;
    stroke-width: 3px;
    marker-end: url(#end-arrow);
  }

  .link-select {
    stroke: #abd8ff!important;
  }

  .link-drag {
    stroke: black;
    stroke-width: 3px;
    marker-end: url(#drag-end-arrow);
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
    this.model.process = new Process();

    this.nodes = [];

    this.links = [];

    this.state = EditorState.None;
  }

  ngOnInit() {
    d3.select(window)
      .on('keydown', () => this.removeElements())

    this.svg = d3.select('svg')
      .on('mouseover', () => this.svgMouseOver());

    this.svg.append('svg:defs')
      .append('svg:marker')
      .attr('id', 'end-arrow')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 42)
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
      .attr('class', 'link-drag hidden')
      .attr('d', 'M0,0L0,0');

    this.svgLinks = graph.append('g')
      .classed('links', true);

    this.svgNodes = graph.append('g')
      .classed('nodes', true);
  }

  showHelp() {
    jQuery('#help').modal('show');
  }

  determineRootNode() {
    if (this.nodes.length === 1) {
      this.rootNode = this.nodes[0];
    } else {
      let notSrc = this.nodes.filter((value: ProcessWrapper) => {
        let check = this.links.some((link: Link) => {
          return link.src === value;
        });

        return !check;
      });

      if (notSrc.length === 1) {
        this.rootNode = notSrc[0];
      } else {
        this.rootNode = null;
      }
    }
  }

  removeElements() {
    switch (d3.event.keyCode) {
      case 8:
      case 46: {
        d3.select('.link-select')
          .each((link: Link) => {
            this.links = this.links.filter((item: Link) => {
              if (link !== item) {
                return true;
              }

              let src = item.src.process;
              let dst = item.dst.process;

              dst.inputs = dst.inputs.filter((proc: Process) => {
                return src.uid !== proc.uid;
              });

              return false;
            });
          });

        this.determineRootNode();

        this.update();
      }
      break;
    }
  }

  addParameterWorkflow() {
    this.model.process.parameters.push({key: '', value: ''} as Parameter);
  }

  removeParameterWorkflow(param: Parameter) {
    let newParams = this.model.process.parameters.filter((value: Parameter) => {
      return param.key !== value.key || param.value !== value.value;
    });

    this.model.process.parameters = newParams;
  }

  addParameter() {
    this.selectedNode.process.parameters.push({key: '', value: ''} as Parameter); 
  }

  removeParameter(param: Parameter) {
    let newParams = this.selectedNode.process.parameters.filter((value: Parameter) => {
      return param.key !== value.key || param.value !== value.value;
    });

    this.selectedNode.process.parameters = newParams;
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

      d3.select(d3.event.target)
        .select('circle')
        .classed('node-connect', true);
    }
  }

  nodeMouseLeave() {
    if (this.state === EditorState.Connecting) {
      this.stateData.dst = null;

      d3.select('.node-connect')
        .classed('node-connect', false);
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
      d3.select('.node-connect')
        .classed('node-connect', false);

      this.svgDrag.classed('hidden', true);

      if (this.stateData !== null && this.stateData.dst !== null) {
        let exists = this.links.findIndex((link: Link) => {
          return link.src === this.stateData.src && link.dst === this.stateData.dst;
        });

        if (exists === -1) {
          let src = this.stateData.src,
            dst = this.stateData.dst;

          dst.process.inputs.push(src.process);

          this.links.push(this.stateData);

          this.determineRootNode();

          this.update();
        }
      }

      this.state = EditorState.None;

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
      .classed('link', true)
      .attr('d', (d: any) => {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      })
      .on('click', (data: any, index: any, group: any) => {
        d3.select(group[index])
          .classed('link-select', true);
      });;

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
      .classed('node', true);

    newNodes.append('text')
      .attr('text-anchor', 'middle')
      .text((d: any) => { return d.display(); });
  }
}
