import { Component, Input, OnInit, ViewEncapsulation, ViewChild } from '@angular/core';

import { Parameter } from './parameter.component';
import { MapComponent } from './map.component';
import { Axis } from './axis.component';
import { 
  ConfigureService, 
  Configuration,
  Process, 
  Variable, 
  Dataset, 
  DatasetCollection 
} from './configure.service';
import { NotificationService } from '../core/notification.service';

import * as d3 from 'd3';

declare var jQuery: any;
declare var $: any;

class WorkflowModel { 
  domain: string;
  axes: Axis[];

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

  .scrollable {
    max-height: 50vh;
    overflow-y: scroll;
  }

  .list-item-axis {
    padding: 5px;
  }

  .select-spacer {
    margin-bottom: 10px;
  }

  .loading {
    cursor: wait;
  }
  `],
  templateUrl: './workflow.component.html'
})
export class WorkflowComponent implements OnInit {
  @Input() processes: any[];
  @Input() datasets: string[];
  @Input() config: Configuration;

  @ViewChild(MapComponent) map: MapComponent;

  model: WorkflowModel = new WorkflowModel();

  nodes: ProcessWrapper[];
  links: Link[];
  rootNode: ProcessWrapper;
  selectedNode: ProcessWrapper;

  loading: boolean = false;

  state: EditorState;
  stateData: any;

  svg: any;
  svgLinks: any;
  svgNodes: any;
  svgDrag: any;

  constructor(
    private configService: ConfigureService,
    private notificationService: NotificationService
  ) { 
    this.model.domain = 'World';

    this.model.process = new Process();

    this.model.process.domain.push({
      id: 'lat',
      start: 90,
      stop: -90,
      step: 1,
      units: 'degress north'
    } as Axis);

    this.model.process.domain.push({
      id: 'lon',
      start: -180,
      stop: 180,
      step: 1,
      units: 'degress west'
    } as Axis);

    this.model.process.domain.push({
      id: 'time',
      start: 0,
      stop: 0,
      step: 1,
      units: 'Custom'
    } as Axis);

    this.nodes = [];

    this.links = [];

    this.state = EditorState.None;
  }

  ngOnInit() {
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

    d3.select(window)
      .on('keydown', () => this.removeElements())

    this.svg = d3.select('.graph-container')
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

  loadDomain() {
    this.model.process.domain = this.model.selectedVariable.axes.map((axis: Axis) => {
      return {... axis};
    });

    $('#datasetExplorer').modal('hide');
  }

  loadVariable() {
    this.loading = true;

    this.config.variable = this.model.selectedVariable;

    this.configService.searchVariable(this.config)
      .then(axes => {
        this.model.selectedVariable.axes = axes.map((axis: Axis) => {
          return {step: 1, ...axis}; 
        });

        this.loading = false;
      })
      .catch(error => { 
        this.loading = false; 

        this.notificationService.error(error);
      });
  }

  showExplorer() {
    this.loadVariable();

    $('#datasetExplorer').modal('show');    
  }

  showHelp() {
    jQuery('#help').modal('show');
  }

  showDomain() {
    this.map.domain = this.model.domain;

    this.map.domainChange();

    jQuery('#map').modal('show');

    // need to invalidate the map after it's presented to the user
    jQuery('#map').on('shown.bs.modal', () => {
      this.map.map.invalidateSize();
    });
  }

  showAbstract(process: any) {
    // Really ugly way to parse XML
    // TODO replace with better parsing
    let parser = new DOMParser();

    let xmlDoc = parser.parseFromString(process.description, 'text/xml');

    let description = xmlDoc.children[0].children[0];

    let abstractText = '';
    let titleText = '';

    Array.from(description.children).forEach((item: any) => {
      if (item.localName === 'Identifier') {
        titleText = item.innerHTML;
      } else if (item.localName === 'Abstract') {
        abstractText = item.innerHTML;
      }
    });

    let modal = $('#abstractModal');

    modal.find('.modal-title').html(`"${titleText}" Abstract`);

    if (abstractText === '') { abstractText = 'No abstract available'; }

    modal.find('#abstract').html(abstractText);
  }

  onScript() {
    this.notificationService.error('Workflow script is unsupported at the moment');
  }

  onExecute() {
    // Cover a few workflow specific checks before executing
    if (this.nodes.length === 0) {
      this.notificationService.error('Workflow must contain atleast 1 process');

      return;
    }

    if (this.rootNode == null) {
      this.notificationService.error('Workflow must converge to a single process');

      return;
    }

    // Assign values from our model
    // These values are not stored in rootNode since this changes with the state
    // of the graph
    this.rootNode.process.domain = this.model.process.domain;

    this.rootNode.process.regrid = this.model.process.regrid;

    this.rootNode.process.parameters = this.model.process.parameters;

    this.configService.execute(this.rootNode.process)
      .then((data: any) => {
        let parser = new DOMParser();
        let xml = parser.parseFromString(data, 'text/xml');
        let el = xml.getElementsByTagName('wps:ExecuteResponse');
        let link = '';

        if (el.length > 0) {
          let statusLocation = el[0].attributes.getNamedItem('statusLocation').value;

          let jobID = statusLocation.substring(statusLocation.lastIndexOf('/')+1);

          link = `/wps/home/user/jobs`;
        }
        
        this.notificationService.message('Succesfully submitted job', link);
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  domainChange() {
    this.map.domain = this.model.domain;

    this.map.domainChange();

    if (this.model.domain === 'Custom') {
      jQuery('#map').modal('show');

      // need to invalidate the map after it's presented to the user
      jQuery('#map').on('shown.bs.modal', () => {
        this.map.map.invalidateSize();
      });
    }
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
    this.model.process.parameters.push(new Parameter());
  }

  removeParameterWorkflow(param: Parameter) {
    let newParams = this.model.process.parameters.filter((value: Parameter) => {
      return param.uid != value.uid;
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

    this.links = this.links.filter((value: Link) => {
      return value.src !== node && value.dst !== node;
    });

    this.nodes = this.nodes.filter((value: ProcessWrapper) => { 
      return node.uid() !== value.uid();
    });

    this.selectedNode = node = null;

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
    this.selectedNode = <ProcessWrapper>d3.select(d3.event.target).datum();

    jQuery('#configure').modal('show');
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
      .data(this.nodes, (item: ProcessWrapper) => { return item.uid(); });

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
