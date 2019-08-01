import { Component, Input, OnInit, AfterViewInit, ViewEncapsulation, ViewChild } from '@angular/core';

import { JoyrideService } from 'ngx-joyride';

import { Parameter } from './parameter';
import { ConfigureService } from './configure.service';
import { Process } from './process';
import { NotificationService } from '../core/notification.service';
import { ConfigService } from '../core/config.service';
import { WPSService } from '../core/wps.service';
import { ProcessWrapper } from './process-wrapper';
import { Link } from './link';
import { EditorState } from './editor-state.enum';
import { Variable } from './variable';
import { RegridModel } from './regrid';
import { AuthService } from '../core/auth.service';

import * as d3 from 'd3';

declare var $: any;

@Component({
  selector: 'workflow',
  encapsulation: ViewEncapsulation.None,
  styles: [`
  svg {
    border: 1px solid #ddd;
  }

  .error {
    stroke: #ff0000!important;
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

  .list-item-axis {
    padding: 5px;
  }

  .select-spacer {
    margin-bottom: 10px;
  }
  `],
  templateUrl: './workflow.component.html'
})
export class WorkflowComponent implements OnInit, AfterViewInit {
  @Input() datasetID: string[];
  @Input() params: any;

  processes: Process[];

  nodes: ProcessWrapper[];
  links: Link[];
  selectedNode: ProcessWrapper;
  selectedAbstract: Process;

  state: EditorState;
  stateData: any;

  base = new Process('dummy');

  svg: any;
  svgLinks: any;
  svgNodes: any;
  svgDrag: any;

  constructor(
    private configureService: ConfigureService,
    private configService: ConfigService,
    private notificationService: NotificationService,
    private wpsService: WPSService,
    private authService: AuthService,
    private joyrideService: JoyrideService,
  ) { 
    this.nodes = [];

    this.links = [];

    this.state = EditorState.None;

    this.wpsService.getCapabilities(this.configService.wpsPath)
      .then((processes: string[]) => {
        this.processes = processes.map((identifier: string) => {
          return new Process(identifier);
        });
      });
  }

  ngOnInit() {
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

  ngAfterViewInit() {
    $('#workflow').ready(() => {
      if (localStorage.getItem('tourCompleted') == null) {
        this.startTour();
      }
    });

    $('#processConfigureModal')
      .on('hidden.bs.modal', () => this.update());
  }

  startTour() {
    $('#processPanel').collapse('show');
    $('#globalRegridPanel').collapse('show');
    $('#globalParameterPanel').collapse('show');

    let tour = this.joyrideService.startTour({
      steps: [
        'editor',
        'globalRegrid',
        'globalParameter',
        'processes',
        'process',
      ], 
      themeColor: '#808080',
    }).subscribe((step: any) => {
      if (step.number === 4) {
        this.selectedNode = this.addProcess('CDAT.subset', [200, 200], true);
      }
    }, (error) => {
      tour.unsubscribe();
    }, () => {
      $('#processPanel').collapse('hide');
      $('#globalRegridPanel').collapse('hide');
      $('#globalParameterPanel').collapse('hide');

      this.removeProcess(this.selectedNode.process);

      this.selectedNode = null;

      localStorage.setItem('tourCompleted', 'true');

      tour.unsubscribe();
    });
  }

  execute() {
    if (this.authService.user === null) {
      this.notificationService.error('Must be logged in to execute a workflow');

      return;
    }

    let processes = this.nodes.map((item: ProcessWrapper) => item.process);
    let api_key = this.authService.user.api_key;

    if (processes.length > 1) {
      let workflow = processes.find((item: Process) => {
        return item.identifier == 'CDAT.workflow';
      });

      if (workflow == undefined) {
        workflow = new Process('CDAT.workflow');

        let inputCandidates = processes.map((item: Process) => {
          return item.inputs.map((x: (Process | Variable)) => { return x.uid; })
        }).reduce((p, c) => {
          return p.concat(c);
        });

        workflow.inputs = processes.filter((item: Process) => {
          return inputCandidates.indexOf(item.uid) != -1;
        });

        if (this.base.parameters.length > 0) {
          Object.assign(workflow.parameters, this.base.parameters);
        }

        if (this.base.regrid.regridType != 'None') {
          Object.assign(workflow.regrid, this.base.regrid);
        }

        processes.unshift(workflow);
      }
    }

    this.wpsService.execute(this.configService.wpsPath, api_key, processes)
      .then((data: any) => {
        this.notificationService.message(`Successfully submitted operation for execution`);
      })
      .catch((e: string) => {
        this.notificationService.error(`Execute failed: ${e}`); 
      });
  }

  removeElements() {
    switch (d3.event.keyCode) {
      case 8:
      case 46: {
        let selectedLinks = this.svgLinks.selectAll('.link-select');

        selectedLinks.each((d: any, i: number, g: any) => {
          this.links = this.links.filter((item: Link) => item != d);
        });

        selectedLinks = selectedLinks.data(this.links, (item: Link) => item.uid);

        selectedLinks.exit().remove();
      }
      break;
    }
  }

  removeProcess(item: Process) {
    this.links = this.links.filter((x: Link) => {
      if (x.src.process == item || x.dst.process == item) {
        return false;
      }

      return true;
    });

    this.nodes = this.nodes.filter((x: ProcessWrapper) => {
      if (item.uid === x.process.uid) {
        return false;
      }

      return true;
    });

    this.update();
  }

  removeInput(item: Variable|Process) {
    if (item instanceof Process) {
      this.links = this.links.filter((x: Link) => {
        if (x.src.process == item || x.dst.process == item) {
          return false;
        }

        return true;
      });
    } else {
      throw new Error('Removing something other than a process');
    }

    this.update();
  }

  removeNode(node: ProcessWrapper) {
    $('#configure').modal('hide');

    this.links = this.links.filter((value: Link) => {
      return value.src.uid() !== node.uid() && value.dst.uid() !== node.uid();
    });

    this.nodes = this.nodes.filter((value: ProcessWrapper) => { 
      return node.uid() !== value.uid();
    });

    this.selectedNode = node = null;

    this.update();
  }

  dropped(event: any) {
    this.state = EditorState.Dropped;

    this.stateData = event.dragData;
  }

  showAbstract(process: Process) {
    this.selectedAbstract = process;

    if (process.description == null) {
      this.wpsService.describeProcess(this.configService.wpsPath, process.identifier)
        .then((description: any) => {
          process.description = description;
        });
    }

    $('#processAbstractModal').modal('show');
  }

  addProcessNode(process: Process, x: number, y: number) {
    let processWrapper = new ProcessWrapper(process, x, y);

    this.nodes.push(processWrapper);

    this.update();

    return processWrapper;
  }

  addProcess(identifier: string, origin: [number,number], skipDescription=false) {
    let process = new Process(identifier);
    let processWrapper = null;

    if (!skipDescription) {
      if (this.stateData.description != null) {
        process.description = {...this.stateData.description};
      } else {
        this.wpsService.describeProcess(this.configService.wpsPath, process.identifier)
          .then((description: any) => {
            process.description = description;

            this.addProcessNode(process, origin[0], origin[1]);
          }).catch((error: any) => {
            this.notificationService.error(`Error describing process ${identifier}, try adding again`);
          });
      }
    } else {
      processWrapper = this.addProcessNode(process, origin[0], origin[1]);
    }

    return processWrapper;
  }

  svgMouseOver() {
    if (this.state === EditorState.Dropped) {
      this.state = EditorState.None;

      let origin = d3.mouse(d3.event.target);

      this.addProcess(this.stateData.identifier, origin);
    }
  }

  nodeClick() {
    this.selectedNode = <ProcessWrapper>d3.select(d3.event.target).datum();

    $('#processConfigureModal').modal('show');
  }

  nodeMouseEnter() {
    if (this.state === EditorState.Connecting) {
      this.stateData.dst = d3.select(d3.event.target).datum();
    }

    d3.select(d3.event.target)
      .select('circle')
      .classed('node-connect', true);
  }

  nodeMouseLeave() {
    if (this.state === EditorState.Connecting) {
      this.stateData.dst = null;
    }

    d3.select('.node-connect')
      .classed('node-connect', false);
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
        let dstProcess = this.stateData.dst.process;

        if (dstProcess.inputs.length + 1 > dstProcess.description.metadata.inputs && this.stateData.src.process.identifier != 'CDAT.workflow') {
          this.notificationService.error('Cannot complete connection, destination has exceeded maximum number of inputs');
        } else {
          let checkPath = this.pathExists(this.stateData.dst.process, this.stateData.src.process);

          if (!checkPath) {
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
          } else {
            this.notificationService.error('Cannot complete connection, creating a loop');
          }
        }
      }

      this.state = EditorState.None;

      this.stateData = null;
    }
  }

  pathExists(src: Process, dst: Process) {
    let stack = [src];
    let nodes = this.nodes.map((item: ProcessWrapper) => { return item.process; });

    while (stack.length > 0) {
      let node = stack.pop();

      if (node.uid == dst.uid) {
        return true;
      }

      let inputs = nodes.filter((item: Process) => { 
        return item.inputs.findIndex((x: Variable|Process) => {
          if (x instanceof Process && x.uid === node.uid) {
            return true; 
          }

          return false;
        }) != -1;
      });

      for (let x of inputs) {
        stack.push(x);
      }
    }

    return false;
  }
  
  update() {
    let links = this.svgLinks
      .selectAll('path')
      .attr('d', (d: any) => {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      })
      .data(this.links, (item: Link) => { return item.uid; });

    links.exit().remove();

    links.enter()
      .append('path')
      .classed('link', true)
      .attr('d', (d: any) => {
        return 'M' + d.src.x + ',' + d.src.y + 'L' + d.dst.x + ',' + d.dst.y;
      })
      .on('click', (data: any, index: any, group: any) => {
        let link = d3.select(group[index]);

        if (link.classed('link-select')) {
          link.classed('link-select', false);
        } else {
          link.classed('link-select', true);
        }
      });;

    let nodes = this.svgNodes
      .selectAll('g')
      .attr('transform', (d: any) => { return `translate(${d.x}, ${d.y})`; })
      .data(this.nodes, (item: ProcessWrapper) => { return item.uid(); });

    // Update the error class on circles
    this.svgNodes
      .selectAll('circle')
      .classed('error', (d: ProcessWrapper) => {
        return d.errors;
      });

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
      .classed('error', (d: ProcessWrapper) => {
        return d.errors;
      })
      .classed('node', true);

    newNodes.append('text')
      .attr('text-anchor', 'middle')
      .text((d: any) => { return d.display(); });
  }
}
