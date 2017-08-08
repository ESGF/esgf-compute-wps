import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { AuthService } from './auth.service';
import { ConfigureService } from './configure.service';
import { Dimension } from './dimension.component';

import * as d3 from './d3.bundle';
import * as topojson from 'topojson';

import { event } from 'd3-selection';

class Config {
  process: string;
  variable: string;
  files: string;
  dimensions: Dimension[];
}

@Component({
  templateUrl: './configure.component.html',
  styleUrls: ['./map.css'],
  encapsulation: ViewEncapsulation.None
})
export class ConfigureComponent implements OnInit  { 
  PROCESSES = ['CDAT.aggregate', 'CDAT.subset'];

  config: Config = new Config();
  variables: string[] = [];
  files: string[] = [];
  roiMove: boolean = false;
  roiResize: boolean[] = [false, false, false, false];
  svg: any;
  roi: any;
  projection: any;
  dimensions: Dimension[] = [
    new Dimension('longitude', 'Degree', -180, 180, 1),
    new Dimension('latitude', 'Degree', 90, -90, 1)
  ];

  constructor(
    private route: ActivatedRoute,
    private authService: AuthService,
    private configService: ConfigureService
  ) { }

  ngOnInit() {
    this.route.queryParams.subscribe(params => this.loadData(params));

    this.config.process = this.PROCESSES[0];

    this.loadMap();
  }

  filterDimensionByName(v: string){
    return (d: Dimension) => {
      return d.name.indexOf(v) > -1;
    }
  }

  updateDimensions(start: [number, number], stop: [number, number]): void {
    let longitude = this.dimensions.filter(this.filterDimensionByName('longitude'));
    let latitude = this.dimensions.filter(this.filterDimensionByName('latitude'));

    if (start !== null) {
      if (start[0] !== -1) {
        let geo = this.projection.invert([start[0], 0]);

        longitude[0].start = geo[0];
      }

      if (start[1] !== -1) {
        let geo = this.projection.invert([0, start[1]]);

        latitude[0].start = geo[1];
      }
    }

    if (stop !== null) {
      if (stop[0] !== -1) {
        let geo = this.projection.invert([stop[0], 0]);

        longitude[0].stop = geo[0];
      }

      if (stop[1] !== -1) {
        let geo = this.projection.invert([0, stop[1]]);

        latitude[0].stop = geo[1];
      }
    }
  }

  onDragStart() {
    return () => {
      const e = <d3.D3DragEvent<SVGRectElement, any, any>> event;

      let coord = d3.mouse(this.svg.node());

      this.roi.attr('x', coord[0])
        .attr('y', coord[1]);

      this.roi.attr('width', 0);
      this.roi.attr('height', 0);

      this.updateDimensions(coord, null);
    }
  }

  onDrag() {
    return () => {
      const e = <d3.D3DragEvent<SVGRectElement, any, any>> event;

      let x = +this.roi.attr('x');
      let y = +this.roi.attr('y');

      let width = +this.roi.attr('width') + e.dx;
      let height = +this.roi.attr('height') + e.dy;

      this.roi.attr('width', width);
      this.roi.attr('height', height);

      this.updateDimensions(null, [x + width, y + height]);
    }
  }

  isTrue(e: boolean, index: number, array: Array<boolean>): boolean {
    return e;
  }

  onROIDragStart() {
    return () => {
      const e = <d3.D3DragEvent<SVGRectElement, any, any>> event;
      const bar = 20;

      let coord = d3.mouse(this.svg.node());

      let x = +this.roi.attr('x');
      let y = +this.roi.attr('y');
      let width = +this.roi.attr('width');
      let height = +this.roi.attr('height');

      this.roiResize[0] = (coord[1] > (y + height - bar));
      this.roiResize[1] = (coord[0] < (x + bar));
      this.roiResize[2] = (coord[1] < (y + bar));
      this.roiResize[3] = (coord[0] > (x + width - bar));

      if (this.roiResize.some(this.isTrue)) {
        this.roiMove = false;
      } else {
        this.roiMove = true;
      }
    }
  }

  onROIDrag() {
    return () => {
      const e = <d3.D3DragEvent<SVGRectElement, any, any>> event;

      let dx = event.dx;
      let dy = event.dy;

      if (this.roiMove) {
        let bboxStart = this.projection([-180, 90]);
        let bboxStop = this.projection([180, -90]);

        let x = +this.roi.attr('x');
        let y = +this.roi.attr('y');
        let width = +this.roi.attr('width');
        let height = +this.roi.attr('height');

        if ((dx <= -1 && x != bboxStart[0]) || (dx >= 1 && (x + width) != bboxStop[0])) {
          x += dx;

          this.roi.attr('x', x);
        }

        if ((dy <= -1 && y != bboxStart[1]) || (dy >= 1 && (y + height) != bboxStop[1])) {
          y += dy;

          this.roi.attr('y', y);
        }

        this.updateDimensions([x, y], [x + width, y + height]);
      } else {
        if (dx !== 0) {
          let x = +this.roi.attr('x');
          let width = +this.roi.attr('width');

          if (this.roiResize[1]) {
            x += dx;
            width -= dx;

            this.roi.attr('x', x);
            this.roi.attr('width', width);

            this.updateDimensions([x, -1], null);
          }

          if (this.roiResize[3]) {
            width += dx;

            this.roi.attr('width', width);

            this.updateDimensions(null, [width, -1]);
          }
        }

        if (dy !== 0) {
          let y = +this.roi.attr('y');
          let height = +this.roi.attr('height');

          if (this.roiResize[0]) {
            height += dy;

            this.roi.attr('height', height);

            this.updateDimensions(null, [-1, height]);
          }

          if (this.roiResize[2]) {
            y += dy;
            height -= dy;

            this.roi.attr('y', y);
            this.roi.attr('height', height);
            
            this.updateDimensions([-1, y], null);
          }
        }
      }
    }
  }

  onROIDragEnd() {
    return () => {
      const e = <d3.D3DragEvent<SVGRectElement, any, any>> event;

      this.roiMove = false;

      for (let i = 0; i < this.roiResize.length; i++) {
        this.roiResize[i] = false;
      }
    }
  }

  loadMap(): void {
    this.svg = d3.select('svg')
      .attr('width', 960)
      .attr('height', 500);

    let color = d3.scaleOrdinal(d3.schemeCategory20);

    this.projection = d3.geoEquirectangular();

    let path = d3.geoPath(this.projection);

    let graticule = d3.geoGraticule();

    this.svg.append('path')
      .datum(graticule)
      .attr('class', 'graticule')
      .attr('d', path);

    this.roi = this.svg.append('rect')
      .attr('class', 'roi')
      .attr('x', 0)
      .attr('y', 0)
      .attr('width', 0)
      .attr('height', 0)
      .call(d3.drag()
        .on('start', this.onROIDragStart())
        .on('drag', this.onROIDrag())
        .on('end', this.onROIDragEnd())
      );

    this.svg.call(d3.drag()
      .on('start', this.onDragStart())
      .on('drag', this.onDrag())
    );

    d3.json('/static/data/ne_50m_admin_0.json', (error: any, world: any) => {
      if (error) throw error;

      let countries = topojson.feature(world, world.objects.countries);
      let neighbors = topojson.neighbors(world.objects.countries.geometries);

      this.svg.selectAll('.country')
        .data(countries.features)
        .enter().insert('path', '.graticule')
          .attr('class', 'country')
          .attr('d', path)
          .style('fill', (d: any, i: number) => {
            return color((d.color = d3.max(neighbors[i], (n: number): number => {
              return countries.features[n].color;
            }) + 1 | 0) + '');
          });

      this.svg.insert('path', '.graticule')
        .datum(topojson.mesh(world, world.objects.countries, (a: number, b: number): boolean => { return a !== b; }))
        .attr('class', 'boundary')
        .attr('d', path);
    });
  }

  onAddDimension(): void {
    let dim = new Dimension();

    dim.remove.subscribe((dimension: Dimension) => {
      for (let i = 0; i < this.dimensions.length; i++) {
        if (this.dimensions[i].uuid === dimension.uuid) {
          this.dimensions.splice(i, 1);
          
          break;
        }
      }
    });

    this.dimensions.unshift(dim);
  }

  onSubmit(): void {
    this.config.files = this.files.filter((x) => { return (x.indexOf(`/${this.config.variable}_`) > 0); }).join(',');

    this.config.dimensions = this.dimensions;

    this.configService.downloadScript(this.config);
  }

  loadData(params: any): void {
    this.configService.searchESGF(params)
      .then(response => this.handleLoadData(response));
  }

  handleLoadData(response: any): void {
    if (response.status && response.status === 'success') {
      let time = response.data.time;

      this.files = response.data.files;

      this.variables = response.data.variables;

      this.dimensions.unshift(new Dimension('time', response.data.time_units, time[0], time[1], 1));

      this.config.variable = this.variables[0];
    } 
  }
}
