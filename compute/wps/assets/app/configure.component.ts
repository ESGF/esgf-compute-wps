import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { AuthService } from './auth.service';
import { ConfigureService } from './configure.service';
import { Dimension } from './dimension.component';

import * as d3 from './d3.bundle';
import * as topojson from 'topojson';

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
  svg: any = null;
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

  loadMap(): void {
    this.svg = d3.select('svg')
      .attr('width', 960)
      .attr('height', 500);

    let color = d3.scaleOrdinal(d3.schemeCategory20);

    let projection = d3.geoEquirectangular();

    let path = d3.geoPath(projection);

    let graticule = d3.geoGraticule();

    this.svg.append('path')
      .datum(graticule)
      .attr('class', 'graticule')
      .attr('d', path);

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
