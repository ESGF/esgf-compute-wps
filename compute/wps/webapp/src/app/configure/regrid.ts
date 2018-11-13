export class RegridModel {
  tool = ['ESMF', 'Regrid2'];

  method = [
    ['Conserve', 'Linear', 'Patch'],
    ['Area Weighted',],
  ];

  regridTool = this.tool[0];
  regridMethod = this.method[0][0];
  regridType = 'None';
  startLats = 0.0;
  nLats = 180.0;
  deltaLats = 1.0;
  startLons = 0.0;
  nLons = 360.0;
  deltaLons = 1.0;

  methods() {
    let index = this.tool.indexOf(this.regridTool);

    if (index == -1) {
      return ['No methods'];
    }

    return this.method[index];
  }

  toJSON() {
    let data = {
      tool: this.regridTool,
      method: this.regridMethod,
    };

    if (this.regridType === 'Gaussian') {
      data['grid'] = `gaussian~${this.nLats}`;
    } else if (this.regridType === 'Uniform') {
      let lats = '';
      let lons = '';

      if (this.startLats != null && this.deltaLats != null) {
        lats = `${this.startLats}:${this.nLats}:${this.deltaLats}` 
      } else {
        lats = `${this.deltaLats}`
      }

      if (this.startLons != null && this.deltaLons != null) {
        lons = `${this.startLons}:${this.nLons}:${this.deltaLons}`
      } else {
        lons = `${this.deltaLons}`
      }

      data['grid'] = `uniform~${lats}x${lons}`;
    }

    return data;
  }

  validate() {
    if (this.regridType == 'Gaussian') {
      if (this.nLats < 1) {
        throw `Invalid number of latitudes "${this.nLats}"`;
      }
    }
  }
}

