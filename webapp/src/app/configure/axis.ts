import { UID } from './uid';
import { CRS } from './crs.enum';

export interface AxisJSON {
  units: string;
  start: any;
  stop: any;
  length: number;
  id: string;
}

export class Axis extends UID {
  public _start: number;
  public _stop: number;
  public _length: number;

  public units: string;
  public id: string;
  public step: number;

  public start_timestamp: string;
  public stop_timestamp: string;

  public custom = false;
  public crs = CRS.Values;

  constructor(
    public start?: any,
    public stop?: any,
    public length?: number,
  ) { 
    super();
  }

  updateValues(value: any) {
    this._start = value.start;

    this._stop = value.stop;

    if (this.crs === CRS.Values) {
      this.start = value.start;

      this.stop = value.stop;
    }

    this.units = value.units;
  }

  updateLength(value: number) {
    this._length = value;

    if (this.crs === CRS.Indices) {
      this.stop = value;
    }
  }

  toJSON() {
    return {
      start: this.start,
      end: this.stop,
      step: this.step,
      crs: this.crs,
    }
  }

  reset(crs: CRS) {
    switch(crs) {
    case CRS.Values:
        this.start = this._start;

        this.stop = this._stop;
        break;
    case CRS.Indices:
        this.start = 0;

        this.stop = this._length;
        break;
    case CRS.Timestamps:
        this.start = this.start_timestamp;

        this.stop = this.stop_timestamp;
        break;
    }
  }

  static fromJSON(json: AxisJSON|string): Axis {
    if (typeof json === 'string') {
      return JSON.parse(json, Axis.reviver);
    } else {
      let axis = new Axis(json.start, json.stop, json.length);

      return Object.assign(axis, json, {
        _start: axis.start,
        _stop: axis.stop,
        _length: axis.length,
      });
    }
  }

  static reviver(key: string, value: any): any {
    return key === '' ? Axis.fromJSON(value) : value;
  }
}
