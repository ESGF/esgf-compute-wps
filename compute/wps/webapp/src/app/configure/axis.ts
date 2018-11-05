import { UID } from './uid';
import { CRS } from './crs.enum';

export interface AxisJSON {
  units: string;
  start: number;
  stop: number;
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

  public custom = false;
  public crs = CRS.Values;

  constructor(
    public start?: number,
    public stop?: number,
    public length?: number,
  ) { 
    super();
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
