//export class Variable {
//  constructor(
//    public id: string,
//    public files: number[],
//    public axes: Axis[] = [],
//    public dataset: string = '',
//    public include: boolean = true,
//    public uid: string = '',
//  ) { 
//    this.uid = Math.random().toString(16).slice(2); 
//  }
//}

import { Input } from './input';
import { UID } from './uid';

export class Variable extends UID implements Input {
  constructor(
    public name: string,
    public files: number[],
  ) { 
    super();
  }
}
