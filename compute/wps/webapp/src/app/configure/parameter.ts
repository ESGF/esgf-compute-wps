import { UID } from './uid';

export class Parameter extends UID {
  constructor(
    public key: string = '',
    public value: string = ''
  ) { 
    super();
  }

  toJSON() {
    let data = {};

    data[this.key] = this.value;

    return data;
  }

  validate() {
    if (this.key == '') {
      throw `Parameter must have a key`;
    } else if (this.value == '') {
      throw `Parameter "${this.key}" must have a value`;
    }
  }
}

