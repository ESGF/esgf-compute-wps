import { Input } from './input';
import { UID } from './uid';
import { Domain } from './domain';

export class Variable extends UID implements Input {
  constructor(
    public name: string,
    public file: string,
    public index?: number,
    public domain?: Domain,
  ) { 
    super();
  }

  toJSON() {
    return {
      id: `${this.name}|${this.uid}`,
      uri: this.file,
    }
  }

  display() {
    let parts;

    try {
      parts = this.file.split('/');
    } catch (TypeError) {
      return 'Unable to display';
    }

    return parts[parts.length-1];
  }
}
