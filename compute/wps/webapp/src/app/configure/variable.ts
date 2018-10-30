import { Input } from './input';
import { UID } from './uid';

export class Variable extends UID implements Input {
  constructor(
    public name: string,
    public files: string[],
  ) { 
    super();
  }

  display() {
    let parts;

    try {
      parts = this.files[0].split('/');
    } catch (TypeError) {
      return 'Unable to display';
    }

    return parts[parts.length-1];
  }
}
