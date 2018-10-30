import { Input } from './input';
import { UID } from './uid';

export class Variable extends UID implements Input {
  constructor(
    public name: string,
    public files: string[],
  ) { 
    super();
  }
}
