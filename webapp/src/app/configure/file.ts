import { Input } from './input';
import { UID } from './uid';

export class File extends UID implements Input {
  constructor(
    public url: string,
    public index: number,
    public included: boolean = true,
    public temporal: any = null,
    public spatial: any = null,
  ) { 
    super();
  }

  display() {
    return this.url;
  }
}
