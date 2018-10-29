import { ProcessWrapper } from './process-wrapper';

export class Link {
  constructor(
    public src: ProcessWrapper,
    public dst?: ProcessWrapper
  ) { }

  uid() {
    return `${this.src.uid()}:${this.dst.uid()}`;
  }
}
