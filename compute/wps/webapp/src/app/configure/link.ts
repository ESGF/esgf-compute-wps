import { ProcessWrapper } from './process-wrapper';
import { UID } from './uid';

export class Link extends UID {
  constructor(
    public src: ProcessWrapper,
    public dst?: ProcessWrapper
  ) { 
    super();
  }
}
