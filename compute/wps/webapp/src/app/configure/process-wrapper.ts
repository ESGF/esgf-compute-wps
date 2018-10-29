import { Process } from './process';

export class ProcessWrapper {
  constructor(
    public process: Process,
    public x: number,
    public y: number
  ) { }

  display() {
    return this.process.identifier;
  }

  uid() {
    return this.process.uid;
  }

  inputDatasets() {
    return this.process.inputs.filter((value: any) => {
      return !(value instanceof Process);
    });
  }
}
