import { Process } from './process';
import { Dataset } from './dataset';
import { Domain } from './domain';

export class ProcessWrapper {
  public selectedDataset: string;
  public selectedVariable: string;

  public dataset: Dataset;
  public domain: Domain;
  public errors = false;

  constructor(
    public process: Process,
    public x: number,
    public y: number
  ) { }

  setSelectedVariable(value: string) {
    this.selectedVariable = value;
  }

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
