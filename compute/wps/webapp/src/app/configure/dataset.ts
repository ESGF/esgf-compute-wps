import { VariableCollection } from './configure.service';

export class Dataset {
  constructor(
    public masterID: string,
    public variableCollection: VariableCollection = null,
  ) { }
}
