import { Variable } from './variable';

export class Dataset {
  constructor(
    public datasetID: string,
    public variables: Variable[],
    public variableNames: string[],
  ) { }

  getVariables(name: string) {
    return this.variables.filter((item: Variable) => {
      if (item.name === name) {
        return true;
      }

      return false;
    });
  }
}
