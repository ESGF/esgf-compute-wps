import { Variable } from './variable';

export class Dataset {
  constructor(
    public datasetID: string,
    public variables: Variable[] = [],
    public fileMap?: string[],
  ) { }
}
