import { Input } from './input';
import { UID } from './uid';
import { FileMeta } from './file-meta';

export class Variable extends UID implements Input {
  constructor(
    public name: string,
    public files: string[],
    public meta: {[file: string]: FileMeta} = {},
  ) { 
    super();
  }

  fileHasMeta(file: string) {
    return file in this.meta;
  }

  addMeta(file: string, meta: FileMeta) {
    if (!(file in this.meta)) {
      this.meta[file] = meta;
    }
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
