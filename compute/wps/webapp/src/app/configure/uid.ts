export class UID {
  uid: string = this.newUID();

  constructor() { }

  newUID(): string {
    return Math.random().toString(16).slice(2);
  }
}
