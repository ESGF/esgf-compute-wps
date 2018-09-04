export class Job {
  elapsed: string;
  id: number;
  process: string;
  server: string;
  created_date: Date;
  status: Status[];

  constructor(options?: any) {
    this.elapsed = options.elapsed || '';

    this.id = options.id || null;

    this.process = options.process || '';

    this.server = options.server || '';

    this.created_date = options.created_date || null;

    this.status = options.status || null;
  }

  get completed(): boolean {
    return (this.latest === null) ? false :
      (this.latest.status === 'ProcessSucceeded' || this.latest.status === 'ProcessFailed') ? true : false;
  }

  get latest(): Status {
    return (this.status.length > 0) ? this.status[this.status.length-1] : null;
  }

  get earliest(): Status {
    return (this.status.length > 0) ? this.status[0] : null;
  }

  updateStatus(updates: Status[]) {
    updates.forEach((update: Status) => {
      let matched = this.status.find((value: Status) => { return value.status === update.status; });

      if (matched === undefined) {
        this.status.push(update);
      } else {
        matched.messages = update.messages;
      }
    });
  }
}

export interface Status {
  status: string;
  output?: any;
  exception?: string;
  messages?: Message[];
  created_date: Date;
  updated_date: Date;
}

export interface Message {
  message: string;
  percent: number;
  created_date: Date;
}
