export class Job {
  elapsed: string;
  id: number;
  process: string;
  server: string;
  created_date: Date;
  status: Status[];

  get completed() {
    return (this.latest === null) ? false :
      (this.latest.status === 'ProcessSucceeded' || this.latest.status === 'ProcessFailed') ? true : false;
  }

  get latest() {
    return (this.status.length > 0) ? this.status[this.status.length-1] : null;
  }

  get earliest() {
    return (this.status.length > 0) ? this.status[0] : null;
  }
}

export interface Status {
  status: string;
  output?: string;
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
