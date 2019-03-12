export class Job {
  accepted_on: string;
  elapsed: string;
  extra: string;
  id: number;
  latest_status: string;
  process: string;
  server: string;
  status: Status[];

  statusUrls: string[];

  constructor(options?: any) {
    this.accepted_on = options.accepted_on || '';

    this.elapsed = options.elapsed || '';

    this.extra = options.extra || '';
    
    this.id = options.id || -1;

    this.latest_status = options.latest_status || '';

    this.process = options.process || '';

    this.server = options.server || '';

    this.status = [];

    this.statusUrls = options.status || [];
  }
}

export class Status {
  created_date: string;
  exception: string;
  id: number;
  output: string;
  status: string;
  messages: Message[];

  constructor(options?: any) {
    this.created_date = options.created_date;

    this.exception = options.exception;

    this.id = options.id;

    this.output = JSON.parse(options.output);

    this.status = options.status;

    this.messages = options.messages
      .map((item: any) => new Message(item))
      .sort((a: any, b: any) => {
        if (a.id > b.id) {
          return 1;
        } else if (a.id < b.id) {
          return -1;
        } else {
          return 0;
        }
      });
  }

  exceptionParsed(): string {
    let p = new DOMParser();

    let xmlDoc = p.parseFromString(this.exception, 'text/xml');

    let elements = xmlDoc.getElementsByTagName('ows:ExceptionText');

    if (elements.length > 0) {
      return elements[0].innerHTML;
    }

    return 'Unknown';
  }
}

export class Message {
  created_date: string;
  id: number;
  message: string;
  percent: number;

  constructor(options?: any) {
    this.created_date = options.created_date;

    this.id = options.id;

    this.message = options.message;

    this.percent = options.percent;
  }
}
