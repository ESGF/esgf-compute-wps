export interface Job {
  created: Date;
  id: number;
  elapsed: string;
  status: Status[];
}

export interface Status {
  status: string;
  output?: string;
  exception?: string;
  messages?: Message[];
  created_date: Date;
}

export interface Message {
  message: string;
  percent: number;
  created_date: Date;
}
