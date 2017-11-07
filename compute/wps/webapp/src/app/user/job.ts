export interface Job {
  elapsed: string;
  id: number;
  process: string;
  server: string;
  created_date: Date;
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
